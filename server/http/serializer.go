package http

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/transport"
)

const (
	ReqIDHeader       = "X-Request-ID"
	ContentTypeHeader = "Content-Type"
	ContentTypeJson   = "application/json; charset=utf-8"
	DefaultErrorMsg   = "internal server error"
)

// ErrAlreadyWritten is returned when the response for the request has already
// been written. The later write is dropped so the response cannot be corrupted.
var ErrAlreadyWritten = errors.New("http: response already written")

// response is the envelope sent to the client.
type response struct {
	ReqId   string `json:"reqId"`
	Status  int    `json:"status"`
	Result  any    `json:"result,omitempty"`
	ErrCode string `json:"errCode,omitempty"`
	ErrMsg  string `json:"errMsg,omitempty"`
}

type SerializerFactory func(id string, w http.ResponseWriter) transport.ISerializer

// defaultSerializer is the default ISerializer implementation. It is bound
// to a single request and writes the response through the standard library, so
// it can be used from any http.HandlerFunc.
type defaultSerializer struct {
	reqID   string
	w       http.ResponseWriter
	written bool
}

// newDefaultSerializer creates a serializer for a single request/response pair.
func newDefaultSerializer(id string, w http.ResponseWriter) transport.ISerializer {
	return &defaultSerializer{reqID: id, w: w}
}

// serializeError answers with oe through s and returns the error the operation fails with: what s returned,
// or oe when s returned nil, so that a serializer reporting only write failures cannot turn a failed
// operation into a success.
func serializeError(s transport.ISerializer, oe operation.Error) error {
	if err := s.Error(oe); err != nil {
		return err
	}
	return oe
}

func (s *defaultSerializer) Success(result any) error {
	resp := response{
		ReqId:  s.reqID,
		Status: http.StatusOK,
		Result: result,
	}
	return s.write(http.StatusOK, resp)
}

func (s *defaultSerializer) Error(err error) error {
	var oe operation.Error
	if errors.As(err, &oe) {
		st := status(oe.Status)
		resp := response{
			ReqId:   s.reqID,
			Status:  st,
			ErrCode: oe.Code,
			ErrMsg:  oe.Message,
		}
		wErr := s.write(st, resp)

		if wErr != nil {
			if cause := oe.Unwrap(); cause != nil {
				return oe.Wrap(fmt.Errorf("cause: %w, writeErr: %w", cause, wErr))
			}
			return oe.Wrap(fmt.Errorf("writeErr: %w", wErr))
		}

		return oe
	}

	http.Error(s.w, DefaultErrorMsg, http.StatusInternalServerError)
	return err
}

// write sends the response as JSON with the given status code. Only the first
// call writes: a handler that reports an error after the response was already
// sent cannot corrupt it. Serialization and write failures are returned to the
// caller, which owns logging them.
func (s *defaultSerializer) write(status int, resp response) error {
	if s.written {
		return ErrAlreadyWritten
	}
	s.written = true

	b, err := json.Marshal(resp)
	if err != nil {
		// The result is not serializable; fall back to a plain http error so the
		// client still gets a response instead of an empty 200.
		http.Error(s.w, DefaultErrorMsg, http.StatusInternalServerError)
		return fmt.Errorf("serialize response: %w", err)
	}

	s.w.Header().Set(ContentTypeHeader, ContentTypeJson)
	s.w.Header().Set(ReqIDHeader, resp.ReqId)
	s.w.WriteHeader(status)
	if _, err := s.w.Write(b); err != nil {
		return fmt.Errorf("write response: %w", err)
	}

	return nil
}
