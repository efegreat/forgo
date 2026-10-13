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
	// ReqIDHeader carries the operation ID. The ID middleware keeps a well-formed one from the request, and
	// the default serializer sets it on every response.
	ReqIDHeader = "X-Request-ID"

	contentTypeHeader = "Content-Type"
	contentTypeJSON   = "application/json; charset=utf-8"
)

// ErrAlreadyWritten is returned by a default serializer that is called again after it wrote its response. The
// later write is dropped so the response cannot be corrupted.
var ErrAlreadyWritten = errors.New("http: response already written")

// response is the envelope sent to the client.
type response struct {
	ReqID   string `json:"reqId"`
	Status  int    `json:"status"`
	Result  any    `json:"result,omitempty"`
	ErrCode string `json:"errCode,omitempty"`
	ErrMsg  string `json:"errMsg,omitempty"`
}

// SerializerFactory creates the serializer of one response, for the operation with ID id. The server creates
// one each time it answers, and only while nothing is written to w yet.
type SerializerFactory func(id string, w http.ResponseWriter) transport.ISerializer

// respond answers result, unless the response is already written, such as by a handler that wrote its own. It
// returns the serializer's failure.
func respond(ctx *operation.Context, sf SerializerFactory, result any) error {
	st := ctx.State.(*State)
	if st.Writer.Written() {
		return nil
	}
	return sf(ctx.ID, st.Writer).Success(result)
}

// respondError answers err, mapped by em, unless err is nil or http.ErrAbortHandler, or the response is already
// written. It returns the error the operation fails with, which Recorder logs: the serializer's when it returned
// one, and otherwise err itself when it holds an operation.Error, so the log keeps what is wrapped around it,
// such as a panic's stack, or else the mapped error, so the log has the code the client got.
func respondError(ctx *operation.Context, sf SerializerFactory, em *operation.ErrorMapper, err error) error {
	if err == nil || errors.Is(err, http.ErrAbortHandler) {
		return err
	}
	st := ctx.State.(*State)
	if st.Writer.Written() {
		return err
	}

	failed := err
	var oe operation.Error
	if !errors.As(err, &oe) {
		oe = em.Map(err)
		// A mapper that leaves err out of its result would leave it out of the log too.
		if oe.Unwrap() == nil {
			oe = oe.Wrap(err)
		}
		failed = oe
	}

	if serr := sf(ctx.ID, st.Writer).Error(oe); serr != nil {
		return serr
	}
	return failed
}

// defaultSerializer writes one response as the JSON envelope, through the standard library, so it can be used
// from any http.HandlerFunc.
type defaultSerializer struct {
	reqID   string
	w       http.ResponseWriter
	written bool
}

// newDefaultSerializer creates a serializer for a single request/response pair.
func newDefaultSerializer(id string, w http.ResponseWriter) transport.ISerializer {
	return &defaultSerializer{reqID: id, w: w}
}

// Success answers 200 with result in the envelope.
func (s *defaultSerializer) Success(result any) error {
	return s.write(response{
		ReqID:  s.reqID,
		Status: http.StatusOK,
		Result: result,
	})
}

// Error answers the Code and Message of the operation.Error in err's chain, with the HTTP status of its Status,
// or those of operation.NewFallbackError when err holds none. It returns nil once the response is written.
func (s *defaultSerializer) Error(err error) error {
	var oe operation.Error
	if !errors.As(err, &oe) {
		oe = operation.NewFallbackError(err)
	}
	if werr := s.write(errorResponse(s.reqID, oe)); werr != nil {
		if cause := oe.Unwrap(); cause != nil {
			return oe.Wrap(fmt.Errorf("%w; %w", cause, werr))
		}
		return oe.Wrap(werr)
	}
	return nil
}

// errorResponse returns the envelope of oe.
func errorResponse(reqID string, oe operation.Error) response {
	return response{
		ReqID:   reqID,
		Status:  status(oe.Status),
		ErrCode: oe.Code,
		ErrMsg:  oe.Message,
	}
}

// write sends resp as JSON with its status. Only the first call writes, so a serializer called again cannot
// corrupt the response. A result that cannot be encoded, such as a NaN, is answered like an unmapped error. The
// failures to encode and to write are returned to the caller, which owns logging them.
func (s *defaultSerializer) write(resp response) error {
	if s.written {
		return ErrAlreadyWritten
	}
	s.written = true

	body, err := json.Marshal(resp)
	if err != nil {
		err = fmt.Errorf("http: encode response: %w", err)
		resp = errorResponse(s.reqID, operation.NewFallbackError(nil))
		body, _ = json.Marshal(resp) // strings and an int always encode
	}

	h := s.w.Header()
	h.Set(contentTypeHeader, contentTypeJSON)
	h.Set(ReqIDHeader, s.reqID)
	s.w.WriteHeader(resp.Status)
	if _, werr := s.w.Write(body); werr != nil && err == nil {
		err = fmt.Errorf("http: write response: %w", werr)
	}
	return err
}
