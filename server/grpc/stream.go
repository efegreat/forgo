package grpc

import (
	"context"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// callStream is grpc-go's stream of a call, below every wrapper, so that it sees every message and every
// metadata the call sends, whoever sends it. It receives a message into a decodeTarget, so that a message that
// cannot be decoded fails with errUndecodable rather than with grpc-go's status. It counts the messages
// received and sent, and records the header and trailer metadata set, for the Recorder. Its Context is the
// call's callCtx.
type callStream struct {
	grpc.ServerStream
	ctx context.Context
	st  *State
	// target is reused, since grpc-go receives on a stream from one goroutine at a time. It keeps the last
	// message, so that a stats handler that holds on to its InPayload, which grpc-go allows it to change
	// afterwards, still reads a message.
	target decodeTarget
}

// Context returns the call's callCtx, which carries the server's logger.
func (s *callStream) Context() context.Context {
	return s.ctx
}

// RecvMsg receives a message into m. A message that cannot be decoded is errUndecodable, which grpc-go did not
// answer.
func (s *callStream) RecvMsg(m any) error {
	msg, ok := m.(proto.Message)
	if !ok {
		return s.count(&s.st.receivedMsgs, s.ServerStream.RecvMsg(m))
	}
	s.target = decodeTarget{msg: msg}
	err := s.ServerStream.RecvMsg(&s.target)
	if err == nil && s.target.err != nil {
		return errUndecodable.Messagef(s.target.err)
	}
	return s.count(&s.st.receivedMsgs, err)
}

// SendMsg sends m, and counts it once it is sent.
func (s *callStream) SendMsg(m any) error {
	return s.count(&s.st.sentMsgs, s.ServerStream.SendMsg(m))
}

func (s *callStream) count(n *atomic.Uint64, err error) error {
	if err == nil {
		n.Add(1)
	}
	return err
}

func (s *callStream) SetHeader(md metadata.MD) error {
	err := s.ServerStream.SetHeader(md)
	if err == nil {
		s.st.metadata.add(&s.st.metadata.header, md)
	}
	return err
}

func (s *callStream) SendHeader(md metadata.MD) error {
	err := s.ServerStream.SendHeader(md)
	if err == nil {
		s.st.metadata.add(&s.st.metadata.header, md)
	}
	return err
}

func (s *callStream) SetTrailer(md metadata.MD) {
	s.ServerStream.SetTrailer(md)
	s.st.metadata.add(&s.st.metadata.trailer, md)
}

// callCtx is the context of a call's operation and of its stream: the server's logger over grpc-go's context,
// with the call's transportStream in place of grpc-go's, so that the metadata that grpc.SetHeader,
// grpc.SendHeader and grpc.SetTrailer set on it are recorded too. It lives in the call, so it costs no
// allocation, unlike a context.WithValue.
type callCtx struct {
	context.Context
	transport transportStream
}

func (c *callCtx) Value(key any) any {
	if key == streamKey {
		return &c.transport
	}
	return c.Context.Value(key)
}

// streamKey is the key grpc-go keeps a call's grpc.ServerTransportStream under in its context, which it does
// not export. grpc.ServerTransportStreamFromContext asks a context for it, so a context that records the key it
// is asked for learns it. TestResponseMetadata fails if grpc-go stops finding the stream this way.
var streamKey = func() any {
	probe := keyProbe{Context: context.Background()}
	grpc.ServerTransportStreamFromContext(&probe)
	return probe.key
}()

type keyProbe struct {
	context.Context
	key any
}

func (p *keyProbe) Value(key any) any {
	p.key = key
	return nil
}

// transportStream is the grpc.ServerTransportStream of a call's callCtx: grpc-go's, found in its own context,
// which also records the metadata set through it.
type transportStream struct {
	ctx context.Context // grpc-go's context of the call
	st  *State
}

func (s *transportStream) Method() string {
	method, _ := grpc.Method(s.ctx)
	return method
}

func (s *transportStream) SetHeader(md metadata.MD) error {
	err := grpc.SetHeader(s.ctx, md)
	if err == nil {
		s.st.metadata.add(&s.st.metadata.header, md)
	}
	return err
}

func (s *transportStream) SendHeader(md metadata.MD) error {
	err := grpc.SendHeader(s.ctx, md)
	if err == nil {
		s.st.metadata.add(&s.st.metadata.header, md)
	}
	return err
}

func (s *transportStream) SetTrailer(md metadata.MD) error {
	err := grpc.SetTrailer(s.ctx, md)
	if err == nil {
		s.st.metadata.add(&s.st.metadata.trailer, md)
	}
	return err
}

// sentMetadata is the header and trailer metadata a call set, safe for concurrent use, since a bidi handler may
// set it from two goroutines.
type sentMetadata struct {
	mu      sync.Mutex
	header  metadata.MD
	trailer metadata.MD
}

// add adds md to *to, which is header or trailer: md itself the first time, so that setting metadata once
// costs nothing, and a copy that joins them after that. A map once stored is never changed, so that get's
// callers read it without the lock.
func (m *sentMetadata) add(to *metadata.MD, md metadata.MD) {
	if len(md) == 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if *to == nil {
		*to = md
	} else {
		*to = metadata.Join(*to, md)
	}
}

// get returns the header and trailer metadata set so far, nil when none was.
func (m *sentMetadata) get() (header, trailer metadata.MD) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.header, m.trailer
}
