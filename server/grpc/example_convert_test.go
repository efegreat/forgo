package grpc_test

import (
	"context"
	"strings"

	fgrpc "github.com/efegreat/forgo/server/grpc"

	testpb "google.golang.org/grpc/interop/grpc_testing"
)

// Echoer is business code: it imports no transport and no generated type. Its types share no field name with
// grpc.testing.SimpleRequest and SimpleResponse, so the functions below convert them by hand.
type Echoer struct{}

type EchoRequest struct {
	Text   string
	Repeat int `validate:"min=1,max=10"` // validated after the conversion
}

type Echo struct {
	Text string
}

func (*Echoer) Echo(_ context.Context, req EchoRequest) (Echo, error) {
	return Echo{Text: strings.Repeat(req.Text, req.Repeat)}, nil
}

// The conversions of a service belong in a file of their own beside the server's configuration, such as
// grpc_echo.go beside grpc.go, with a function per message: the configuration only names them, and business
// code never imports the generated package.

func echoRequestOf(m *testpb.SimpleRequest) (EchoRequest, error) {
	return EchoRequest{Text: string(m.GetPayload().GetBody()), Repeat: int(m.GetResponseSize())}, nil
}

func simpleResponseOf(e Echo) (*testpb.SimpleResponse, error) {
	return &testpb.SimpleResponse{Username: e.Text}, nil
}

func ExampleConvert() {
	conf := fgrpc.Config{
		Services: []fgrpc.ServiceConfig{{
			Desc: &testpb.TestService_ServiceDesc,
			Methods: []fgrpc.MethodConfig{{
				Method:  "UnaryCall",
				Handler: fgrpc.HandlerFor((*Echoer).Echo),
				// New checks the functions against the handler's types and the method's messages. A call runs
				// them without reflection.
				Convert: fgrpc.Convert(echoRequestOf, simpleResponseOf),
			}},
		}},
	}
	_ = conf
}
