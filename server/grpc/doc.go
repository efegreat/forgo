// Package grpc is forgo's gRPC server, built on grpc-go.
//
// A Config declares services and their methods the way http.Config declares routes: each method's handler
// is a method expression or a constructor resolved once, in New, and checked against the generated service
// descriptor, so a wrong method name, streaming kind or message type fails at startup. Every call, stream
// or not, is one operation with the base middlewares ID, Logger, Recorder, ErrorResponder and Recovery.
// Errors are answered with the gRPC code of their operation.Status, their Message, and an
// errdetails.ErrorInfo whose Reason is their Code.
//
// The package is named grpc, like grpc-go: alias one of them in files that import both, such as
// fgrpc "github.com/efegreat/forgo/server/grpc".
package grpc
