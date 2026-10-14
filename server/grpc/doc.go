// Package grpc is the home of the gRPC server, which forgo does not ship yet.
//
// It holds the mapping of operation.Status to gRPC status codes the server will answer with, so that
// the statuses of operation.Error stay transport-agnostic while only HTTP serves them.
package grpc
