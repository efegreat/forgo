package grpc

import (
	"github.com/efegreat/forgo"

	"google.golang.org/grpc/reflection"
	reflectionv1 "google.golang.org/grpc/reflection/grpc_reflection_v1"
	reflectionv1alpha "google.golang.org/grpc/reflection/grpc_reflection_v1alpha"
)

// The v1alpha messages are deprecated, but tools still use them, and grpc-go's reflection.Register serves them
// too.
type (
	//lint:ignore SA1019 served for the tools that only speak v1alpha
	alphaRequest = reflectionv1alpha.ServerReflectionRequest
	//lint:ignore SA1019 served for the tools that only speak v1alpha
	alphaResponse = reflectionv1alpha.ServerReflectionResponse
)

// reflectionServices returns the services of gRPC server reflection, v1 and v1alpha, which list the
// services of services.
func reflectionServices(services reflection.ServiceInfoProvider) []ServiceConfig {
	opts := reflection.ServerOptions{Services: services}
	v1 := reflection.NewServerV1(opts)
	v1alpha := reflection.NewServer(opts)
	return []ServiceConfig{
		{
			Desc: &reflectionv1.ServerReflection_ServiceDesc,
			Methods: []MethodConfig{{
				Method: "ServerReflectionInfo",
				Handler: bidiStreamFactory(func(forgo.Resolver, buildEnv) (BidiStreamHandler[reflectionv1.ServerReflectionRequest, reflectionv1.ServerReflectionResponse], error) {
					return v1.ServerReflectionInfo, nil
				}),
			}},
		},
		{
			Desc: &reflectionv1alpha.ServerReflection_ServiceDesc,
			Methods: []MethodConfig{{
				Method: "ServerReflectionInfo",
				Handler: bidiStreamFactory(func(forgo.Resolver, buildEnv) (BidiStreamHandler[alphaRequest, alphaResponse], error) {
					return v1alpha.ServerReflectionInfo, nil
				}),
			}},
		},
	}
}
