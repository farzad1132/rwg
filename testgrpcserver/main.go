package main

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/farzad1132/rwg/protobuf"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

type Server struct {
	protobuf.UnimplementedGRPCServerServer
}

func (s *Server) Testendpoint(ctx context.Context, req *protobuf.TestRequest) (*protobuf.TestResponse, error) {
	// just echo back the input string for testing
	resp := &protobuf.TestResponse{
		Output: "Hello, " + req.Input,
	}
	return resp, nil
}

func (s *Server) Run() error {
	opts := []grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Timeout: 120 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			PermitWithoutStream: true,
		}),
		//grpc.UnaryInterceptor(tracingInterceptor),
	}

	srv := grpc.NewServer(opts...)
	protobuf.RegisterGRPCServerServer(srv, s)

	fmt.Println("Successfully registered gRPC server")

	reflection.Register(srv)

	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		panic(fmt.Sprintf("failed to listen: %v", err))
	}

	return srv.Serve(lis)
}

func main() {
	s := &Server{}
	if err := s.Run(); err != nil {
		panic(err)
	}
}
