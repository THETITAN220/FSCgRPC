package main

import (
	"context"
	"log"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/THETITAN220/FSCgRPC/gateway/proto"
)

var encoderClient pb.EncoderServiceClient

type server struct {
	pb.UnimplementedCoreAppServiceServer
}

func (s *server) ProcessQuery(ctx context.Context, req *pb.TextRequest) (*pb.QueryResponse, error) {
	log.Println("--> Backend logic executed (Cache Miss)")
	return &pb.QueryResponse{
		Answer: "This is the expensive backend answer for: " + req.GetText(),
		Cached: false,
	}, nil
}

func semanticCacheInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {

	textReq, ok := req.(*pb.TextRequest)
	if !ok {
		return handler(ctx, req)
	}

	log.Printf("[Interceptor] Received query: '%s'", textReq.GetText())

	log.Println("[Interceptor] Requesting embedding from Python...")
	embedRes, err := encoderClient.GetEmbedding(ctx, &pb.TextRequest{Text: textReq.GetText()})
	if err != nil {
		log.Printf("[Interceptor] Failed to get embedding: %v", err)
		return handler(ctx, req) 
	}

	log.Printf("[Interceptor] Success! Received vector with %d dimensions.", len(embedRes.GetVector()))

	return handler(ctx, req)
}

func main() {
	conn, err := grpc.Dial("localhost:50052", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to Python Encoder: %v", err)
	}
	defer conn.Close()
	
	encoderClient = pb.NewEncoderServiceClient(conn)
	log.Println("Connected to Python Encoder on :50052")

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer(
		grpc.UnaryInterceptor(semanticCacheInterceptor),
	)

	pb.RegisterCoreAppServiceServer(s, &server{})

	log.Printf("Go Gateway listening on %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}