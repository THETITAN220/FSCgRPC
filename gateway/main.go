package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"
	"math"
	"net"
	"strconv"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/THETITAN220/FSCgRPC/gateway/proto"
)

var encoderClient pb.EncoderServiceClient
var rdb *redis.Client

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

func float32ToByte(f []float32) []byte {
	bytes := make([]byte, len(f)*4)
	for i, v := range f {
		binary.LittleEndian.PutUint32(bytes[i*4:], math.Float32bits(v))
	}
	return bytes
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

	queryStr := textReq.GetText()
	log.Printf("[Interceptor] Received query: '%s'", queryStr)

	embedRes, err := encoderClient.GetEmbedding(ctx, &pb.TextRequest{Text: queryStr})
	if err != nil {
		log.Printf("[Interceptor] Encoder failed: %v. Bypassing cache.", err)
		return handler(ctx, req)
	}

	vectorBytes := float32ToByte(embedRes.GetVector())
	
	redisQuery := "*=>[KNN 1 @vector $query_vec AS score]"

	res, err := rdb.Do(ctx,
		"FT.SEARCH", "idx:cache", redisQuery,
		"PARAMS", "2", "query_vec", vectorBytes,
		"RETURN", "3", "response", "score", "text",
		"DIALECT", "2",
	).Result()

	if err != nil {
		log.Printf("[Interceptor] REDIS SEARCH ERROR: %v", err)
	} else {
		resSlice, ok := res.([]interface{})
		if !ok {
			log.Printf("[Interceptor] Parsing Error: Could not cast Redis response. Type: %T", res)
		} else if len(resSlice) > 0 {

			var totalHits int64
			switch v := resSlice[0].(type) {
			case int64:
				totalHits = v
			case int:
				totalHits = int64(v)
			}

			if totalHits > 0 && len(resSlice) >= 3 {
				matchData, ok := resSlice[2].([]interface{})
				if !ok {
					log.Printf("[Interceptor] Parsing Error: matchData cast failed. Type: %T", resSlice[2])
				} else {
					var scoreStr, cachedResponse, matchedText string

					for i := 0; i < len(matchData); i += 2 {
						key := fmt.Sprintf("%s", matchData[i])
						val := fmt.Sprintf("%s", matchData[i+1])

						if key == "score" {
							scoreStr = val
						} else if key == "response" {
							cachedResponse = val
						} else if key == "text" {
							matchedText = val
						}
					}

					scoreFloat, err := strconv.ParseFloat(scoreStr, 64)
					// Threshold: 0.20 means it must be at least 80% similar
					similarityThreshold := 0.20

					if err == nil && scoreFloat <= similarityThreshold {
						log.Printf("[Interceptor] 🟢 CACHE HIT! Distance: %f (Matched against: '%s')", scoreFloat, matchedText)
						return &pb.QueryResponse{
							Answer: cachedResponse,
							Cached: true,
						}, nil
					} else {
						log.Printf("[Interceptor] 🟡 WEAK MATCH (Distance: %f, Closest neighbor: '%s'). Rejecting cache.", scoreFloat, matchedText)
					}
				}
			}
		}
	}

	log.Println("[Interceptor] 🔴 CACHE MISS. Executing heavy backend logic...")
	resp, err := handler(ctx, req)
	if err != nil {
		return resp, err
	}

	queryResp := resp.(*pb.QueryResponse)

	hash := sha256.Sum256([]byte(queryStr))
	safeHash := hex.EncodeToString(hash[:])
	newCacheKey := fmt.Sprintf("cache:%s", safeHash)

	err = rdb.HSet(ctx, newCacheKey, map[string]interface{}{
		"text":     queryStr,
		"response": queryResp.GetAnswer(),
		"vector":   vectorBytes,
	}).Err()

	if err != nil {
		log.Printf("[Interceptor] ❌ Failed to save to Redis: %v", err)
	} else {
		log.Printf("[Interceptor] ✅ Saved new query to Redis (Key: %s).", newCacheKey)
	}

	return resp, err
}

func main() {
	conn, err := grpc.NewClient("localhost:50052", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to Python Encoder: %v", err)
	}
	defer conn.Close()

	encoderClient = pb.NewEncoderServiceClient(conn)
	log.Println("Connected to Python Encoder on :50052")

	rdb = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Protocol: 2,
	})
	log.Println("Connected to Redis Vector Store on :6379")

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
