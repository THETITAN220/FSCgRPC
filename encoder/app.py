import time
import grpc
from concurrent import futures
from sentence_transformers import SentenceTransformer

import api_pb2
import api_pb2_grpc

print("Loading SentenceTransformer model...")
model = SentenceTransformer('all-MiniLM-L6-v2')
print("Model loaded successfully.")

class EncoderService(api_pb2_grpc.EncoderServiceServicer):
    def GetEmbedding(self, request, context):
        """
        Receives a TextRequest, encodes the text, and returns an EmbeddingResponse.
        """
        start_time = time.time()
        
        raw_text = request.text
        
        vector = model.encode(raw_text).tolist()
        
        elapsed_ms = (time.time() - start_time) * 1000
        print(f"Encoded query in {elapsed_ms:.2f}ms: '{raw_text}'")
        
        return api_pb2.EmbeddingResponse(vector=vector)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    api_pb2_grpc.add_EncoderServiceServicer_to_server(EncoderService(), server)
    
    server.add_insecure_port('[::]:50052')
    server.start()
    print("Python Encoder Service running on port 50052...")
    
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
