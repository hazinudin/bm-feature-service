"""
GRPC Server for LRS feature service.
"""
import lrs_pb2_grpc
from servicer.servicer import RoadNetwork

import grpc

from concurrent import futures
import logging

import os
from dotenv import load_dotenv

load_dotenv('.env')
PORT = os.getenv('LRS_GRPC_PORT')
MAX_WORKERS = 10

logger = logging.getLogger(__name__)

def serve():
    port = PORT
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=MAX_WORKERS))
    lrs_pb2_grpc.add_RoadNetworkServicer_to_server(RoadNetwork(logger=logger), server)

    server.add_insecure_port("[::]:" + port)
    server.start()

    logger.info("LRS GRPC Feature Service started, listening on " + port)
    server.wait_for_termination()

if __name__ == "__main__":
    logging.basicConfig(level=0)
    serve()
