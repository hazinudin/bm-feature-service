"""
GRPC Server for Bridge feature service.
"""
# Bridge master proto generated code
import bridge_master_pb2, bridge_master_pb2_grpc
from servicer.master_data.servicer import BridgeMaster

import grpc
from grpc_reflection.v1alpha import reflection

from concurrent import futures
import logging

import os
from dotenv import load_dotenv

load_dotenv('.env')
PORT = os.getenv('BRIDGE_MASTER_GRPC_PORT')
MAX_WORKERS = 10

logger = logging.getLogger(__name__)

def serve():
    port = PORT
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=MAX_WORKERS))
    bridge_master_pb2_grpc.add_BridgeMasterServicer_to_server(BridgeMaster(logger=logger), server)
    
    SERVICE_NAMES = (
        bridge_master_pb2.DESCRIPTOR.services_by_name["BridgeMaster"].full_name,
        reflection.SERVICE_NAME,
    )
    reflection.enable_server_reflection(SERVICE_NAMES, server)

    server.add_insecure_port("[::]:" + port)
    server.start()

    logger.info("Server started, listening on " + port)
    server.wait_for_termination()

if __name__ == "__main__":
    logging.basicConfig(level=0)
    serve()
