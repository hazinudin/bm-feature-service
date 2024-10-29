import os, sys

here = os.path.dirname(__file__)
sys.path.append(os.path.join(here, '..'))

import lrs_pb2, lrs_pb2_grpc
import logging
import grpc

def run():
    with grpc.insecure_channel("localhost:50052") as channel:
        stub = lrs_pb2_grpc.RoadNetworkStub(channel)
        routes_req = lrs_pb2.RouteRequests(routes=['01001'], output_shp='lrs_01001.shp')

        file_path = stub.DownloadAsSHP(routes_req)
        print(f"Request: {routes_req}, Response: {file_path}")

if __name__ == '__main__':
    logging.basicConfig()
    run()