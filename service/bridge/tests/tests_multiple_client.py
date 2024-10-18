from __future__ import print_function
import os, sys

from multiprocessing import Event, Process
import time

here = os.path.dirname(__file__)
sys.path.append(os.path.join(here, '..'))

import bridge_master_pb2, bridge_master_pb2_grpc
import grpc


def task(event):
    event.wait()

    with grpc.insecure_channel("localhost:50051") as channel:
        pid = os.getpid()
        stub = bridge_master_pb2_grpc.BridgeMasterStub(channel)
        req_id = bridge_master_pb2.BridgeIdRequests(bridge_ids=['4000068'])

        query_response = stub.GetByID(req_id)  # Returns Bridges
        
        # Create edited Bridge for insertion
        edited = query_response.bridges[0]  # Bridge object
        edited.attributes.bridge_name = 'TOLONG_DIHAPUS'
               
        insert_response = stub.Insert(bridge_master_pb2.Bridges(bridges=[edited]))  # Return editResult

        print(f"Insert response: {insert_response}, pid: {pid}, finished: {time.time()}")

        # request_oids = [result.objectid for result in insert_response.add_results]
        # delete_response = stub.Delete(bridge_master_pb2.ObjectIdRequests(objectids=request_oids))  # Delete editResult

        # print(f"Delete response: {delete_response}, pid: {pid}, finished: {time.time()}")

def run():
    print("Simulate multiple concurrent client requests")

    e = Event()

    for _ in range(10):
        p = Process(target=task, args=(e, ))
        p.start()

    e.set()        
    

if __name__ == '__main__':
    run()
