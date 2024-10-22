from __future__ import print_function
import os, sys

here = os.path.dirname(__file__)
sys.path.append(os.path.join(here, '..'))

import bridge_master_pb2, bridge_master_pb2_grpc
import logging
import grpc


def run():
    print("Try to get bridge data")

    with grpc.insecure_channel("localhost:50051") as channel:
        stub = bridge_master_pb2_grpc.BridgeMasterStub(channel)
        req_name = bridge_master_pb2.NameRequests(name=['TOLONG_DIHAPUS'])

        query_response = stub.GetByName(req_name)
        print(f"Request: {req_name}, Response: {query_response.bridges}")
        
        insert_response = stub.Insert(query_response)  # Return editResult
        print(f"Insert response: {insert_response}")

        # retire_response = stub.Retire(query_response)  # Return editResult
        # print(f"Retire response: {retire_response}")

        # Query response from TOLONG_DIHAPUS
        for bridge in query_response.bridges:
            bridge.attributes.bridge_id = 'TEST_ID'

        update_response = stub.Update(query_response)
        print(f"Update response: {update_response}")

        request_oids = [bridge.attributes.objectid for bridge in query_response.bridges]
        delete_response = stub.Delete(bridge_master_pb2.ObjectIdRequests(objectids=request_oids))  # Delete editResult
        print(f"Delete response: {delete_response}")

if __name__ == '__main__':
    logging.basicConfig()
    run()
