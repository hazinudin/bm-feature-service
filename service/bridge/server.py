"""
GRPC Server for National Bridge feature service.
"""
import api.nat_bridge_api as bridge_api

import bridge_master_pb2, bridge_master_pb2_grpc
import grpc
from google.protobuf.json_format import MessageToDict, ParseDict
from grpc_reflection.v1alpha import reflection

from concurrent import futures
import logging

import os
from dotenv import load_dotenv

load_dotenv('.env')
PORT = os.getenv('BRIDGE_MASTER_GRPC_PORT')
MAX_WORKERS = 10


class BridgeMaster(bridge_master_pb2_grpc.BridgeMasterServicer):
    def GetByID(self, request, context):
        req_id = request.bridge_ids
        response = bridge_api.bridge_id_query(req_id)
        bridges = bridge_master_pb2.Bridges()
        
        for feature in response.features:
            bridge = self.feature_to_pb_bridge(feature)
            bridges.bridges.append(bridge)

        return bridges
    
    def GetByName(self, request, context):
        req_name = request.name
        response = bridge_api.bridge_name_query(req_name)
        bridges = bridge_master_pb2.Bridges()
            
        for feature in response.features:
            bridge = self.feature_to_pb_bridge(feature)
            bridges.bridges.append(bridge)

        return bridges
       
    def Insert(self, request, context):
        bridge_dict = MessageToDict(request,
                                    preserving_proto_field_name=True)
        
        results = bridge_api.insert(bridge_dict['bridges'])
        results_pb = list()
       
        for result in results['addResults']:
            result_pb = bridge_master_pb2.Result()
            result_pb.objectid = result['objectId']
            result_pb.success = result['success']

            if result.get('globalId') is not None:
                result_pb.global_id = result.get('globalId')

            results_pb.append(result_pb)

        return bridge_master_pb2.EditResults(add_results = results_pb)
    
    def Update(self, request, context):
        bridge_dict = MessageToDict(request,
                                    preserving_proto_field_name=True)
        
        # Validate if all Bridge object has ObjectID
        if not all([bridge.attributes.objectid is not None for bridge in request.bridges]):
            raise(ValueError, "Bridges contain bridge without objectid.")

        results = bridge_api.update(bridge_dict['bridges'])
        results_pb = list()
       
        for result in results['updateResults']:
            result_pb = bridge_master_pb2.Result()
            result_pb.objectid = result['objectId']
            result_pb.success = result['success']

            if result.get('globalId') is not None:
                result_pb.global_id = result.get('globalId')

            results_pb.append(result_pb)

        return bridge_master_pb2.EditResults(update_results = results_pb)
    
    def Delete(self, request, context):
        oids = [oid for oid in request.objectids]
        results = bridge_api.delete(oids)
        results_pb = list()
       
        for result in results['deleteResults']:
            result_pb = bridge_master_pb2.Result()
            result_pb.objectid = result['objectId']
            result_pb.success = result['success']

            if result.get('globalId') is not None:
                result_pb.global_id = result.get('globalId')

            results_pb.append(result_pb)

        return bridge_master_pb2.EditResults(delete_results = results_pb)
    
    @staticmethod
    def feature_to_pb_bridge(feature):
        attr = feature.attributes
        geom = feature.geometry
        attr = {k.lower(): v for k, v in attr.items()}

        bridge = bridge_master_pb2.Bridge()
        bridge_attr = bridge.attributes
        bridge_geom = bridge.geometry

        if geom is not None:
            ParseDict(geom, bridge_geom)

        for field in bridge_attr.DESCRIPTOR.fields:
            field_name = field.name
            val = attr.get(field_name)

            if val is None:
                continue

            field_class = type(bridge_attr.__getattribute__(field_name))
            bridge_attr.__setattr__(field_name, field_class(val))

        return bridge


def serve():
    port = PORT
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=MAX_WORKERS))
    bridge_master_pb2_grpc.add_BridgeMasterServicer_to_server(BridgeMaster(), server)
    
    SERVICE_NAMES = (
        bridge_master_pb2.DESCRIPTOR.services_by_name["BridgeMaster"].full_name,
        reflection.SERVICE_NAME,
    )
    reflection.enable_server_reflection(SERVICE_NAMES, server)

    server.add_insecure_port("[::]:" + port)
    server.start()

    print("Server started, listening on " + port)
    server.wait_for_termination()

if __name__ == "__main__":
    logging.basicConfig()
    serve()
