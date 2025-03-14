import bridge_master_pb2, bridge_master_pb2_grpc
import logging
import time
from google.protobuf.json_format import MessageToDict, ParseDict
from .api import nat_bridge_api as bridge_api
import copy


def service_logger():
    """
    Attach logger to the service end point.
    """
    def decorator(fn):
        def wrapper(*args, **kwargs):
            cls = args[0]

            start_time = time.perf_counter()
            results = fn(*args, **kwargs)
            end_time =  time.perf_counter()

            cls.logger.info(f"{fn.__name__}, total time: {end_time-start_time}, request {args[1]}")

            return results

        wrapper.__name__ = fn.__name__
        return wrapper
    
    return decorator

class BridgeMaster(bridge_master_pb2_grpc.BridgeMasterServicer):
    def __init__(self, logger=None, *args, **kwargs):
        if logger is None:
            self.logger = logging.getLogger(__name__)
        else:
            self.logger = logger

        super(BridgeMaster, self).__init__(*args, **kwargs)
    
    @service_logger()
    def GetByID(self, request, context):
        """
        Bridge Master data query using bridge ID.
        """
        req_id = request.bridge_ids
        response = bridge_api.bridge_id_query(req_id)
        bridges = bridge_master_pb2.Bridges()
        
        for feature in response.features:
            bridge = self.feature_to_pb_bridge(feature)
            bridges.bridges.append(bridge)

        return bridges
    
    @service_logger()
    def GetByName(self, request, context):
        """
        Bridge Master data query using bridge name.
        """
        req_name = request.name
        response = bridge_api.bridge_name_query(req_name)
        bridges = bridge_master_pb2.Bridges()
            
        for feature in response.features:
            bridge = self.feature_to_pb_bridge(feature)
            bridges.bridges.append(bridge)

        return bridges
    
    @service_logger()
    def GetByBridgeNumber(self, request, context):
        """
        Bridge Master data query using bridge number
        """
        req_num = request.number
        response = bridge_api.bridge_number_query(req_num)
        bridges = bridge_master_pb2.Bridges()

        for feature in response.features:
            bridge = self.feature_to_pb_bridge(feature)
            bridges.bridges.append(bridge)

        return bridges
    
    @service_logger()
    def GetBySpatialFilter(self, request, context):
        """
        Bridge Master data query using spatial filter.
        """
        geojson = request.geojson
        crs = request.crs
        response = bridge_api.bridge_spatial_query(geojson, crs)
        bridges = bridge_master_pb2.Bridges()

        for feature in response.features:
            bridge = self.feature_to_pb_bridge(feature)
            bridges.bridges.append(bridge)

        return bridges

    @service_logger()
    def Insert(self, request, context):
        """
        Implementation for Bridge Master data isnert using ArcGIS API for Python through GRPC Service.
        """
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
    
    @service_logger()
    def Update(self, request, context):
        """
        Implementation for Bridge Master data update using ArcGIS API for Python through GRPC Service.
        """
        # For holding bridges that has been updated with objectid
        # If requested bridges already has objectid, then it will be copied to here
        updated_bridges = bridge_master_pb2.Bridges()

        # Validate if all requested Bridge object has ObjectID
        # Iterate through all requested Bridge
        for bridge in request.bridges:
            self.fill_objectid(bridge, updated_bridges)
        
        bridge_dict = MessageToDict(updated_bridges,
                                    preserving_proto_field_name=True)
        
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
    
    @service_logger()
    def Delete(self, request, context):
        """
        Implementation for Bridge Master data delete using ArcGIS API for Python through GRPC Service.
        Delete Bridge Master data based on requestd objectids.
        """
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
    
    @service_logger()
    def Retire(self, request, context):
        """
        Implementation for Bridge Master data retire using ArcGIS API for Python through GRPC Service. 
        Update the END_DATE column and set it to current time.
        """
        # For holding bridges that has been updated with objectid
        # If requested bridges already has objectid, then it will be copied to here
        updated_bridges = bridge_master_pb2.Bridges()

        # Validate if all requested Bridge object has ObjectID
        # Iterate through all requested Bridge
        for bridge in request.bridges:
            updated_bridge = bridge_master_pb2.Bridge()  # Create a new empty Bridge object

            # ONLY SEND UPDATE REQUEST USING BRIDGE_ID, OBJECT_ID and END_DATE
            updated_bridge.attributes.bridge_id = bridge.attributes.bridge_id  # Fill in the bridge id
            updated_bridge.attributes.objectid = bridge.attributes.objectid  # Fill in the object id
            updated_bridge.attributes.end_date = time.strftime('%m/%d/%Y %H:%M:%S')  # Set the end date to current time

            self.fill_objectid(bridge, updated_bridges)  # Get the objectid for input bridge, if not available
        
        bridge_dict = MessageToDict(updated_bridges,
                                    preserving_proto_field_name=True)
        
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
    
    def fill_objectid(self, bridge: bridge_master_pb2.Bridge, target_bridges: bridge_master_pb2.Bridges)->bridge_master_pb2.Bridges:
        """
        Fill OBJECTID field of a protocol buffer Bridge object. If OBJECTID is already available then do nothing.
        """
        if (bridge.attributes.objectid == 0) and (bridge.attributes.bridge_id is None):
                raise(ValueError, "Bridges contain bridge without objectid and bridgeid. Could not proceed to update.")
        
        if (bridge.attributes.objectid == 0 ) and (bridge.attributes.bridge_id is not None):
            # Get all the existing bridges with the same bridge_id
            for feature in bridge_api.bridge_id_query(bridge_id=[bridge.attributes.bridge_id], columns=['BRIDGE_ID']):
                existing_bridge = self.feature_to_pb_bridge(feature)
                oid = existing_bridge.attributes.objectid  # Get the objectid
                bridge.attributes.objectid = oid  # Change the objectid of the input bridge
                
                # Append the updated requested bridge to the updated bridges
                target_bridges.bridges.append(copy.deepcopy(bridge))
        else:
            target_bridges.bridges.append(bridge)
        
        return target_bridges 
