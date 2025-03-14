import lrs_pb2, lrs_pb2_grpc
import logging
import zipfile
from .ms_graph_api.client import upload_file
from .lrs_api import *

from geopandas import read_file, GeoDataFrame
from pandas import concat, DataFrame
from shapely import get_point, ops
import copy
import time

OUTPUT_SHP_FOLDER=os.getenv('OUTPUT_SHP_FOLDER')

def service_logger():
    """
    Attach logger to the service end point.
    """
    def decorator(fn):
        def wrapper(*args, **kwargs):
            cls = args[0]
            start_time = time.perf_counter()
            results = fn(*args, **kwargs)
            cls.logger.info(f"{fn.__name__}, request {args[1]}, total time: {time.perf_counter()-start_time}")

            return results

        wrapper.__name__ = fn.__name__
        return wrapper
    
    return decorator


class RoadNetwork(lrs_pb2_grpc.RoadNetworkServicer):
    def __init__(self, logger=None, *args, **kwargs):
        if logger is None:
            self.logger = logging.getLogger(__name__)
        else:
            self.logger = logger

        super(RoadNetwork, self).__init__(*args, **kwargs)
    
    @service_logger()
    def DownloadAsSHP(self, request, context):
        """
        Download requested routes as ESRI Shapefile and return a file path/download link.
        Generate route shapefile and route start/end point.
        """
        routes = request.routes  # Route list
        output_name = request.output_shp  # Output SHP name
        point_shp_prefix = 'AWAL_AKHIR_'

        if len(output_name) == 0:
            raise ValueError("Output file name is empty string.")
        
        if output_name.split('.')[1] != 'shp':
            raise ValueError("Output file format should be .shp not {0}".format(output_name.split('.')[1]))

        if '.shp' not in output_name:  # Append .shp file format
            output_name = str(output_name).upper()
            output_file_name = output_name + '.shp'
        else:
            output_name = output_name.split('.')[0]  # Get the name without file format
            output_name = str(output_name).upper()
            output_file_name = output_name + '.shp'
        
        # Output zip file name
        output_zip = output_name + '.zip'

        lrs_shp_outdir = OUTPUT_SHP_FOLDER + '/' + output_file_name
        point_shp_outdir = OUTPUT_SHP_FOLDER + '/' + point_shp_prefix + output_file_name
        zip_outdir = OUTPUT_SHP_FOLDER + '/' + output_zip
        features = routes_query(routes=routes, columns=['LINKID', 'LINK_NAME'])  # Get route features from API
        
        # Load into GeoPandas DataFrame
        line_gdf = read_file(features.to_geojson, crs=4326)
        
        # Convert MultiLineString into LineString
        line_gdf['geometry'] = line_gdf.apply(lambda x: ops.linemerge(x.geometry) if x.geometry.type != 'LineString' else x.geometry, axis=1)

        # Create end point for LRS
        point_gdf = GeoDataFrame()
        
        # Create start and end point for each route line geometry.
        for _, row in line_gdf.iterrows():
            route_geometry = copy.deepcopy(row['geometry'])
            for end_type in [('Awal ruas', 0), ('Akhir ruas', -1)]:
                end_str = end_type[0]
                end_ind = end_type[1]

                row.geometry = get_point(route_geometry, end_ind)
                row['KETERANGAN'] = end_str

                point_gdf = concat([point_gdf, copy.deepcopy(row.to_frame().T)])

        # Export to ShapeFile
        GeoDataFrame(point_gdf, crs=4326).to_file(point_shp_outdir, driver='ESRI Shapefile')
        line_gdf.to_file(lrs_shp_outdir, driver='ESRI Shapefile')

        # Pack into single ZIP file
        with zipfile.ZipFile(zip_outdir, 'w') as zipf:
            for _shp_comp in ['.cpg', '.dbf', '.prj', '.shp', '.shx']:
                # Polyline
                file_path = os.path.join(OUTPUT_SHP_FOLDER, output_name + _shp_comp)
                zipf.write(file_path, output_name + _shp_comp)

                # Point
                file_path = os.path.join(OUTPUT_SHP_FOLDER, point_shp_prefix + output_name + _shp_comp)
                zipf.write(file_path, 'awal_akhir_' + output_name + _shp_comp)

        # Upload to PUPR SharePoint
        download_url = upload_file(file_path=zip_outdir, file_name=output_zip)

        # Return SharePoint download URL
        return lrs_pb2.FilePath(path=download_url)
    
    @service_logger()
    def GetByRouteId(self, request, context):
        routes = request.routes  # Route list
        features = routes_query(routes=routes)  # Get route features from API

        return lrs_pb2.Routes(geojson=features.to_geojson)
