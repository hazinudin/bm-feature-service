import lrs_pb2, lrs_pb2_grpc
import logging
import zipfile
from .ms_graph_api.client import upload_file
from .lrs_api import *
from geopandas import read_file

OUTPUT_SHP_FOLDER=os.getenv('OUTPUT_SHP_FOLDER')

def service_logger():
    """
    Attach logger to the service end point.
    """
    def decorator(fn):
        def wrapper(*args, **kwargs):
            cls = args[0]
            results = fn(*args, **kwargs)
            cls.logger.info(f"{fn.__name__}, request {args[1]}")

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
        """
        routes = request.routes  # Route list
        output_name = request.output_shp  # Output SHP name

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

        shp_outdir = OUTPUT_SHP_FOLDER + '/' + output_file_name
        zip_outdir = OUTPUT_SHP_FOLDER + '/' + output_zip
        features = get_geom(routes=routes)  # Get route features from API
        gdf = read_file(features.to_geojson)  # Load into GeoPandas DataFrame

        gdf.to_file(shp_outdir, driver='ESRI Shapefile')

        # Pack into single ZIP file
        with zipfile.ZipFile(zip_outdir, 'w') as zipf:
            for _shp_comp in ['.cpg', '.dbf', '.prj', '.shp', '.shx']:
                file_path = os.path.join(OUTPUT_SHP_FOLDER, output_name + _shp_comp)
                zipf.write(file_path, output_name + _shp_comp)

        # Upload to PUPR SharePoint
        download_url = upload_file(file_path=zip_outdir, file_name=output_zip)

        return lrs_pb2.FilePath(path=download_url)
