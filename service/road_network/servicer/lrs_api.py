from arcgis.gis import GIS
from arcgis.features import FeatureSet
from pandas import concat
import os
from dotenv import load_dotenv


load_dotenv('.env')
PORTAL_URL=os.getenv('PORTAL_URL')
PORTAL_USERNAME=os.getenv('PORTAL_USERNAME')
PORTAL_PWD=os.getenv('PORTAL_PWD')

FEATURE_SERVICE_NAME='BinaMargaLRS'

# Login
portal = GIS(PORTAL_URL, PORTAL_USERNAME, PORTAL_PWD)

# Search feature layer
service_query = 'title: "{}" AND type: "Map Service"'.format(FEATURE_SERVICE_NAME)
search_result = portal.content.search(query=service_query, max_items=5)
lrs_lyr = search_result[0].layers[0]

def routes_query(routes: list, columns: None | list = None, routeid_col='LINKID'):
    """
    Query Road Network LRS for route data.
    """
    def _execute_query(route_list):
        str_list = str(route_list).strip('[]')
        query = f"{routeid_col} IN ({str_list})"
        results = _raw_query_with_active_date(query, columns)

        return results

    if len(routes) <= 20:
        query_results = _execute_query(routes)
    else:
        query_results = None  # FeatureSet results
        chunk = [0, 20]  # Initial part chunk

        while chunk[0] < len(routes):
            # Execute query
            if chunk[1] > len(routes):
                chunk_result = _execute_query(routes[chunk[0]:len(routes)])
            else:
                chunk_result = _execute_query(routes[chunk[0]:chunk[1]])

            if query_results is None:
                query_results = chunk_result
            else:
                query_results = FeatureSet.from_dataframe(concat([query_results.sdf, chunk_result.sdf]))

            chunk = [x + 20 for x in chunk]  # Update chunk

    return query_results

def get_geom(routes: list):
    """
    Query Road Network LRS for route geometry.
    """
    return routes_query(routes, columns=["LINKID"])

def _raw_query_with_active_date(query: str, columns: None | list = None, start_date_col='FROMDATE', end_date_col='TODATE'):
    """
    Execute raw query on National Bridge layer with added active date query
    """
    active_query = "({0} is NULL or {0} < CURRENT_TIMESTAMP) AND ({1} is NULL or {1} > CURRENT_TIMESTAMP)".format(start_date_col, end_date_col)

    query = query + " AND " + active_query

    if columns is None:
        query_results = lrs_lyr.query(where=query, out_fields="*", out_sr=4326)
    else:
        query_results = lrs_lyr.query(where=query, out_fields=columns, out_sr=4326)

    return query_results