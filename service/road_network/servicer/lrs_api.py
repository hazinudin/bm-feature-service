from arcgis.gis import GIS
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
    str_list = str(routes).strip('[]')
    query = f"{routeid_col} IN ({str_list})"
    query_results = _raw_query_with_active_date(query, columns)

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