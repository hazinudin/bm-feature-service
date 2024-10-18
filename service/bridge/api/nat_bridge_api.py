from arcgis.gis import GIS
from arcgis.geometry import Point
from arcgis.features import Feature
import os
from dotenv import load_dotenv


# Load the environment variable
load_dotenv('.env')
PORTAL_URL=os.getenv('PORTAL_URL')
PORTAL_USERNAME=os.getenv('PORTAL_USERNAME')
PORTAL_PWD=os.getenv('PORTAL_PWD')

FEATURE_SERVICE_NAME='National_Bridge'

# Login
portal = GIS(PORTAL_URL, PORTAL_USERNAME, PORTAL_PWD)

# Search feature layer
service_query = 'title: "{}" AND type: "Feature Service"'.format(FEATURE_SERVICE_NAME)
search_result = portal.content.search(query=service_query, max_items=5)
nbridge_lyr = search_result[0].layers[0]

def insert(features: list)->dict:
    """
    Add features to the National Bridge layer.
    """
    results = nbridge_lyr.edit_features(adds=features)

    return results

def delete(oids: list)->dict:
    """
    Delete features from National Bridge layer.
    """
    results = nbridge_lyr.edit_features(deletes=oids)

    return results

def update(features: list)->dict:
    """
    Update features from National Bridge layer.
    """
    results = nbridge_lyr.update_features(updates=features)

    return results

def get_active_oids(bridge_id: list, **kwargs)->int:
    """
    Return an GeoDatabase object ID from requested brige ID.
    """
    results = bridge_id_query(bridge_id, ['OBJECTID'], **kwargs)
    df = results.sdf

    return df['OBJECTID'][0]

def bridge_id_query(bridge_id: list, columns:None | list = None, bridge_id_col='BRIDGE_ID'):
    """
    Query National Bridge layer with added active date query
    """
    str_list = str(bridge_id).strip('[]')  # Strip string list
    query = f"{bridge_id_col} IN ({str_list})"
    query_results = _raw_query_with_active_date(query, columns)

    return query_results

def bridge_name_query(bridge_name: list, columns: None | list = None, bridge_name_col='BRIDGE_NAME'):
    """
    Query National Bridge layer using bridge name with added active date query
    """
    str_list = str(bridge_name).strip('[]')
    query = f"{bridge_name_col} IN ({str_list})"
    query_results = _raw_query_with_active_date(query, columns)

    return query_results

def _raw_query_with_active_date(query: str, columns: None | list = None, start_date_col='START_DATE', end_date_col='END_DATE'):
    """
    Execute raw query on National Bridge layer with added active date query
    """
    active_query = "({0} is NULL or {0} < CURRENT_TIMESTAMP) AND ({1} is NULL or {1} > CURRENT_TIMESTAMP)".format(start_date_col, end_date_col)

    query = query + " AND " + active_query

    if columns is None:
        query_results = nbridge_lyr.query(where=query, out_fields="*")
    else:
        query_results = nbridge_lyr.query(where=query, out_fields=columns)

    return query_results    

