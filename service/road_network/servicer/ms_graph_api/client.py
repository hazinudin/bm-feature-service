from azure.identity import ClientSecretCredential
import requests
from dotenv import load_dotenv
import os
import json

load_dotenv('.env')
AZURE_TENANT_ID = os.getenv('AZURE_TENANT_ID')
AZURE_CLIENT_ID = os.getenv('AZURE_CLIENT_ID')
AZURE_CLIENT_SECRET = os.getenv('AZURE_CLIENT_SECRET')
AZURE_SCOPES = "https://graph.microsoft.com/.default"

SHAREPOINT_ROOT_ID = os.getenv('SHAREPOINT_ROOT_ID')
SHAREPOINT_SHP_FOLDER = os.getenv('SHAREPOINT_SHP_FOLDER')

azure_credential = ClientSecretCredential (
    tenant_id=AZURE_TENANT_ID,
    client_id=AZURE_CLIENT_ID,
    client_secret=AZURE_CLIENT_SECRET
)

def upload_file(file_path: str, file_name: str)->str:
    """
    Upload file into LRS Shape File folder within root drive in PUPR sharepoint.
    Returns SharePoint download URL.
    """
    access_token = azure_credential.get_token(AZURE_SCOPES).token

    # URL for file name under LRS ShapeFile folder.
    url = f"https://graph.microsoft.com/v1.0/sites/puprtes.sharepoint.com/drives/{SHAREPOINT_ROOT_ID}/items/{SHAREPOINT_SHP_FOLDER}:/{file_name}:/createUploadSession"

    # Read the file that will be uploaded.
    with open(file_path, "rb") as fb:
        fbyte = fb.read()
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    upload_session_payload = {
        "item": {
            "@microsoft.graph.conflictBehavior": "replace",  # Replace the old file with the new file.
            "name": file_name
        }
    }

    # Get the upload session URL
    upload_session_response = requests.post(url, headers=headers, data=json.dumps(upload_session_payload))

    if upload_session_response.status_code == 200:
        upload_session_url = json.loads(upload_session_response.content)['uploadUrl']
    else:
        raise upload_session_response.raise_for_status()

    # Start upload the file through upload session URL
    upload_headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Range": f"bytes 0-{len(fbyte)-1}/{len(fbyte)}",
        "Content-Length": str(len(fbyte))
    }

    response = requests.put(upload_session_url, headers=upload_headers, data=fbyte)

    if response.status_code == 201 or response.status_code == 200:
        # If file is created then delete the upload session
        requests.delete(upload_session_url)
    else:
        response.raise_for_status()
    
    # Return download link from SharePoint
    return json.loads(response.content)["@content.downloadUrl"]

# graph_client = GraphServiceClient(credentials=azure_credential, scopes=['https://graph.microsoft.com/.default'])

# async def get_root_id():
#     """
#     Get root drive ID.
#     """
#     get_item = await graph_client.drives.get()
#     child_id = get_item.values[0].id

#     root_drive = await graph_client.drives.by_drive_id(child_id).root.get()

#     return root_drive.id 

# async def create_folder(folder_name:str):
#     """
#     Create a folder from root drives in PUPR SharePoint.
#     """
#     request_body = DriveItem(
#         name = folder_name,
#         folder = Folder()
#     )

#     result = await graph_client.drives.items.children.post(request_body)

#     return result