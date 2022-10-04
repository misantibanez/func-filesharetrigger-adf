import logging
import azure.functions as func
import json
import adal
import requests

def main(event: func.EventHubEvent):
    log_event = event.get_body().decode()
    json_log_event = json.loads(log_event)
    filename_uri = json_log_event['records'][0]['uri']
    logging.info(f' {filename_uri}')
    param_foldername = filename_uri.split("?",1)[0].split("/",5)[4]
    logging.info(f' {param_foldername}')
    param_filename = filename_uri.split("?",1)[0].split("/",5)[5]
    logging.info(f' {param_filename}')

    WORKSPACENAME='synapse_workspace_name'
    PIPELINE_NAME='pipeline_name'

    TENANT_ID = 'tenant_id'
    CLIENT_ID = 'client_id'
    CLIENT_SECRET = 'secret_id'
    AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"

    body_runs = {
    'filename': '' + param_filename,
    'foldername': '' + param_foldername
    }

    context = adal.AuthenticationContext(AUTHORITY)
    token = context.acquire_token_with_client_credentials(resource="https://dev.azuresynapse.net/", client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
    logging.info(f' {token}')

    endpoint=f"https://{WORKSPACENAME}.dev.azuresynapse.net/pipelines/{PIPELINE_NAME}/createRun?api-version=2020-12-01"
    http_headers = {
        'Authorization': 'Bearer ' + token['accessToken']
    }
    response = requests.post(url=endpoint, headers=http_headers, json=body_runs).json
    logging.info(f'State: {response}')

