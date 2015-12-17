from test import DiskManager
from azure.mgmt.common import SubscriptionCloudCredentials
from azure.mgmt.resource import ResourceManagementClient

import requests

def get_token_from_client_credentials(endpoint, client_id, client_secret):
    payload = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'resource': 'https://management.core.windows.net/',
    }
    response = requests.post(endpoint, data=payload).json()
    return response['access_token']

auth_token = get_token_from_client_credentials(
    endpoint='https://login.microsoftonline.com/[TENANT_ID]/oauth2/token',
    client_id='CLIENT_ID',
    client_secret='CLIENT_SECRET',
)

print "Recieved Token:"
print auth_token

# TODO: Replace this with your subscription id
subscription_id = 'subscription_id'
creds = SubscriptionCloudCredentials(subscription_id, auth_token)

resource_client = ResourceManagementClient(creds)

print "Got Client:"
print resource_client

manager = DiskManager(resource_client, 'sedouard-flockerdev', 'southeastasia')
manager.attach_or_detach_disk('somename', 'somelink')