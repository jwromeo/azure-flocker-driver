import requests


class AuthToken(object):

    device_path = ''
    lun = ''

    def __init__():
        return

    @staticmethod
    def get_token_from_client_credentials(subscription_id,
                                          tenant_id,
                                          client_id,
                                          client_secret):
        payload = {
            'grant_type': 'client_credentials',
            'client_id': client_id,
            'client_secret': client_secret,
            'resource': 'https://management.core.windows.net/',
        }
        print('making request: ' +
              'https://login.microsoftonline.com/' +
              tenant_id + '/oauth2/token')
        response = requests.post('https://login.microsoftonline.com/' +
                                 tenant_id +
                                 '/oauth2/token', data=payload).json()

        if 'access_token' not in response:
            raise Exception(response['error_description'])

        return response['access_token']
