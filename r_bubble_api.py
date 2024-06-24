'''
This script is purely for demo purposes.

In in the first function, (1) GET_custom_limit_objects, I've set up a call to our
bubble app with default parameters (cursor=0, limit = 100) and passed in limit=10000 in the call.

The return json still only returns a count of 100 items.
'''
import requests

#(1)
def GET_custom_limit_objects(obj: str, headers, cursor=0, limit = 100):
    url = f"https://ifish.tech/version-test/api/1.1/obj/{obj}"
    params = {'cursor': cursor, 'limit': limit}

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        return data

    except requests.RequestException as e:
        print(f"Error fetching all objects: {e}")
        return None

#our bubble credentials
headers = {
            'Content-type': 'application/json',
            'Authorization': 'Bearer 3d83175353e3af62cc0d4dd5c167a855',
        }

#The object (FISH) Disbursement_new has around 16.7k entries
return_json = GET_custom_limit_objects('(FISH) Disbursement_new', headers=headers, cursor=0, limit = 10000)

print(return_json) # the whole json

'''
This is how we've made it work, using cursor to loop over each 100 records. But this is exceptionally slow.

We are calling limit in correspondence with the API documentation, but still receiving 100 records.

I have removed out API keys from this second function so it will not run.
'''

#(2)
import requests
import json
from datetime import datetime

class BubbleAPI:
    def __init__(self, raw_url: str, apikey: str):
        self.raw_url = raw_url
        self.apikey = apikey
        self.headers = {
            'Content-type': 'application/json',
            'Authorization': f'Bearer {apikey}',
        }

    def merge_constraints(self, key_list, type_list, value_list):
        if len(key_list) != len(type_list) or len(key_list) != len(value_list):
            raise ValueError("Input lists must have the same length")

        return [
            {"key": key, "constraint_type": constraint_type, "value": value}
            for key, constraint_type, value in zip(key_list, type_list, value_list)
        ]

    def get_datetime(self):
        return datetime.today().strftime('%Y-%m-%d')

    def GET_single_object(self, obj: str, **kwargs):
        url = f"{self.raw_url}/{obj}"
        try:
            response = requests.get(url, headers=self.headers, params=kwargs)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Error fetching single object: {e}")
            return {}

    def GET_all_objects(self, obj: str, cursor=0, **kwargs):
        url = f"{self.raw_url}/{obj}"
        results = []
        params = {'cursor': cursor, **kwargs}

        try:
            while True:
                response = requests.get(url, headers=self.headers, params=params)
                response.raise_for_status()
                data = response.json()

                if "response" in data and "results" in data['response']:
                    results.extend(data['response']['results'])
                    remaining = data['response'].get('remaining', 0)
                    if remaining > 0:
                        cursor += 100
                        params['cursor'] = cursor
                    else:
                        break
                else:
                    break
        except requests.RequestException as e:
            print(f"Error fetching all objects: {e}")

        return {'response': {'results': results}}


#print(bubble_api.GET_all_objects('Loan')) will give us every Loan in the test database, exceeding 100, but only 100 at a time.