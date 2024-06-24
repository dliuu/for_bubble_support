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

    def write_to_file(self, obj: str, api_type: str, filename: str):
        if api_type == 'single':
            raw_obj = self.GET_single_object(obj)
        elif api_type == 'all':
            raw_obj = self.GET_all_objects(obj)
        else:
            print('Error: Please input a valid api_type')
            return

        json_obj = json.dumps(raw_obj, indent=4)
        day = self.get_datetime()

        with open(f"{day}-{filename}.json", "w") as outfile:
            outfile.write(json_obj)

    def write_snapshot_files(self):
        # Example usage:
        snapshot_items = [
            ('Loan', 'all', 'test-loans-snapshot'),
            ('Loan Application', 'all', 'test-loan-applications-snapshot'),
            ('(FISH) Contact', 'all', 'test-contacts-snapshot'),
            ('(FISH) Company', 'all', 'test-companies-snapshot'),
            ('(FISH) Property', 'all', 'test-properties-snapshot'),
            ('(FISH) Payments', 'all', 'test-payments-snapshot'),
            ('(FISH) Funding', 'all', 'test-funding-snapshot'),
            ('(FISH) Disbursement_new', 'all', 'test-disbursements-snapshot'),
            ('(FISH)_Draw', 'all', 'test-draws-snapshot'),
            ('Loan Extension', 'all', 'test-loan-extensions-snapshot'),
            ('Loan Payoff', 'all', 'test-payoffs-snapshot'),
            ('User', 'all', 'test-users-snapshot')
        ]

        for obj, api_type, filename in snapshot_items:
            self.write_to_file(obj, api_type, filename)


# Example usage:
raw_url = 'https://ifish.tech/version-test/api/1.1/obj'
apikey = ''
bubble_api = BubbleAPI(raw_url, apikey)
#print(bubble_api.GET_all_objects('Loan'))

# Uncomment to test fetching and writing snapshot files
# bubble_api.write_snapshot_files()
