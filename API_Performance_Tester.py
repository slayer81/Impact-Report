# Import necessary modules
import os, requests, urllib3, json, sys
from datetime import timedelta, datetime
from pympler import asizeof
import pandas as pd
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
##################################################################
pd.options.mode.chained_assignment = None
run_start_time = datetime.now()
print("Start  :", str(datetime.now().time())[:-7])
##################################################################

lookbackDays = 90
dataDir = '~/OneDrive/Documents/Companies/Unravel Data/Work/SLA App/Data/High Impact'
urlsDict = {
    'bigquery': 'https://playground-dataproc.unraveldata.com'
}
platform = 'bigquery'
appTypes = ['bigquery']

breakString = '################################################################################'
spacer = '         '
entityList = [1, 0, 2, 6, 16, 15, 11]
baseLabels = ['id', 'clusterId', 'cents', 'status']
base_url = urlsDict[platform]
impactLabels = ['High', 'Medium']
statusMapDict = {
    'K': 'Killed',
    'F': 'Failed',
    'R': 'Running',
    'S': 'Success',
    'P': 'Pending',
    'U': 'Unknown',
    'W': 'Waiting'
}
appStatus = list(statusMapDict.keys())

end_time = datetime.now().astimezone().isoformat()

##################################################################
# Start Functions Block
##################################################################
def print_api_debug_info(f_status, f_data, f_message: str, f_result: str):
    f_message = "{}{} Failed to acquire {}.  Status code: {}".format(
        spacer, f_status, f_message, f_data.status_code
    )
    print(f_message)
    print("{}BEGIN DEBUG OUTPUT".format(spacer))
    print("{}\tURL:\t\t\t{}".format(spacer, f_data.request.url))
    print("{}\tHEADERS:\t\t{}".format(spacer, f_data.request.headers))
    if f_data.request.body.__contains__('username' or 'password='):
        print("{}\tBODY:\t\t\t**REDACTED FOR SECURITY**".format(spacer))
    else:
        print("{}\tBODY:\t\t\t{}".format(spacer, f_data.request.body))
    print("{}\tRESPONSE DATA:\t{}".format(spacer, f_data.json()))
    print("{}{}...".format(spacer, f_result))

##################################################################


##################################################################
def subtract_days_from_now(days):
    # Get the current date and time
    now = datetime.now()

    # Subtract the specified number of days from the current date
    new_date = now - timedelta(days=days)

    # Convert the new date to an ISO-formatted string
    new_date_str = new_date.isoformat()

    return new_date_str
##################################################################


##################################################################
def get_auth_token(platform):
    # Create a dictionary with the username and password stored in $USER_ENV
    authDict = {
        'username': os.getenv('unravel_username', None),
        'password': os.getenv('unravel_password', None)
    }
    endpoint_url = '{}/api/v1/signIn'.format(urlsDict[platform])

    # POST to the auth endpoint with the auth data
    response = requests.post(
        endpoint_url,
        data=authDict,
        verify=True
    )

    # Check the response status code
    if response.status_code != 200:
        print_api_debug_info('CRITICAL FAILURE!', response, 'authentication token', 'Exiting....')
        exit(1)

    # Parse the authentication token from the response body
    if response.json()['token']:
        auth_token = 'JWT {}'.format(response.json()['token'])
        print("{}Successfully generated authentication token.".format(spacer))
    else:
        print_api_debug_info('CRITICAL FAILURE!', response, 'authentication token', 'Exiting....')
        exit(1)

    # Return the authentication token
    return auth_token
##################################################################


##################################################################
def record_count(url, auth_token):
    # Construct the UnifiedSearch API URL
    search_url = url + '/api/v1/apps/unifiedsearch'

    params_dict = {'from': 0,
                   'size': 1,
                   'start_time': subtract_days_from_now(lookbackDays),
                   'end_time': end_time,
                   'executed_by_unravel': False,
                   'appStatus': appStatus,
                   'appTypes': appTypes
                   }

    # Request the number of available records
    response = requests.post(
        search_url,
        data=json.dumps(params_dict),
        verify=False,
        headers={'Authorization': auth_token,
                 'Accept': 'application/json',
                 'Content-Type': 'application/json'})

    # Check the response status code
    if response.status_code != 200:
        print_api_debug_info('CRITICAL FAILURE!', response, 'record count', 'Exiting....')
        exit(1)

#     # Navigate down looking for presence of 'totalRecords' k:v pair
#     if not isinstance(response.json(), dict):
#         print_api_debug_info('CRITICAL FAILURE!', response, 'record count', 'Exiting....')
#         exit(1)
#
#     if 'metadata' not in response.json().keys():
#         print_api_debug_info('CRITICAL FAILURE!', response, 'record count', 'Exiting....')
#         exit(1)
#
#     if 'totalRecords' not in response.json()['metadata'].keys():
#         print_api_debug_info('CRITICAL FAILURE!', response, 'record count', 'Exiting....')
#         exit(1)
#
#     if not isinstance(response.json()['metadata']['totalRecords'], int):
#         print_api_debug_info('CRITICAL FAILURE!', response, 'record count', 'Exiting....')
#         exit(1)

    # Verify the presence of 'totalRecords' k:v pair
    if not isinstance(response.json()['metadata']['totalRecords'], int):
        print_api_debug_info('CRITICAL FAILURE!', response, 'record count', 'Exiting....')
        exit(1)

    # Now we can capture the response value for 'totalRecords'
    responseCount = response.json()['metadata']['totalRecords']

    if responseCount == 0:
        # Clearly something went wrong, as the returned value was zero
        print("\n{}".format(breakString))
        print("{}Well, this is awkward......".format(spacer))
        print("{}In our attempt to get our first stage data (high level data of all your queries)".format(spacer))
        print("{}\t\twe got ZERO results. Exiting.......".format(spacer))
        print("{}\n".format(breakString))
        # Exit
        exit(1)

    # Check for a 'clusters' block to capture a cluster count
    if 'clusters' in response.json()['metadata'].keys():
        if isinstance(response.json()['metadata']['clusters'], dict
        ) and isinstance(len(response.json()['metadata']['clusters'].keys()), int):
            cCount = len(response.json()['metadata']['clusters'].keys())
            print("{}Total query count: {}, from {} clusters".format(spacer, responseCount, cCount))
    # No 'clusters' block detected, so skipping
    else:
        print("{}Total query count: {}".format(spacer, responseCount))
    return responseCount
##################################################################


##################################################################
def unified_search(url, auth_token, count: int):
    # Construct the UnifiedSearch API URL
    search_url = url + '/api/v1/apps/unifiedsearch'

    # set the count parameter to 250, for testing
    # count = 250

    # set the count parameter to 90%, for testing
    # count = round(count * 0.90)

    params_dict = {'from': 0,
                   'size': count,
                   'start_time': subtract_days_from_now(lookbackDays),
                   'end_time': end_time,
                   'executed_by_unravel': False,
                   'appStatus': appStatus,
                   'appTypes': appTypes
                   }

    print("{}Retrieving data for {} queries:".format(spacer, params_dict['size']))

    # Query UnifiedSearch API to get reference data on all queries
    response = requests.post(
        search_url,
        data=json.dumps(params_dict),
        verify=False,
        headers={'Authorization': auth_token,
                 'Accept': 'application/json',
                 'Content-Type': 'application/json'})

    # Check the response status code
    if response.status_code != 200:
        print_api_debug_info('CRITICAL FAILURE!', response, 'initial query data', 'Exiting....')
        exit(1)

    return response
##################################################################


##################################################################
def inefficient_apps(url, auth_token, count: int):
    headers_dict = {'Authorization': auth_token,
                    'Accept': 'application/json'}

    # params_dict = {'start_time': start_time,
    params_dict = {'start_time': subtract_days_from_now(lookbackDays),
                   'from': 0,
                   'size': count,
                   'end_time': end_time,
                   'entityType': str(entityList)}

    apps_url = url + '/api/v1/apps/events/inefficient_apps_newux/'
    search_response = requests.get(
        apps_url,
        verify=False,
        params=params_dict,
        headers=headers_dict).json()
#     print("Old method returned a data type of: {}".format(type(search_response)))
    print("\tOld method returned a dataset containing: {}".format(search_response.keys()))
    print("\tOld method returned a dataset count total of: {}".format(search_response['total']))
#     print("Old method returned aggregations: {}".format(search_response['aggregations']))
    print("\tOld method returned a results count of: {}".format(len(search_response['results'])))
#     print("Old method returned results of: {}".format(search_response['results']))
#     exit(1)
    return search_response
##################################################################


##################################################################
def main():
    # Get auth_token
    print("Stage 1: Generating authentication token")
    auth_token = get_auth_token(platform)

    # Set FQDN of our API endpoint
    base_url = urlsDict[platform]

    # Get a count of available queries
    print("Stage 2: Getting record count")
    recordCount = record_count(base_url, auth_token)

#     # Construct the UnifiedSearch API URL
#     search_url = base_url + '/api/v1/apps/events/inefficient_apps_newux/'

    # Test against Inefficient Apps API
    run_starttime = datetime.now()
    print("Start  (1):", str(datetime.now().time())[:-7])
    json_dict = inefficient_apps(base_url, auth_token, recordCount)
    print("Total execution time (inefficient_apps): {}".format(datetime.now() - run_starttime))

    # Test against Unified Search API
    run_starttime = datetime.now()
    print("Start  (2):", str(datetime.now().time())[:-7])
    query_data = unified_search(base_url, auth_token, recordCount)
    print("Total execution time (unified_search): {}".format(datetime.now() - run_starttime))
    exit(1)

##################################################################
# End Functions Block
##################################################################

if __name__ == "__main__":
    main()
