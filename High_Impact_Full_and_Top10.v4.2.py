##################################################################
# WARNING! This script is provided WITHOUT WARRANTY
# While we have taken reasonable measures to ensure execution safety,
# we strongly advise that you DO NOT RUN IT ON PRODUCTION SYSTEMS
# until you have tested it against your corporate polices.
# Author:           Scott Lutz
# Org:              Unravel Data
# Version:          4.1
# Release Date:     6 April, 2023
##################################################################

# Output report contains these fields, unique per query:
# "clusterId"           ==> The identifier of the parent cluster
# "id"                  ==> Pipeline/query identifier
# "Unravel UI link"     ==> Direct link to Unravel UI
# "Cost (USD)"          ==> Query cost, in USD
# "Status"              ==> Query execution status
# "Impact Value"        ==> Sum of all query Impact score values
# "Insights Count"      ==> The total number of "Insights"      # Removed, due to no real value
# "High Impact"         ==> "High Impact" count
# "Medium Impact"       ==> "Medium Impact" count
# "Low Impact"          ==> "Low Impact" count
# "Instance Count"      ==> Sum of all insight instances
# "Insights"            ==> Comma-separated list of unique "Insights" with (Impact Value)
##################################################################

# Import necessary modules
import os, requests, urllib3, json, sys
from datetime import timedelta, datetime
from ydata_profiling import ProfileReport
from pympler import asizeof
import pandas as pd

# Debug mode flag. If active, there is a LOT more verbosity
# Set to either True or False.
debug = False

# Disable warnings from urllib3 about unverified HTTPS requests
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Prevent interpreter complaining about wobbly file handles
pd.options.mode.chained_assignment = None

# Record the start time of the script
run_start_time = datetime.now()
print("Start  :", str(datetime.now().time())[:-7])
##################################################################

# Define the required configurations for the script
# All items in this block need to be configured before first run
##################################################################

# lookbackDays => Define the number of days to look back
lookbackDays = 90

# dataDir => Define the directory location to save the report to
# dataDir = '/mnt/data/'
dataDir = '~/OneDrive/Documents/Companies/Unravel Data/Work/SLA App/Data/High Impact'

# Define the URLs for each supported platform
urlsDict = {
    # 'bigquery': 'https://unravel.yourdomain.com:3000'
    'bigquery': 'https://playground-dataproc.unraveldata.com'
}

# platform => Define the target platform to query
# Choices:
# 1. AWS EMR        ==> 'emr'
# 2. GCP Dataproc   ==> 'dataproc'
# 3. GCP Bigquery   ==> 'bigquery'
# 4. Databricks     ==> 'databricks'
platform = 'bigquery'

# The activity types to search for. You can remove any not required
# appTypes = ['spark', 'impala', 'hive', 'mr', 'tez', 'bigquery']
appTypes = ['bigquery']

# You can use Unravel UI credentials to generate auth_tokens at runtime.
# Example format for storing Unravel credentials in your *nix/macOS profile:
# export unravel_username=username
# export unravel_password=password

# End of customer-defined config items
##################################################################
breakString = '################################################################################'
spacer = '         '
entityList = [1, 0, 2, 6, 16, 15, 11]
baseLabels = ['id', 'clusterId', 'cents', 'status']

# Set FQDN of our API endpoint
base_url = urlsDict[platform]

# The impact labels for the report
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
def get_impact_label(value: int):
    rating = ''
    if value > 70:
        rating = 'High'
    elif (value > 30) and (value < 71):
        rating = 'Medium'
    else:
        rating = 'Low'

    return rating
##################################################################


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

    # Navigate down looking for presence of 'totalRecords' k:v pair
    if not isinstance(response.json(), dict):
        print_api_debug_info('CRITICAL FAILURE!', response, 'record count', 'Exiting....')
        exit(1)

    if 'metadata' not in response.json().keys():
        print_api_debug_info('CRITICAL FAILURE!', response, 'record count', 'Exiting....')
        exit(1)

    if 'totalRecords' not in response.json()['metadata'].keys():
        print_api_debug_info('CRITICAL FAILURE!', response, 'record count', 'Exiting....')
        exit(1)

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
def get_entitiesV2(i_base_url, auth_token, df):
    entitiesV2List = []
    headers_dict = {'Authorization': auth_token,
                    'Accept': 'application/json'}

    print("{}Retrieving Entity data for {} records".format(spacer, df.shape[0]))

    # The following section is hard coded for Unravel for Google Bigquery
    # Please advise if you have requirements for other supported platforms
    if debug:
        print("{}Processing {} records".format(spacer, df.shape[0]))
        print("{}Number of variables: {}".format(spacer, df.shape[1]))
        print("{}Dataframe columns: {}".format(spacer, list(df.columns)))
        out_file = dataDir + '/Get-Entities-Source_df-{}.csv'.format(
            datetime.now().strftime('%Y%m%d_%H%M%S.%f')[:-7]
        )
        df.to_csv(out_file, index=False)

    # queryCount = df.shape[0]
    for i in df.index:
        url = i_base_url + '/api/v1/bigquery/{}/{}/analysis'.format(df['clusterId'][i], df['id'][i])
        link_url = i_base_url + '/#/app/application/apptype/bigquery?execId={}&projectId={}'.format(
            df['id'][i], df['clusterId'][i]
        )
        entities_response = requests.get(url, verify=False, headers=headers_dict)

        # Check the response status code
        if entities_response.status_code != 200:
            # Print debug info
            print_api_debug_info('WARNING!', response, 'entity metadata', 'Skipping....')

            # Exit this loop iteration if we can't collect entity metadata
            continue

        # Exit this loop iteration if no data contained in query response
        # if len(entities_response.text) == 0:
        if not isinstance(entities_response.json(), dict) or len(entities_response.text) == 0:
            continue

        # Now serialise our response data
        entities = json.loads(entities_response.text)

        # Exit this loop iteration if no "insightsV2' data contained in query response
        if len(entities['insightsV2']) == 0:
            continue

        # Initialise dict to hold all data for each report query
        entities_dict = {
            'clusterId': df['clusterId'][i],
            'id': df['id'][i],
            'Unravel UI link': link_url,
            'Cost (USD)': round(df['cents'][i])/100,
            'Status': statusMapDict[df['status'][i]],
            'Impact Value': 0,
            # 'Insights Count': 0,
            'High Impact': 0,
            'Medium Impact': 0,
            'Low Impact': 0,
            'Instance Count': 0
        }

        for ent in entities['insightsV2']:
            # Every key is a unique Insight name, so collect all 'categories''key' values
            insights_list = list(ent['categories'].keys())
            # entities_dict['Insights Count'] = len(insights_list)
            impact_value = 0
            ext_insights_labels = []
            for x in insights_list:
                impact_value = impact_value + int(ent['categories'][x]['impact'])

                # Add Impact score for Insight to Insight label
                label_name = '{} ({})'.format(x, impact_value)
                ext_insights_labels.append(label_name)

                # Summation of 'Impact Value'
                entities_dict['Impact Value'] = entities_dict['Impact Value'] + impact_value

                # Impact Labels
                # Assign Impact Label to 'Impact Value' and then increment label counter
                impact_label = get_impact_label(int(impact_value))
                if impact_label == 'High':
                    entities_dict['High Impact'] += 1
                elif impact_label == 'Medium':
                    entities_dict['Medium Impact'] += 1
                else:
                    entities_dict['Low Impact'] += 1

                # 'Instance Count'
                # Keep tally of all potential inefficiency points
                entities_dict['Instance Count'] = entities_dict['Instance Count'] + len(ent['categories'][x]['instances'])

            entities_dict['Insights'] = ext_insights_labels

            if debug:
                print("{}Categories: count: {}, values: {}".format(spacer, len(insights_list), str(insights_list)))
                print("{}Here is the results of our dictionary:\n\t{}".format(spacer, entities_dict))

        # Filter out rows if total query 'Impact Value' is less than 30
        if entities_dict['Impact Value'] < 30:
            if debug:
                print("{}Nothing to see here. Discarding this query".format(spacer))
            entities_dict = {}
            continue

        entitiesV2List.append(entities_dict)
        entities_dict = {}

    # Sort results on 'Impact Value', DESC to generate "Top 10" report
    entitiesV2List_sorted = sorted(entitiesV2List, key=lambda l: l['Impact Value'], reverse=True)
    entitiesV2List = []
    print("{}Our EntitiesV2 sorted list has a size of: {} kB".format(
        spacer, round(asizeof.asizeof(entitiesV2List_sorted) / 1024))
    )

    return entitiesV2List_sorted
##################################################################


##################################################################
def main():
    # Get auth_token
    print("Stage 1: Generating authentication token")
    auth_token = get_auth_token(platform)

    # Get a count of available queries
    print("Stage 2: Getting record count")
    recordCount = record_count(base_url, auth_token)
    # recordCount = 19000

    # Get Query data from UnifiedSearch API
    discount = 5
    print("Stage 3: Getting query IDs")
    query_data = unified_search(base_url, auth_token, recordCount)
    while not isinstance(query_data.json(), dict):
        newRecordCount = round(recordCount * ((100 - discount) / 100))
        print("{}Incorrect data type returned. Reducing requested record count by {}%".format(spacer, discount))
        query_data = unified_search(base_url, auth_token, newRecordCount)
        discount += 5

    if newRecordCount:
        print("{}Our API query of {} results set has a size of: {} kB".format(
            spacer, newRecordCount, round(asizeof.asizeof(query_data.json()) / 1024)))
    else:
        print("{}Our API query of {} results set has a size of: {} kB".format(
            spacer, recordCount, round(asizeof.asizeof(query_data.json()) / 1024)))
    if 'results' not in query_data.json().keys() or len(query_data.json()['results']) == 0:
        print("{}Unfortunately, we received no required data".format(spacer))
        print("{}Response field \"results\" missing from API response".format(spacer))
        print("{}API response contained these fields: {}".format(spacer, list(query_data.json().keys())))
        print("{}Exiting, sorry......".format(spacer))
        exit(1)

    # Store API response as DataFrame
    temp_df = pd.DataFrame(query_data.json()['results'])
    print("{}Our Query IDs dataframe has a size of: {} kB".format(spacer, round((asizeof.asizeof(temp_df) / 1024))))
    # exit()

    # Drop all unwanted columns
    print("Stage 4: Extracting required fields from API response data")
    df = temp_df.drop(columns=[col for col in temp_df if col not in baseLabels])
    print("{}Our results dataframe has a size of: {} kB".format(spacer, round(asizeof.asizeof(df) / 1024)))
    temp_df.loc[:] = None

    if debug:
        print("{}Unified Search dataframe columns: {}".format(spacer, list(df.columns)))

    # Get entity data for queries in dataframe
    print("Stage 5: Begin collecting query entity data")
    insights_df = pd.DataFrame(get_entitiesV2(base_url, auth_token, df))
    print("{}The entire entities dataframe has a size of: {} kB".format(
        spacer, round(asizeof.asizeof(insights_df) / 1024))
    )
    print("{}Collection of query entity data completed".format(spacer))

    if debug:
        print("{}Insights df columns: {}".format(spacer, list(insights_df.columns)))
        out_file = dataDir + '/Insights-Source_df-{}.csv'.format(datetime.now().strftime('%Y%m%d_%H%M%S.%f')[:-7])
        insights_df.to_csv(out_file, index=False)

    # Generate output file handles
    print("Stage 6: Generating report output files")
    # Full output
    out_file = dataDir + '/Impact_Report-{}.csv'.format(datetime.now().strftime('%Y%m%d_%H%M%S.%f')[:-7])

    # Top 10 output
    top10_out_file = dataDir + '/Impact_Report-Top-10-{}.csv'.format(datetime.now().strftime('%Y%m%d_%H%M%S.%f')[:-7])

    # Write df to csv
    print("Stage 7: Writing report files to disk")
    try:
        # Full output
        insights_df.to_csv(out_file, index=False)
        print("{}Report of {} records output to:\n\t\t {}".format(spacer, insights_df.shape[0], out_file))
    except:
        print("{}Failure when writing complete data to CSV file: {}".format(spacer, out_file))

    try:
        insights_df.head(10).to_csv(top10_out_file, index=False)
        print("{}Report of Top 10 records output to:\n\t\t {}".format(spacer, top10_out_file))
    except:
        print("{}Failure when writing Top 10 Report to CSV file: {}".format(spacer, top10_out_file))

    print("Stage 8: Report generation completed!")

    # And that's it!
##################################################################


if __name__ == "__main__":
    main()


print("\nFinish :", str(datetime.now().time())[:-7])
print("Total execution time: {}".format(datetime.now() - run_start_time))
