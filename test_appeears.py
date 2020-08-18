import sys
import json
from urllib.parse import unquote_plus

import urllib3
import certifi
import requests
from time import sleep
import boto3 as boto3

from mosquito_util import load_json_from_s3, update_status_on_s3

data_bucket = "mosquito-data"

auth = ('mosquito2019', 'Malafr#1')

s3 = boto3.resource(
    's3')

test_count = 0
# Create a urllib PoolManager instance to make requests.
http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())
# http = urllib3.PoolManager()
# Set the URL for the GES DISC subset service endpoint
url = 'https://disc.gsfc.nasa.gov/service/subset/jsonwsp'


# This method POSTs formatted JSON WSP requests to the GES DISC endpoint URL
# It is created for convenience since this task will be repeated more than once
def get_http_data(request):
    hdrs = {'Content-Type': 'application/json',
            'Accept': 'application/json'}
    data = json.dumps(request)
    r = http.request('POST', url, body=data, headers=hdrs)
    response = json.loads(r.data)
    print('request ', request)
    print('response ', response)
    # Check for errors
    if response['type'] == 'jsonwsp/fault':
        print('API Error: faulty %s request' % response['methodname'])
        sys.exit(1)
    return response


def update_status_test(bucket, request_id, type, status, message):
    global test_count
    statusJson = {"request_id": request_id, "type": type, "status": status, "message": message}
    with open("/tmp/" + request_id + "_" + type + ".json", 'w') as status_file:
        json.dump(statusJson, status_file)
    #        json.dump(districtPrecipStats, json_file)
    status_file.close()

    #    bucket.upload_file("/tmp/" + request_id + "_" + type +".json",
    #                                       "status/" + request_id + "_" + type +".json")
    #    bucket.upload_file("/tmp/" + request_id + "_" + type +".json",
    #                                       "status/" + request_id + "_" + type + str(test_count) +".json")
    bucket.upload_file("/tmp/" + request_id + "_" + type + ".json",
                       "status/" + request_id + ".json")
    bucket.upload_file("/tmp/" + request_id + "_" + type + ".json",
                       "status/" + request_id + str(test_count) + ".json")
    test_count = test_count + 1


def download_imerg(subset_request, request_id, creation_time_in, dataset_name):
    # Define the parameters for the data subset
    download_results = []
    # Submit the subset request to the GES DISC Server
    response = get_http_data(subset_request)
    # Report the JobID and initial status
    myJobId = response['result']['jobId']
    print('Job ID: ' + myJobId)
    print('Job status: ' + response['result']['Status'])
    update_status_on_s3(s3.Bucket(data_bucket), request_id,
                        "download", "working", "initiated GES DISC order...",
                        creation_time=creation_time_in, dataset=dataset_name)

    # Construct JSON WSP request for API method: GetStatus
    status_request = {
        'methodname': 'GetStatus',
        'version': '1.0',
        'type': 'jsonwsp/request',
        'args': {'jobId': myJobId}
    }
    status_change_count = 0
    previous_status = ''
    # Check on the job status after a brief nap
    while response['result']['Status'] in ['Accepted', 'Running']:
        sleep(2)
        response = get_http_data(status_request)
        status = response['result']['Status']
        if status == previous_status:
            status_change_count = status_change_count + 1
            previous_status = status
        else:
            status_change_count = 0
            previous_status = status
        if status_change_count > 30:
            print('Job status has not changed in 60 seconds, probably hung on GES DISC server')
            update_status_on_s3(s3.Bucket(data_bucket), request_id, "download", "failed",
                                "Connection problem with GES DISC, order failed ",
                                creation_time=creation_time_in, dataset=dataset_name)
            sys.exit(1)
        percent = response['result']['PercentCompleted']
        print('Job status: %s (%d%c complete)' % (status, percent, '%'))
    if response['result']['Status'] == 'Succeeded':
        update_status_on_s3(s3.Bucket(data_bucket), request_id, "download", "working", "GES DISC Job Success.",
                            creation_time=creation_time_in, dataset=dataset_name)
        print('Job Finished:  %s' % response['result']['message'])
    else:
        #    print('Job Failed: %s' % response['fault']['code'])
        print('Job Failed: %s' % response['result']['message'])
        update_status_on_s3(s3.Bucket(data_bucket), request_id, "download", "failed",
                            #                           "GES DISC order failed: " + response['result']['message'],
                            "GES DISC order failed: Server error",
                            creation_time=creation_time_in, dataset=dataset_name)
        sys.exit(1)

    # Retrieve a plain-text list of results in a single shot using the saved JobID
    try:
        result = requests.get('https://disc.gsfc.nasa.gov/api/jobs/results/' + myJobId)
        result.raise_for_status()
        print(result.text)
        #    urls = result.text.split('\n')
        urls = result.text.splitlines()
    #        for i in urls: print('%s' % i)
    except:
        print('Request returned error code %d' % result.status_code)
        update_status_on_s3(s3.Bucket(data_bucket), request_id, "download", "failed",
                            #                           "GES DISC retrieve results list failed: " + result.status_code,
                            "GES DISC retrieve results list failed: ",
                            creation_time=creation_time_in, dataset=dataset_name)
        sys.exit(1)
    # count the valild files
    filelist = []
    for item in urls:
        outfn = item.split('/')
        if len(outfn) <= 0:
            print('skipping unknown file ' + outfn)
            continue
        outfn = outfn[len(outfn) - 1].split('?')[0]
        # skip pdf documentation files staged automatically by request
        if not outfn.endswith('.pdf'):
            entry = {"outfn": outfn, "url": item}
            filelist.append(entry)
        else:
            print('skipping documentation file ' + outfn)

    numfiles = len(filelist)

    # Use the requests library to submit the HTTP_Services URLs and write out the results.
    count = 0
    for entry in filelist:
        URL = entry["url"]
        outfn = entry["outfn"]
        download_results.append("imerg/" + outfn)
        print('outfile %s ' % outfn)
        print("item " + item)
        s = requests.Session()
        s.auth = auth

        try:
            r1 = s.request('get', URL)
            result = s.get(r1.url)
            result.raise_for_status()
            tmpfn = '/tmp/' + outfn
            f = open(tmpfn, 'wb')
            f.write(result.content)
            f.close()
            print(outfn)

            s3.Bucket(data_bucket).upload_file(tmpfn, "imerg/" + outfn)
            count = count + 1
            update_status_on_s3(s3.Bucket(data_bucket), request_id, "download", "working", "GES DISC downloaded file "
                                + str(count)
                                + " of " + str(numfiles), creation_time=creation_time_in, dataset=dataset_name)
        except:
            update_status_on_s3(s3.Bucket(data_bucket), request_id, "download", "failed",
                                "GES DISC retrieve results failed on file " + str(count)
                                #                               + " of " + str(numfiles) + ": " + str(result.status_code),
                                + " of " + str(numfiles) + ": " + str(result.status_code),
                                creation_time=creation_time_in, dataset=dataset_name)
            print('Error! Status code is %d for this URL:\n%s' % (result.status.code, URL))
            print('Help for downloading data is at https://disc.gsfc.nasa.gov/data-access')
            sys.exit(1)

    return download_results


def load_json(bucket, key):
    print("event key " + key)
    # strip off directory from key for temp file
    key_split = key.split('/')
    download_fn = key_split[len(key_split) - 1]
    file = "/tmp/" + download_fn
    s3.Bucket(bucket).download_file(key, file)

    try:
        with open(file) as f:
            jsonData = json.load(f)
        f.close()
    except IOError:
        print("Could not read file:" + file)
        jsonData = {"message": "error"}

    return jsonData


def lambda_handler(event, context):
    #    product = 'GPM_3IMERGDE_06'
    # use "Late" product
    # product = 'GPM_3IMERGDL_06'
    # varName = 'HQprecipitation'

    global test_count
    test_count = 0
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])

        #        input_json = load_json(bucket, key)
        input_json = load_json_from_s3(s3.Bucket(bucket), key)
        if "message" in input_json and input_json["message"] == "error":
            update_status_on_s3(s3.Bucket(data_bucket), request_id, "download", "failed",
                                "load_json_from_s3 could not load " + key)
            sys.exit(1)

        dataset = input_json["dataset"]
        org_unit = input_json["org_unit"]
        agg_period = input_json["agg_period"]
        request_id = input_json["request_id"]
        print("request_id ", request_id)

        start_date = input_json['start_date']
        end_date = input_json['end_date']
        # begTime = '2015-08-01T00:00:00.000Z'
        # endTime = '2015-08-01T23:59:59.999Z'

        minlon = input_json['min_lon']
        maxlon = input_json['max_lon']
        minlat = input_json['min_lat']
        maxlat = input_json['max_lat']
        creation_time_in = input_json['creation_time']

        statType = 'mean'
        product = 'GPM_3IMERGDF_06'
        varName = 'precipitationCal'
        if "stat_type" in input_json:
            statType = input_json['stat_type']
        print('stat_type' + statType)
        if "product" in input_json:
            product = input_json['product']
        print('product' + product)
        if "var_name" in input_json:
            varName = input_json['var_name']
        print('var_name' + varName)

        data_element_id = input_json['data_element_id']

        #    varName = event['variable']
        # Construct JSON WSP request for API method: subset
        #        subset_request = {
        #            'methodname': 'subset',
        #            'type': 'jsonwsp/request',
        #            'version': '1.0',
        #            'args': {
        #                'role': 'subset',
        #                'start': start_date,
        #                'end': end_date,
        #                'box': [minlon, minlat, maxlon, maxlat],
        #                'extent': [minlon, minlat, maxlon, maxlat],
        #                'data': [{'datasetId': product,
        #                          'variable': varName
        #                          }]
        #            }
        #        }

        subset_request = {
            'methodname': 'subset',
            'type': 'jsonwsp/request',
            'version': '1.0',
            'args': {
                'role': 'subset',
                'start': start_date,
                'end': end_date,
                'box': [minlon, minlat, maxlon, maxlat],
                'crop': True,
                'data': [{'datasetId': product,
                          'variable': varName
                          }]
            }
        }

        download_results = download_imerg(subset_request, request_id, creation_time_in, dataset)

        # need error check on download_imerg

        # write out file list as json file into monitored s3 bucket to trigger aggregation
        # format new json structure
        aggregateJson = {"request_id": request_id, "data_element_id": data_element_id, "variable": varName,
                         "dataset": dataset, "org_unit": org_unit, "agg_period": agg_period,
                         "s3bucket": data_bucket, "files": download_results, "stat_type": statType,
                         "creation_time": creation_time_in}

        aggregate_pathname = "requests/aggregate/precipitation/"

        with open("/tmp/" + request_id + "_aggregate.json", 'w') as aggregate_file:
            json.dump(aggregateJson, aggregate_file)
        #        json.dump(districtPrecipStats, json_file)
        aggregate_file.close()

        s3.Bucket(data_bucket).upload_file("/tmp/" + request_id + "_aggregate.json",
                                           aggregate_pathname + request_id + "_aggregate.json")

    update_status_on_s3(s3.Bucket(data_bucket), request_id, "download", "complete",
                        "All requested files successfully downloaded ", creation_time=creation_time_in, dataset=dataset)

def main():
    #curl --request POST --user your-username:your-password --header "Content-Length: 0" "https://lpdaacsvc.cr.usgs.gov/appeears/api/login"
    # {
    #     "token_type": "Bearer",
    #     "token": "31ncqphv-1jpPjcTe-hgWXM2xZ1bBqQxST5pcieiHKq0cMwz8IFKOxG3FZgLQonk8hBsLV_ruAqikYXfzWy7kw",
    #     "expiration": "2017-10-12T19:32:05Z"
    # }
    pass

if __name__ == '__main__':
   main()
