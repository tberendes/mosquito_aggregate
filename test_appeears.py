import cgi
import os
import sys
import json
from csv import DictReader
from urllib.parse import unquote_plus

import urllib3
import certifi
import requests
from time import sleep
import boto3 as boto3

from mosquito_util import load_json_from_s3, update_status_on_s3

data_bucket = "mosquito-data"
appeears_url = "https://lpdaacsvc.cr.usgs.gov/appeears/api/"

auth = ('mosquito2019', 'Malafr#1')

s3 = boto3.resource(
    's3')
# make token global
token = ''

test_count = 0
# Create a urllib PoolManager instance to make requests.
http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())
# http = urllib3.PoolManager()


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


# def lambda_handler(event, context):
#     #    product = 'GPM_3IMERGDE_06'
#     # use "Late" product
#     # product = 'GPM_3IMERGDL_06'
#     # varName = 'HQprecipitation'
#
#     global test_count
#     test_count = 0
#     for record in event['Records']:
#         bucket = record['s3']['bucket']['name']
#         key = unquote_plus(record['s3']['object']['key'])
#
#         #        input_json = load_json(bucket, key)
#         input_json = load_json_from_s3(s3.Bucket(bucket), key)
#         if "message" in input_json and input_json["message"] == "error":
#             update_status_on_s3(s3.Bucket(data_bucket), request_id, "download", "failed",
#                                 "load_json_from_s3 could not load " + key)
#             sys.exit(1)
#
#         dataset = input_json["dataset"]
#         org_unit = input_json["org_unit"]
#         agg_period = input_json["agg_period"]
#         request_id = input_json["request_id"]
#         print("request_id ", request_id)
#
#         start_date = input_json['start_date']
#         end_date = input_json['end_date']
#         # begTime = '2015-08-01T00:00:00.000Z'
#         # endTime = '2015-08-01T23:59:59.999Z'
#
#         minlon = input_json['min_lon']
#         maxlon = input_json['max_lon']
#         minlat = input_json['min_lat']
#         maxlat = input_json['max_lat']
#         creation_time_in = input_json['creation_time']
#
#         statType = 'mean'
#         product = 'GPM_3IMERGDF_06'
#         varName = 'precipitationCal'
#         if "stat_type" in input_json:
#             statType = input_json['stat_type']
#         print('stat_type' + statType)
#         if "product" in input_json:
#             product = input_json['product']
#         print('product' + product)
#         if "var_name" in input_json:
#             varName = input_json['var_name']
#         print('var_name' + varName)
#
#         data_element_id = input_json['data_element_id']
#
#         #    varName = event['variable']
#         # Construct JSON WSP request for API method: subset
#         #        subset_request = {
#         #            'methodname': 'subset',
#         #            'type': 'jsonwsp/request',
#         #            'version': '1.0',
#         #            'args': {
#         #                'role': 'subset',
#         #                'start': start_date,
#         #                'end': end_date,
#         #                'box': [minlon, minlat, maxlon, maxlat],
#         #                'extent': [minlon, minlat, maxlon, maxlat],
#         #                'data': [{'datasetId': product,
#         #                          'variable': varName
#         #                          }]
#         #            }
#         #        }
#
#         subset_request = {
#             'methodname': 'subset',
#             'type': 'jsonwsp/request',
#             'version': '1.0',
#             'args': {
#                 'role': 'subset',
#                 'start': start_date,
#                 'end': end_date,
#                 'box': [minlon, minlat, maxlon, maxlat],
#                 'crop': True,
#                 'data': [{'datasetId': product,
#                           'variable': varName
#                           }]
#             }
#         }
#
#         download_results = download_imerg(subset_request, request_id, creation_time_in, dataset)
#
#         # need error check on download_imerg
#
#         # write out file list as json file into monitored s3 bucket to trigger aggregation
#         # format new json structure
#         aggregateJson = {"request_id": request_id, "data_element_id": data_element_id, "variable": varName,
#                          "dataset": dataset, "org_unit": org_unit, "agg_period": agg_period,
#                          "s3bucket": data_bucket, "files": download_results, "stat_type": statType,
#                          "creation_time": creation_time_in}
#
#         aggregate_pathname = "requests/aggregate/precipitation/"
#
#         with open("/tmp/" + request_id + "_aggregate.json", 'w') as aggregate_file:
#             json.dump(aggregateJson, aggregate_file)
#         #        json.dump(districtPrecipStats, json_file)
#         aggregate_file.close()
#
#         s3.Bucket(data_bucket).upload_file("/tmp/" + request_id + "_aggregate.json",
#                                            aggregate_pathname + request_id + "_aggregate.json")
#
#     update_status_on_s3(s3.Bucket(data_bucket), request_id, "download", "complete",
#                         "All requested files successfully downloaded ", creation_time=creation_time_in, dataset=dataset)


# This method POSTs formatted JSON WSP requests to the GES DISC endpoint URL
# It is created for convenience since this task will be repeated more than once
def post_http_data(hdrs, url, request):
    data = json.dumps(request)
    r = requests.post(url, data=request, auth=auth, headers=hdrs)
    #response = json.loads(r.data)
    #r = http.request('POST', url, body=data, headers=hdrs)
    response = json.loads(r.text)
    print('request ', request)
    print('response ', response)
    # Check for errors
    # if response['type'] == 'jsonwsp/fault':
    #     print('API Error: faulty %s request' % response['methodname'])
    #     sys.exit(1)
    return response

def login():
    hdrs = {'Content-Length': '0'}
    r = requests.post(appeears_url+'login', data={}, auth=auth, headers=hdrs)
    response = json.loads(r.text)
    print('login response ', response)
    if 'token' not in response:
        if 'message' in response:
            except_msg = response['message']
        else:
            except_msg = "Login errror: unknown failure"
        print("Login failed:")
        raise Exception(except_msg)
    print("Successfully logged in")
    return response['token']

def logout(token):
    hdrs = {'Content-Length': '0', 'Authorization': 'Bearer '+token}
    r = requests.post(appeears_url+'logout', data={}, headers=hdrs)
    print('logout status ', r.status_code)
    if r.status_code != 204:
        print("Logout failed:")
        raise Exception("logout error: status code "+str(r.status_code))
    else:
        print("Successfully logged out")
    # Check for errors
    # if response['type'] == 'jsonwsp/fault':
    #     print('API Error: faulty %s request' % response['methodname'])
    #     sys.exit(1)
    return r.status_code

def submit_task(task_name,product,layer,start_date,end_date):
    task_json = {}
    task_json['task_name'] = task_name
    task_json['task_type'] = 'area'
    params = {}
    params['dates'] = []
    params['dates'].append({'startDate': start_date})
    params['dates'].append({'endDate': end_date})
    params['layers'] = []
    params['layers'].append({'product':product})
    params['layers'].append({'layer':layer})
    params['output'] = {'projection':'geographic','format':{'type':'geotiff'}} #or netcdf4
    # set up geojson dictionary
    params['geo'] = {"geometry":{"type":"Polygon","coordinates":[[[-11.8091,9.2032],[-11.8102,9.1944],[-11.8128,9.1793],[-11.8144,9.1747],[-11.816,9.1681],[-11.8177,9.1623],[-11.8212,9.1524],[-11.8242,9.1459],[-11.8258,9.1401],[-11.8263,9.1335],[-11.8258,9.1283],[-11.8238,9.1213],[-11.8207,9.113],[-11.8219,9.1044],[-11.8217,9.0986],[-11.8201,9.0915],[-11.8185,9.087],[-11.8164,9.084],[-11.8123,9.0821],[-11.8014,9.081],[-11.7953,9.0793],[-11.7908,9.0786],[-11.7831,9.0784],[-11.7661,9.0787],[-11.7609,9.0781],[-11.7569,9.0759],[-11.7544,9.0715],[-11.754,9.0663],[-11.7549,9.0626],[-11.7565,9.0597],[-11.7585,9.0577],[-11.7613,9.0564],[-11.7696,9.0539],[-11.7729,9.0513],[-11.7751,9.0479],[-11.7771,9.0388],[-11.7782,9.0359],[-11.7799,9.034],[-11.785,9.0306],[-11.7875,9.0283],[-11.7896,9.0243],[-11.7912,9.0188],[-11.7966,9.014],[-11.8017,9.0119],[-11.807,9.0119],[-11.8105,9.0132],[-11.8167,9.017],[-11.8201,9.0181],[-11.8235,9.0183],[-11.8291,9.0167],[-11.832,9.0146],[-11.834,9.0116],[-11.8353,9.0072],[-11.8365,8.9968],[-11.8388,8.9919],[-11.8426,8.9897],[-11.847,8.9899],[-11.8513,8.9915],[-11.8551,8.9923],[-11.8591,8.9922],[-11.8623,8.9912],[-11.8652,8.9894],[-11.869,8.9854],[-11.8739,8.9783],[-11.8776,8.9751],[-11.8827,8.9718],[-11.8867,8.9682],[-11.8895,8.964],[-11.8895,8.9595],[-11.8877,8.9553],[-11.8832,8.9495],[-11.8776,8.9401],[-11.8766,8.9336],[-11.8771,8.9296],[-11.8785,8.9251],[-11.883,8.9149],[-11.8859,8.9092],[-11.8811,8.9029],[-11.8784,8.8964],[-11.8738,8.8891],[-11.8726,8.8854],[-11.8723,8.8815],[-11.8731,8.8764],[-11.8759,8.8716],[-11.8824,8.8664],[-11.8854,8.8635],[-11.8878,8.8601],[-11.8892,8.8569],[-11.8902,8.8532],[-11.8906,8.8502],[-11.8915,8.828],[-11.8936,8.8237],[-11.894,8.8199],[-11.8937,8.8146],[-11.8927,8.8107],[-11.8892,8.8043],[-11.8879,8.8012],[-11.8872,8.7967],[-11.8878,8.7932],[-11.889,8.79],[-11.8937,8.7829],[-11.8952,8.7797],[-11.8963,8.7763],[-11.8977,8.7685],[-11.8988,8.7649],[-11.9042,8.7554],[-11.9069,8.7521],[-11.91,8.7493],[-11.9163,8.7451],[-11.9262,8.7377],[-11.9337,8.7344],[-11.9467,8.7254],[-11.9493,8.7231],[-11.9537,8.7182],[-11.9584,8.7152],[-11.9681,8.7105],[-11.9748,8.7086],[-11.9802,8.7066],[-11.9864,8.7055],[-11.9887,8.701],[-11.9937,8.6936],[-11.9977,8.6849],[-11.9998,8.6816],[-12.0017,8.6795],[-12.0046,8.6776],[-12.008,8.6767],[-12.0121,8.677],[-12.0278,8.6825],[-12.0307,8.6832],[-12.0337,8.6835],[-12.0383,8.6835],[-12.0428,8.6831],[-12.0457,8.6824],[-12.0503,8.6808],[-12.0547,8.6799],[-12.0607,8.6797],[-12.0651,8.6802],[-12.0679,8.6812],[-12.0712,8.6845],[-12.0752,8.6919],[-12.0786,8.6941],[-12.0825,8.6939],[-12.0853,8.6918],[-12.0864,8.6882],[-12.0851,8.681],[-12.0852,8.6766],[-12.0877,8.672],[-12.0905,8.6691],[-12.0951,8.6662],[-12.1052,8.6635],[-12.1088,8.6615],[-12.112,8.6588],[-12.1151,8.6558],[-12.121,8.649],[-12.1272,8.6418],[-12.1297,8.6401],[-12.1331,8.6391],[-12.136,8.6387],[-12.1505,8.638],[-12.1546,8.6429],[-12.1571,8.6467],[-12.1609,8.6553],[-12.1648,8.6627],[-12.17,8.6698],[-12.1764,8.6802],[-12.1801,8.6888],[-12.1827,8.6942],[-12.1895,8.7122],[-12.1901,8.7167],[-12.2029,8.7171],[-12.2104,8.7167],[-12.2147,8.7157],[-12.2277,8.7104],[-12.2322,8.7108],[-12.2347,8.7126],[-12.2362,8.7155],[-12.2374,8.7204],[-12.2379,8.73],[-12.2391,8.7329],[-12.2422,8.7348],[-12.2457,8.7343],[-12.249,8.7319],[-12.2526,8.7245],[-12.2563,8.7173],[-12.2593,8.7147],[-12.2627,8.7144],[-12.2648,8.7155],[-12.2666,8.7188],[-12.2668,8.7232],[-12.2647,8.7274],[-12.2602,8.7319],[-12.2566,8.7394],[-12.2598,8.7418],[-12.2642,8.7415],[-12.2678,8.7394],[-12.2721,8.7401],[-12.2755,8.7419],[-12.2775,8.7452],[-12.2798,8.7525],[-12.282,8.7559],[-12.2846,8.7574],[-12.288,8.7579],[-12.2936,8.7565],[-12.2967,8.7547],[-12.299,8.752],[-12.303,8.7447],[-12.3098,8.7298],[-12.3109,8.7256],[-12.311,8.7227],[-12.3094,8.7159],[-12.3088,8.7123],[-12.3093,8.7092],[-12.3113,8.7068],[-12.316,8.7059],[-12.3197,8.7067],[-12.3231,8.7089],[-12.3279,8.7131],[-12.3329,8.7184],[-12.3354,8.7216],[-12.3368,8.7249],[-12.3373,8.7293],[-12.336,8.7336],[-12.3307,8.7416],[-12.3263,8.75],[-12.3247,8.756],[-12.3243,8.7616],[-12.3244,8.773],[-12.3239,8.7771],[-12.3219,8.7816],[-12.3179,8.7863],[-12.3172,8.7873],[-12.3126,8.796],[-12.31,8.8028],[-12.3076,8.8106],[-12.3069,8.8151],[-12.3076,8.8197],[-12.3095,8.824],[-12.314,8.83],[-12.3172,8.835],[-12.3225,8.842],[-12.3264,8.8494],[-12.328,8.8538],[-12.3286,8.8573],[-12.3285,8.8608],[-12.3259,8.8722],[-12.3249,8.8827],[-12.3323,8.8794],[-12.3382,8.8758],[-12.3414,8.8745],[-12.3448,8.8741],[-12.3483,8.8749],[-12.3515,8.8768],[-12.3548,8.8798],[-12.3724,8.8975],[-12.3814,8.9061],[-12.3849,8.9089],[-12.3915,8.9133],[-12.4004,8.9211],[-12.4038,8.9239],[-12.4124,8.9287],[-12.4202,8.9319],[-12.4244,8.9327],[-12.433,8.9333],[-12.4332,8.9333],[-12.4398,8.9329],[-12.4451,8.9331],[-12.4493,8.9346],[-12.4514,8.9366],[-12.4551,8.9424],[-12.4586,8.95],[-12.4605,8.9556],[-12.4618,8.961],[-12.4629,8.9647],[-12.4642,8.9679],[-12.4676,8.9745],[-12.4695,8.981],[-12.4713,8.9854],[-12.4741,8.9894],[-12.4769,8.9918],[-12.4853,8.9962],[-12.4908,8.9986],[-12.4959,8.9996],[-12.5012,8.9992],[-12.5084,8.997],[-12.5126,8.9965],[-12.5183,8.9967],[-12.5223,8.9976],[-12.5334,9.0027],[-12.5411,9.0051],[-12.5487,9.0091],[-12.552,9.0115],[-12.5571,9.0163],[-12.5658,9.0262],[-12.5547,9.038],[-12.5502,9.0418],[-12.5452,9.0453],[-12.5424,9.0485],[-12.5413,9.0528],[-12.5416,9.0567],[-12.543,9.0603],[-12.5466,9.0664],[-12.5466,9.0702],[-12.5441,9.0738],[-12.5339,9.0804],[-12.53,9.0799],[-12.5279,9.0769],[-12.5285,9.0735],[-12.5299,9.0707],[-12.5272,9.0682],[-12.5224,9.0671],[-12.5187,9.0677],[-12.5162,9.0694],[-12.5132,9.0752],[-12.5108,9.0781],[-12.5076,9.0799],[-12.5039,9.0796],[-12.5021,9.0779],[-12.4983,9.0727],[-12.4952,9.0712],[-12.4902,9.0711],[-12.4812,9.0742],[-12.4788,9.0769],[-12.4787,9.0818],[-12.482,9.088],[-12.4846,9.0932],[-12.4872,9.0998],[-12.4887,9.1057],[-12.4882,9.1105],[-12.4867,9.1132],[-12.4839,9.1154],[-12.4763,9.1183],[-12.4709,9.1184],[-12.4631,9.1265],[-12.457,9.1325],[-12.4527,9.1361],[-12.4485,9.1386],[-12.4453,9.141],[-12.4412,9.1447],[-12.4375,9.1488],[-12.4354,9.1521],[-12.4318,9.1596],[-12.4301,9.1653],[-12.4284,9.173],[-12.4328,9.1825],[-12.4337,9.1862],[-12.4332,9.19],[-12.428,9.2076],[-12.4276,9.2121],[-12.4291,9.2176],[-12.4327,9.2251],[-12.4376,9.2333],[-12.4402,9.236],[-12.4432,9.238],[-12.4538,9.2437],[-12.4637,9.2515],[-12.4689,9.2547],[-12.472,9.2569],[-12.4795,9.2588],[-12.4776,9.2641],[-12.4737,9.2668],[-12.4686,9.2666],[-12.4646,9.2643],[-12.4563,9.2639],[-12.4516,9.263],[-12.4488,9.2619],[-12.4429,9.258],[-12.4395,9.2572],[-12.4356,9.2591],[-12.4341,9.2618],[-12.4297,9.2739],[-12.4284,9.2784],[-12.4278,9.2823],[-12.4273,9.292],[-12.4266,9.2959],[-12.4247,9.2993],[-12.4187,9.305],[-12.4111,9.2966],[-12.4006,9.2859],[-12.3953,9.2806],[-12.3919,9.2778],[-12.388,9.2758],[-12.3838,9.275],[-12.3799,9.2752],[-12.3768,9.2765],[-12.3753,9.2787],[-12.375,9.2818],[-12.3765,9.2862],[-12.3784,9.2889],[-12.3853,9.2941],[-12.3932,9.3009],[-12.3967,9.3037],[-12.4064,9.3093],[-12.4131,9.3127],[-12.4104,9.3174],[-12.4083,9.3221],[-12.4063,9.3251],[-12.4009,9.3305],[-12.3975,9.3363],[-12.3934,9.346],[-12.391,9.3493],[-12.3889,9.3512],[-12.3857,9.3527],[-12.3809,9.3531],[-12.3726,9.3508],[-12.3662,9.35],[-12.365,9.3552],[-12.3637,9.3616],[-12.3668,9.3654],[-12.3677,9.3693],[-12.3676,9.3745],[-12.3654,9.3838],[-12.3651,9.3879],[-12.3657,9.3918],[-12.3676,9.3958],[-12.3702,9.3983],[-12.3754,9.4009],[-12.3808,9.4019],[-12.3909,9.402],[-12.3951,9.4024],[-12.3982,9.4037],[-12.4008,9.4062],[-12.4024,9.4108],[-12.4028,9.4241],[-12.404,9.4313],[-12.4078,9.439],[-12.4096,9.4449],[-12.4104,9.4519],[-12.4105,9.4636],[-12.4119,9.467],[-12.4134,9.4695],[-12.4162,9.4712],[-12.4204,9.471],[-12.4288,9.4684],[-12.4334,9.4666],[-12.4378,9.4658],[-12.4423,9.4654],[-12.4501,9.4653],[-12.4693,9.4658],[-12.4736,9.4806],[-12.4752,9.4875],[-12.477,9.4931],[-12.4837,9.5121],[-12.4854,9.5192],[-12.4854,9.5234],[-12.483,9.5342],[-12.4827,9.5399],[-12.4832,9.5456],[-12.4858,9.5564],[-12.4861,9.5617],[-12.4856,9.5657],[-12.4832,9.575],[-12.4829,9.5816],[-12.4841,9.5863],[-12.4857,9.5891],[-12.4881,9.5906],[-12.4908,9.5909],[-12.5,9.5879],[-12.5071,9.5854],[-12.5122,9.585],[-12.5167,9.5866],[-12.5216,9.5905],[-12.5313,9.5932],[-12.5373,9.5952],[-12.5416,9.5956],[-12.5459,9.5954],[-12.5501,9.5947],[-12.556,9.5928],[-12.5618,9.5921],[-12.5662,9.5922],[-12.57,9.5928],[-12.5767,9.5949],[-12.5835,9.5964],[-12.5917,9.5999],[-12.5906,9.6125],[-12.5854,9.6289],[-12.5856,9.6353],[-12.588,9.6422],[-12.5915,9.6541],[-12.5897,9.6606],[-12.5871,9.6631],[-12.5832,9.6636],[-12.5785,9.6581],[-12.5755,9.6599],[-12.5742,9.6701],[-12.5696,9.6774],[-12.568,9.6839],[-12.5689,9.6924],[-12.5667,9.7027],[-12.5567,9.7035],[-12.5475,9.709],[-12.5361,9.7205],[-12.5336,9.7201],[-12.5289,9.7091],[-12.5257,9.7059],[-12.5224,9.7054],[-12.5194,9.7073],[-12.5192,9.7142],[-12.5281,9.7364],[-12.5256,9.7417],[-12.523,9.7434],[-12.5091,9.7429],[-12.5073,9.7459],[-12.5084,9.7494],[-12.5184,9.7539],[-12.5187,9.7565],[-12.5124,9.7634],[-12.5113,9.7751],[-12.5014,9.7892],[-12.5003,9.808],[-12.4973,9.8103],[-12.4913,9.8113],[-12.4902,9.826],[-12.4923,9.829],[-12.4967,9.8299],[-12.4995,9.8322],[-12.5006,9.8347],[-12.4989,9.8393],[-12.499,9.8436],[-12.4801,9.8555],[-12.4569,9.8546],[-12.4525,9.8572],[-12.4435,9.8662],[-12.4375,9.8775],[-12.4317,9.8816],[-12.3937,9.8878],[-12.3713,9.8927],[-12.3659,9.8957],[-12.3532,9.8962],[-12.2892,9.9088],[-12.2874,9.9109],[-12.266,9.9158],[-12.2271,9.9247],[-12.2227,9.9244],[-12.2153,9.9187],[-12.2129,9.9141],[-12.205,9.907],[-12.1774,9.8974],[-12.1569,9.8821],[-12.1418,9.8743],[-12.1265,9.8714],[-12.12,9.8719],[-12.0825,9.8835],[-12.0795,9.8855],[-12.0614,9.8892],[-12.0475,9.8953],[-12.0292,9.8988],[-11.9723,9.9161],[-11.9379,9.9265],[-11.9256,9.9289],[-11.9186,9.9326],[-11.9066,9.9358],[-11.9017,9.9417],[-11.9029,9.9369],[-11.9026,9.9284],[-11.9033,9.9186],[-11.9021,9.9079],[-11.9024,9.9025],[-11.9035,9.8995],[-11.9098,9.8937],[-11.912,9.8902],[-11.9122,9.8841],[-11.9106,9.8804],[-11.9031,9.8714],[-11.9006,9.8692],[-11.8956,9.8668],[-11.8934,9.8649],[-11.8913,9.861],[-11.8903,9.8571],[-11.8901,9.8529],[-11.8904,9.8485],[-11.8932,9.8364],[-11.8932,9.8297],[-11.892,9.8248],[-11.8907,9.8216],[-11.8873,9.8151],[-11.8865,9.8113],[-11.8865,9.806],[-11.8889,9.7954],[-11.8891,9.7901],[-11.8886,9.7861],[-11.8862,9.7766],[-11.8857,9.7722],[-11.8854,9.7647],[-11.8854,9.754],[-11.8865,9.7386],[-11.8854,9.7291],[-11.8847,9.711],[-11.8831,9.7001],[-11.89,9.7009],[-11.9082,9.7],[-11.9128,9.6994],[-11.9204,9.6974],[-11.9308,9.6963],[-11.9352,9.6955],[-11.9379,9.6945],[-11.9452,9.6903],[-11.9627,9.684],[-11.9713,9.6791],[-11.9776,9.6765],[-11.9873,9.6694],[-11.9912,9.6674],[-12.0009,9.6638],[-12.0075,9.6618],[-12.0197,9.6567],[-12.0297,9.6543],[-12.0374,9.6507],[-12.0433,9.647],[-12.0459,9.6457],[-12.0502,9.6447],[-12.0621,9.6435],[-12.0735,9.6401],[-12.0771,9.6387],[-12.0823,9.636],[-12.0859,9.6335],[-12.0913,9.6285],[-12.0986,9.621],[-12.1014,9.6176],[-12.1035,9.6139],[-12.1062,9.6039],[-12.1083,9.6002],[-12.1122,9.5958],[-12.1166,9.5919],[-12.1202,9.5897],[-12.1271,9.5863],[-12.1301,9.5827],[-12.1324,9.5773],[-12.1262,9.5747],[-12.1187,9.572],[-12.1014,9.5638],[-12.0973,9.5627],[-12.0928,9.5623],[-12.0867,9.5626],[-12.0823,9.5635],[-12.0764,9.5655],[-12.0721,9.5662],[-12.0664,9.566],[-12.0629,9.565],[-12.0599,9.5629],[-12.0577,9.5601],[-12.0567,9.556],[-12.0576,9.5516],[-12.0605,9.5458],[-12.0612,9.5428],[-12.0601,9.5393],[-12.057,9.537],[-12.0502,9.5351],[-12.0473,9.5333],[-12.044,9.5288],[-12.0403,9.5221],[-12.0362,9.5162],[-12.0262,9.5236],[-12.0186,9.5276],[-12.0135,9.5287],[-12.0095,9.5287],[-12.0057,9.5276],[-11.9998,9.5241],[-11.9971,9.5205],[-11.9942,9.5144],[-11.9919,9.511],[-11.988,9.507],[-11.9846,9.5045],[-11.9807,9.5031],[-11.9764,9.5027],[-11.9707,9.5027],[-11.9605,9.5044],[-11.954,9.5087],[-11.9489,9.5143],[-11.9459,9.5163],[-11.9432,9.5172],[-11.9335,9.5189],[-11.9249,9.5233],[-11.9222,9.5243],[-11.9193,9.5249],[-11.9149,9.5253],[-11.8981,9.5254],[-11.8937,9.5249],[-11.8908,9.5241],[-11.8872,9.5219],[-11.8824,9.5168],[-11.8776,9.5082],[-11.8735,9.4996],[-11.8721,9.4951],[-11.8715,9.4922],[-11.8711,9.4864],[-11.8711,9.476],[-11.8714,9.4701],[-11.8722,9.4594],[-11.8709,9.4544],[-11.8674,9.448],[-11.8535,9.4331],[-11.8474,9.4252],[-11.8446,9.423],[-11.8371,9.4188],[-11.8351,9.4161],[-11.8338,9.413],[-11.8317,9.4042],[-11.83,9.4005],[-11.8244,9.3926],[-11.8222,9.387],[-11.8212,9.3784],[-11.8212,9.3651],[-11.8217,9.3593],[-11.824,9.3498],[-11.8245,9.3457],[-11.8248,9.3386],[-11.8245,9.33],[-11.8237,9.3244],[-11.8215,9.3163],[-11.821,9.3135],[-11.8206,9.3077],[-11.8204,9.2899],[-11.8201,9.284],[-11.8194,9.2798],[-11.817,9.2701],[-11.8166,9.266],[-11.8163,9.2585],[-11.8161,9.2342],[-11.8157,9.2267],[-11.8152,9.2239],[-11.8116,9.2103],[-11.8091,9.2032]]]}}
    task_json['params'] =  params
    json_str = json.dumps(task_json)
    print('json ',json_str)
    # task_json = {"task_type": "{task_type}",\
    #     "task_name": "{task_name}",\
    #     "params":\
    #         {"dates": ["startDate": "{startDate}",\
    #             "endDate": "{endDate}",\
    #             "recurring": true,\
    #             "yearRange": [start,end]\
    #         ],\
    #         "layers": [\
    #             "product": "{product_id}",\
    #             "layer": "{layer_name}"\
    #         ]\
    #         }\
    #     }
def post(url, json_payload, hdrs, timeout):
    task_response=requests.post(url, json=json_payload, headers=hdrs, timeout=timeout)
    task_response.raise_for_status()
    return task_response

def get(url, hdrs, timeout):
    task_response = requests.get(url, headers=hdrs, timeout=timeout)
    task_response.raise_for_status()
    return task_response


def main():
    #curl --request POST --user your-username:your-password --header "Content-Length: 0" "https://lpdaacsvc.cr.usgs.gov/appeears/api/login"
    # {
    #     "token_type": "Bearer",
    #     "token": "31ncqphv-1jpPjcTe-hgWXM2xZ1bBqQxST5pcieiHKq0cMwz8IFKOxG3FZgLQonk8hBsLV_ruAqikYXfzWy7kw",
    #     "expiration": "2017-10-12T19:32:05Z"
    # }
    product = "MOD11A2.006"
    layer = "LST_Day_1km"

    #product = "VNP21A2.001" # 8 day composite
    #layer = "LST_Day_1km"

    # product = "VNP13A2.001" # 16 day composite
    # layer = "_1_km_16_days_NDVI"

    start_date = "03-01-2018"
    #end_date = "04-30-2018"
    end_date = "03-31-2018"

    # product = "ECO2LSTE.001"
    # layer = "SDS_LST"
    # start_date = "07-09-2018"
    # end_date = "12-31-2018"
    payload = "sample_payload_MOD11B2.json"
    outDir = "test"

    with open(payload) as f:
        jsonData = json.load(f)
    f.close()

    boundaries = jsonData['boundaries']

    tasks = {}
    org_unit_id = {}
    data_element_id = jsonData["data_element_id"]
    for boundary in boundaries:
        geoJson = {}
        geoJson['type'] = 'FeatureCollection'
        features = []
        feature_entry = {}
        feature_entry['type'] = 'Feature'
        feature_entry['properties'] = {'name': boundary['name'], 'id': boundary['id']}
        feature_entry['geometry'] = boundary['geometry']
        features.append(feature_entry)
        geoJson['features'] = features
        org_unit_id[boundary['name']]=boundary['id']
        # set up tasks:
        tasks[boundary['name']] = {'task_type': 'area',
                'task_name': boundary['name'],
                'params': {'dates': [{'startDate': start_date, 'endDate': end_date}],
                           'layers': [{'layer': layer, 'product': product}],
                           #                'output': {'format': {'type': 'netcdf4'}, 'projection': 'native'},
                           'output': {'format': {'type': 'geotiff'}, 'projection': 'native'},
                           'geo': geoJson}}

        #print(tasks[boundary['name']])



    #     org_unit = {}
#     for boundary in boundaries:
#         geoJson = {}
#         geoJson['type'] = 'FeatureCollection'
#         features = []
#         feature_entry = {}
#         feature_entry['type']='Feature'
#         feature_entry['properties'] = {'name':boundary['name'],'id':boundary['id']}
#         feature_entry['geometry'] = boundary['geometry']
#         features.append(feature_entry)
#         geoJson['features'] = features
#
#         org_unit[boundary['name']] = geoJson
#
#         print(geoJson)
#
#     # set up tasks:
#     task = {'task_type': 'area',
#      'task_name': 'Western Area',
#      'params': {'dates': [{'startDate': start_date, 'endDate': end_date}],
#                 'layers': [{'layer': layer, 'product': product}],
# #                'output': {'format': {'type': 'netcdf4'}, 'projection': 'native'},
#                 'output': {'format': {'type': 'geotiff'}, 'projection': 'native'},
#                 'geo': org_unit['Western Area']}}

    try:
        token = login()
    except Exception as e:
        print("Exception: ", e)


    try:
        #token = login()
        #task_id = submit_task('test_task',product, layer, start_date, end_date)

        # Post json to the API task service, return response as json
        hdrs = {'Content-Length': '0', 'Authorization': 'Bearer ' + token}
        # r = requests.post(appeears_url + 'logout', data={}, headers=hdrs)
        task_id=[]
        task_status = {}
        task_id_to_name = {}
        for key,task in tasks.items():
            print("task ",task)
#            task_response = requests.post(appeears_url +'task', json=task, headers=hdrs, timeout=10.0)
            task_response = post(appeears_url +'task', task, hdrs, 30.0)
            #print("task response", task_response.json())
            id=task_response.json()['task_id']
            task_id.append(id)
            task_status[id]=False
            task_id_to_name[id] = key

        # check for status of tasks
        # Use while statement to ping the API every 2 seconds until a response of 'done' is returned
        count = 0

        while True:
            #req = requests.get(appeears_url + 'task/' + task_id, headers=hdrs)
            found_all = True
            for id in task_id:
                # skip completed task
                if task_status[id]==True:
                    continue
                else:
                    found_all = False
                #req = requests.get(appeears_url + 'task/' + id, headers=hdrs, timeout=10.0)
                req = get(appeears_url + 'task/' + id, hdrs, 30.0)
                #print("request ",req)
                status=req.json()['status']
                #print("count ", count, " status: ",status)
                if status == 'done':
                    task_status[id]=True
                    print("task ", task_id_to_name[id], " done")
                    #break;
                else: # at least one job not done, stop checking status
                    break
            if found_all==True:
                break
            sleep(2)
            count = count+1
            print("count ", count)
            if count > 100:
                print("request timed out with status: ", status)
                break;
        #print(requests.get(appeears_url +'task/'+task_id, headers=hdrs).json()['status'])

        outputJson = []
        # when finished download bundles
        bundles={}
        for id in task_id:
            #bundles[id] = requests.get(appeears_url +'bundle/'+id, headers=hdrs).json()  # Call API and return bundle contents for the task_id as json
            bundles[id] = get(appeears_url +'bundle/'+id, hdrs, 30.0).json()  # Call API and return bundle contents for the task_id as json
        #bundles[id] = requests.get(appeears_url +'bundle/'+task_id, headers=hdrs).json()  # Call API and return bundle contents for the task_id as json

        # extract filenames from bundle, download and process
        csv_file = product.replace('.','-')+'-Statistics.csv'
        for task_id,bundle in bundles.items():
            files = {}
            for file in bundle['files']:
                files[file['file_id']] = file['file_name']  # Fill dictionary with file_id as keys and file_name as values

            # download files
            # set up output directory for each bundle based on task name
            # replace spaces with "-"
            destDir = outDir+'/'+str(task_id_to_name[task_id]).replace(' ','-')
            # Set up output directory on local machine
            if not os.path.exists(destDir):
                os.makedirs(destDir)
            found_stats = False
            for file in files:
                #print("downloading ", file)
                if files[file]!=csv_file:
                    continue
                else:
                    found_stats=True
                print("downloading ", files[file])
                # download only .csv statistics file, set flag if found

                download_response = requests.get(appeears_url +'bundle/'+task_id+'/'+ file,
                                                 stream=True)  # Get a stream to the bundle file
                download_response.raise_for_status()
                filename = os.path.basename(cgi.parse_header(download_response.headers['Content-Disposition'])[1][
                                                'filename'])  # Parse the name from Content-Disposition header
                filepath = os.path.join(destDir, filename)  # Create output file path
                with open(filepath, 'wb') as fp:  # Write file to dest dir
                    for data in download_response.iter_content(chunk_size=8192):
                        fp.write(data)

                # parse out csv file
                with open(filepath, 'r') as read_obj:
                    # pass the file object to DictReader() to get the DictReader object
                    csv_dict_reader = DictReader(read_obj)
                    # iterate over each line as a ordered dictionary
                    # for row in csv_dict_reader:
                    #     # row variable is a dictionary that represents a row in csv
                    #     print(row)
                    # column_names = csv_dict_reader.fieldnames
                    # print(column_names)

# File Name,Dataset,aid,Date,Count,Minimum,Maximum,Range,Mean,Standard Deviation,Variance,Upper Quartile,Upper 1.5 IQR,Median,Lower 1.5 IQR,Lower Quartile
# MOD11A2_006_LST_Day_1km_doy2018057_aid0001,LST_Day_1km,aid0001,2018-02-26,6803.0,297.38,311.6,"(297.38,311.6)",303.7146,1.7401,3.028,304.87,308.44,303.74,298.9,302.48
# MOD11A2_006_LST_Day_1km_doy2018065_aid0001,LST_Day_1km,aid0001,2018-03-06,6807.0,296.16,310.72,"(296.16,310.72)",301.8998,1.796,3.2256,303.04,306.8,302.02,296.86,300.52
# MOD11A2_006_LST_Day_1km_doy2018073_aid0001,LST_Day_1km,aid0001,2018-03-14,5213.0,295.9,310.22,"(295.9,310.22)",303.2248,1.9724,3.8905,304.78,308.98,303.08,297.78,301.98
# MOD11A2_006_LST_Day_1km_doy2018081_aid0001,LST_Day_1km,aid0001,2018-03-22,1685.0,292.6,304.8,"(292.6,304.8)",299.7435,1.7759,3.1537,301.06,304.22,299.68,295.14,298.66
# MOD11A2_006_LST_Day_1km_doy2018089_aid0001,LST_Day_1km,aid0001,2018-03-30,3423.0,296.82,306.86,"(296.82,306.86)",300.2546,1.2143,1.4745,300.88,302.98,300.16,297.4,299.48
                # append statistics to json
                    for row in csv_dict_reader:
                        value = row['Mean']
                        dateStr = row['Date'].replace('-','')
                        #dateStr = startTime.strftime("%Y%m%d")
                        jsonRecord = {'dataElement': data_element_id, 'period': dateStr, 'orgUnit': org_unit_id[task_id_to_name[task_id]], 'value': value}
                        outputJson.append(jsonRecord)

        print(outputJson)
            # process csv file and create output records, if csv stats file not found, data is missing

        print("Downloading complete!")

        #resp = logout(token)
    except Exception as e:
        print("Exception: ",e)

    # logout
    try:
        resp = logout(token)
    except Exception as e:
        print("Exception: ", e)

    # hdrs = {'Content-Length': 0}
    # response = post_http_data(hdrs, 'https://lpdaacsvc.cr.usgs.gov/appeears/api/login', {})
    # print("login response: ", response)
    # token = response['token']

    # hdrs = {'Content-Length': 0, 'Authorization': 'Bearer '+token}
    # print('hdrs ',hdrs)
    # sleep(4)
    # response = post_http_data(hdrs, 'https://lpdaacsvc.cr.usgs.gov/appeears/api/logout', {})
    # print("logout response: ", response)


if __name__ == '__main__':
   main()
