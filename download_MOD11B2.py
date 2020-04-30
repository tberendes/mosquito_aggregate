import sys
import json
from urllib.parse import unquote_plus, urlparse, urljoin

import numpy as np
import urllib3
import certifi
import requests
from time import sleep
import boto3 as boto3

import rasterio

from numpy import ma
from netCDF4 import Dataset as NetCDFFile

from bs4 import BeautifulSoup
from  mosquito_util import load_json_from_s3, update_status_on_s3

data_bucket = "mosquito-data"

auth = ('mosquito2019', 'Malafr#1')

s3 = boto3.resource(
    's3')

test_count = 0
# Create a urllib PoolManager instance to make requests.
http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())
#http = urllib3.PoolManager()
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

def update_status_test(bucket,request_id, type, status, message):
    global test_count
    statusJson = {"request_id": request_id, "type": type, "status": status, "message": message}
    with open("/tmp/" + request_id + "_"+ type +".json", 'w') as status_file:
        json.dump(statusJson, status_file)
    #        json.dump(districtPrecipStats, json_file)
    status_file.close()

#    bucket.upload_file("/tmp/" + request_id + "_" + type +".json",
#                                       "status/" + request_id + "_" + type +".json")
#    bucket.upload_file("/tmp/" + request_id + "_" + type +".json",
#                                       "status/" + request_id + "_" + type + str(test_count) +".json")
    bucket.upload_file("/tmp/" + request_id + "_" + type +".json",
                                       "status/" + request_id + ".json")
    bucket.upload_file("/tmp/" + request_id + "_" + type +".json",
                                       "status/" + request_id + str(test_count) +".json")
    test_count = test_count + 1

def download_imerg(subset_request, request_id, creation_time_in):

    # Define the parameters for the data subset
    download_results = []
    # Submit the subset request to the GES DISC Server
    response = get_http_data(subset_request)
    # Report the JobID and initial status
    myJobId = response['result']['jobId']
    print('Job ID: ' + myJobId)
    print('Job status: ' + response['result']['Status'])
    update_status_on_s3(s3.Bucket(data_bucket),request_id,
                        "download", "working", "initiated GES DISC order...",
                        creation_time=creation_time_in)

    # Construct JSON WSP request for API method: GetStatus
    status_request = {
        'methodname': 'GetStatus',
        'version': '1.0',
        'type': 'jsonwsp/request',
        'args': {'jobId': myJobId}
    }
    status_change_count=0
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
            update_status_on_s3(s3.Bucket(data_bucket),request_id, "download", "failed",
                                "Connection problem with GES DISC, order failed ",
                                creation_time=creation_time_in)
            sys.exit(1)
        percent = response['result']['PercentCompleted']
        print('Job status: %s (%d%c complete)' % (status, percent, '%'))
    if response['result']['Status'] == 'Succeeded':
        update_status_on_s3(s3.Bucket(data_bucket),request_id, "download", "working", "GES DISC Job Success.",
                            creation_time=creation_time_in)
        print('Job Finished:  %s' % response['result']['message'])
    else:
    #    print('Job Failed: %s' % response['fault']['code'])
        print('Job Failed: %s' % response['result']['message'])
        update_status_on_s3(s3.Bucket(data_bucket),request_id, "download", "failed",
                           "GES DISC order failed: " + response['result']['message'],
                            creation_time=creation_time_in)
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
        update_status_on_s3(s3.Bucket(data_bucket),request_id, "download", "failed",
                           "GES DISC retrieve results list failed: " + result.status_code,
                            creation_time=creation_time_in)
        sys.exit(1)
    # count the valild files
    filelist = []
    for item in urls:
        outfn = item.split('/')
        if len(outfn) <= 0:
            print('skipping unknown file '+outfn)
            continue
        outfn = outfn[len(outfn) - 1].split('?')[0]
        # skip pdf documentation files staged automatically by request
        if not outfn.endswith('.pdf'):
            entry = {"outfn": outfn, "url":item}
            filelist.append(entry)
        else:
            print('skipping documentation file '+outfn)

    numfiles = len(filelist)

    # Use the requests library to submit the HTTP_Services URLs and write out the results.
    count = 0
    for entry in filelist:
        URL = entry["url"]
        outfn = entry["outfn"]
        download_results.append("imerg/"+outfn)
        print('outfile %s ' % outfn)
        print("item " + item)
        s=requests.Session()
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

            s3.Bucket(data_bucket).upload_file(tmpfn, "imerg/"+outfn)
            count = count + 1
            update_status_on_s3(s3.Bucket(data_bucket),request_id, "download", "working", "GES DISC downloaded file "
                               + str(count)
                          + " of " + str(numfiles), creation_time=creation_time_in)
        except:
            update_status_on_s3(s3.Bucket(data_bucket),request_id, "download", "failed",
                               "GES DISC retrieve results failed on file " + str(count)
                               + " of " + str(numfiles) + ": " + str(result.status_code),
                                creation_time=creation_time_in)
            print('Error! Status code is %d for this URL:\n%s' % (result.status.code, URL))
            print('Help for downloading data is at https://disc.gsfc.nasa.gov/data-access')
            sys.exit(1)

    return download_results

def load_json(bucket, key):

    print("event key " + key)
    # strip off directory from key for temp file
    key_split = key.split('/')
    download_fn=key_split[len(key_split) - 1]
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
    #product = 'GPM_3IMERGDL_06'
    #varName = 'HQprecipitation'

    global test_count
    test_count = 0
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])

#        input_json = load_json(bucket, key)
        input_json = load_json_from_s3(s3.Bucket(bucket), key)
        if "message" in input_json and input_json["message"] == "error":
            update_status_on_s3(s3.Bucket(data_bucket),request_id, "download", "failed",
                               "load_json_from_s3 could not load " + key)
            sys.exit(1)

        dataset = input_json["dataset"]
        org_unit = input_json["org_unit"]
        agg_period = input_json["agg_period"]
        request_id = input_json["request_id"]
        print("request_id ", request_id)

        start_date = input_json['start_date']
        end_date = input_json['end_date']
        #begTime = '2015-08-01T00:00:00.000Z'
        #endTime = '2015-08-01T23:59:59.999Z'

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

        download_results=download_imerg(subset_request, request_id, creation_time_in)

        # need error check on download_imerg

        # write out file list as json file into monitored s3 bucket to trigger aggregation
        # format new json structure
        aggregateJson = {"request_id": request_id, "data_element_id": data_element_id, "variable": varName,
                         "dataset": dataset, "org_unit": org_unit, "agg_period": agg_period,
                         "s3bucket": data_bucket, "files": download_results, "stat_type":statType,
                         "creation_time":creation_time_in}

        aggregate_pathname = "requests/aggregate/precipitation/"

        with open("/tmp/" + request_id + "_aggregate.json", 'w') as aggregate_file:
            json.dump(aggregateJson, aggregate_file)
        #        json.dump(districtPrecipStats, json_file)
        aggregate_file.close()

        s3.Bucket(data_bucket).upload_file("/tmp/" + request_id + "_aggregate.json",
                                           aggregate_pathname + request_id + "_aggregate.json")

    update_status_on_s3(s3.Bucket(data_bucket),request_id, "download", "complete",
                       "All requested files successfully downloaded ", creation_time=creation_time_in)

def get_tile_hv(lat, lon, data):
    lat = 40.015
    lon = -105.2705

    in_tile = False
    i = 0
    # find vertical and horizontal tile containing lat/lon point
    while (not in_tile):
        in_tile = lat >= data[i, 4] and lat <= data[i, 5] and lon >= data[i, 2] and lon <= data[i, 3]
        i += 1
    vert = data[i - 1, 0]
    horiz = data[i - 1, 1]
    print('Horizontal Tile: ', horiz,' Vertical Tile: ', vert)
    return horiz, vert

def is_valid(url):
    """
    Checks whether `url` is a valid URL.
    """
    parsed = urlparse(url)
    return bool(parsed.netloc) and bool(parsed.scheme)

def get_all_website_links(url):
    """
    Returns all URLs that belong to the same website
    """
    # all URLs of `url`
    urls = set()
    # domain name of the URL without the protocol
    domain_name = urlparse(url).netloc
    soup = BeautifulSoup(requests.get(url).content, "html.parser")

    for a_tag in soup.findAll("a"):
        href = a_tag.attrs.get("href")
        if href == "" or href is None:
            # href empty tag
            continue
        # join the URL if it's relative (not absolute link)
        href = urljoin(url, href)
        parsed_href = urlparse(href)
        # remove URL GET parameters, URL fragments, etc.
        href = parsed_href.scheme + "://" + parsed_href.netloc + parsed_href.path
        if not is_valid(href):
            # not a valid URL
            continue
        urls.add(href)
        print("link: "+href)
    return urls

def download_url(url,filename):

    s = requests.Session()
    s.auth = auth

    try:
        r1 = s.request('get', url)
        result = s.get(r1.url)
        result.raise_for_status()
        f = open(filename, 'wb')
        f.write(result.content)
        f.close()
    except:
        # update_status_on_s3(s3.Bucket(data_bucket),request_id, "download", "failed",
        #                    "GES DISC retrieve results failed on file " + str(count)
        #                    + " of " + str(numfiles) + ": " + str(result.status_code),
        #                     creation_time=creation_time_in)
        print('Error! could not download URL: '+url)
        sys.exit(1)

    # s3.Bucket(data_bucket).upload_file(tmpfn, "imerg/" + outfn)


def main():

    # first seven rows contain header information
    # bottom 3 rows are not data
    data = np.genfromtxt('sn_bound_10deg.txt',
                         skip_header=7,
                         skip_footer=3)

    staging_url = 'https://e4ftl01.cr.usgs.gov/MOLA/MYD11B2.006/'

    test_url = 'https://e4ftl01.cr.usgs.gov/MOLA/MYD11B2.006/2020.04.06/MYD11B2.A2020097.h16v08.006.2020105174027.hdf'

    modis_data_product='MYD11B2'
    year='2020'
    day='097'
    test_od = "http://ladsweb.modaps.eosdis.nasa.gov/opendap/hyrax/allData/6/MYD11B2/2020/097/MYD11B2.A2020097.h16v08.006.2020105174027.hdf?LST_Day_6km,LST_Night_6km,Latitude,Longitude"
    nc = NetCDFFile(test_od)
    day_temp = nc.variables['LST_Day_6km'][:]
    night_temp = nc.variables['LST_Night_6km'][:]
    scale_factor = getattr(nc.variables['LST_Night_6km'],'scale_factor')
    lat = nc.variables['Latitude'][:]
    lon = nc.variables['Longitude'][:]
    print ("night_temp ", ma.getdata(night_temp)*scale_factor)
    # need to get masked values, and scale using attribute scale_factor
    print ("lat ", lat[0][0], "lon", lon[0][0])

    nc.close()

    # result = requests.get(staging_url)
    # result.raise_for_status()
    # print(result.text.splitlines())

    #get_all_website_links(staging_url)

    #download_url(test_url,"/home/dhis/tmp/MYD11B2.A2020097.h16v08.006.2020105174027.hdf")

    #fn = '/home/dhis/tmp/MYD11B2.A2020097.h16v08.006.2020105174027.hdf'

    # with rasterio.open(fn) as src:
    #     subdatasets = src.subdatasets
    # print(subdatasets)
    # # --Pull out the needed variables, lat/lon, time and precipitation.  These subsetted files only have precip param.
    # day_temp = nc.variables['MODIS_Grid_8Day_6km_LST/Data_fields/LST_Day_6km_Aggregated_from_1km'][:]
    # night_temp = nc.variables['MODIS_Grid_8Day_6km_LST/Data_fields/LST_Night_6km_Aggregated_from_1km'][:]
    # hdfeos_crs = nc.variables['MODIS_Grid_8Day_6km_LST/Data_fields/_HDFEOS_CRS'][:]

    # p_modis_grid = Proj('+proj=sinu +R=6371007.181 +nadgrids=@null +wktext')
    # x, y = p_modis_grid(0, 0)
    # # or the inverse, from x, y to lon, lat
    # lon, lat = p_modis_grid(x, y, inverse=True)



if __name__ == '__main__':
   main()
