# ---------------------------------------------------------------------------------------------
#
#  aggregate_data_orig.py
#
#  Description: as the file name suggests this script reads data from an subsetted IMERG Day granule, formatted in netCDF
#               and parses out the values based on geographic polygons (districts) and generates a JSON return
#               of mean and median values at a district level
#
#  Syntax: currently no input parameters
#
#  To Do: modify to accept input parameters, such as filename and maybe the location of district coordinates
#
# ---------------------------------------------------------------------------------------------


# --Do all the necessary imports
import statistics
import sys
import random

import boto3 as boto3
from netCDF4 import Dataset as NetCDFFile
import json
from matplotlib.patches import Polygon
import matplotlib.path as mpltPath
import uuid
from urllib.parse import unquote_plus
import datetime
from datetime import date
from datetime import timedelta

from mosquito_util import load_json_from_s3, update_status_on_s3

s3 = boto3.resource(
    's3')

data_bucket = "mosquito-data"
test_count = 20

import PIL
from PIL import Image


# creating a image object (new image object) with
# RGB mode and size 200x200
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


# def accumPrecipByDistrict(polylist, precip, lat, lon, districtPrecip,minlat,minlon,maxlat,maxlon,im):
def accumPrecipByDistrict(polylist, precip, lat, lon, districtPrecip, minlat, minlon, maxlat, maxlon):
    #    print('calc stats')
    #    districtPrecip={}
    #    r = random.randint(0, 255)
    #    g = random.randint(0, 255)
    #    b = random.randint(0, 255)
    #    width, height = im.size
    for poly in polylist:
        if poly.get_label() not in districtPrecip.keys():
            districtPrecip[poly.get_label()] = []
        #        for ptLat,ptLon,val in lat,lon,precip:
        #        print("poly ", poly.get_label())
        for i in range(lon.shape[0]):
            if lon[i] < minlon or lon[i] > maxlon:
                continue
            #            print("i ",i)
            for j in range(lat.shape[0]):
                if lat[j] < minlat or lat[j] > maxlat:
                    continue
                #                print("j ",j)
                #                print("lat ", lat[i], " lon ", lon[j], " poly ", poly.get_label())
                path = mpltPath.Path(poly.xy)
                inside = path.contains_point((lon[i], lat[j]))
                if inside:
                    # add precip value to district
                    if precip[i][j] >= 0.0:
                        districtPrecip[poly.get_label()].append(float(precip[i][j]))
                    else:
                        districtPrecip[poly.get_label()].append(0.0)


#                    im.putpixel((i,height-1-j),(r, g, b))
#                    print("lat ", lat[j], " lon ", lon[i], " precip ", precip[i][j], " inside ", poly.get_label())

def calcDistrictStats(districtPrecip, districtPrecipStats):
    for dist in districtPrecip.keys():
        if dist not in districtPrecipStats.keys():
            districtPrecipStats[dist] = {}
        if len(districtPrecip[dist]) > 0:
            #            print('len ',len(districtPrecip[dist]))
            #            print('points ',districtPrecip[dist])
            mean = statistics.mean(districtPrecip[dist])
            median = statistics.median(districtPrecip[dist])
            maxval = max(districtPrecip[dist])
            minval = min(districtPrecip[dist])
        else:
            mean = 0.0
            median = 0.0
            maxval = 0.0
            minval = 0.0
        #        meadian_high = statistics.median_high(districtPrecip[dist])
        #        meadian_low = statistics.median_low(districtPrecip[dist])
        #        std_dev = statistics.stdev(districtPrecip[dist])
        #        variance = statistics.variance(districtPrecip[dist])
        districtPrecipStats[dist] = dict([
            ('mean', mean),
            ('median', median),
            ('max', maxval),
            ('min', minval),
            ('count', len(districtPrecip[dist]))
        ])


def find_maxmin_latlon(lat, lon, minlat, minlon, maxlat, maxlon):
    if lat > maxlat:
        maxlat = lat
    if lat < minlat:
        minlat = lat
    if lon > maxlon:
        maxlon = lon
    if lon < minlon:
        minlon = lon
    return minlat, minlon, maxlat, maxlon


def process_file(geometry, dataElement, statType, precipVar, s3_bucket, key):
    districts = geometry["boundaries"]
    numDists = len(districts)

    #    print("key " + key)
    # strip off directory from key for temp file
    key_split = key.split('/')
    download_fn = key_split[len(key_split) - 1]
    s3.Bucket(s3_bucket).download_file(key, "/tmp/" + download_fn)
    nc = NetCDFFile("/tmp/" + download_fn)

    # --Pull out the needed variables, lat/lon, time and precipitation.  These subsetted files only have precip param.
    lat = nc.variables['lat'][:]
    lon = nc.variables['lon'][:]
    dayssince1970 = nc.variables['time'][...]
    #    print("dayssince1970 ", dayssince1970[0])

    StartDate = "1/1/70"
    date_1 = datetime.datetime.strptime(StartDate, "%m/%d/%y")
    end_date = date_1 + datetime.timedelta(days=dayssince1970[0])

    #    print(end_date)
    #    print(end_date.strftime("%Y%m%d"))

    dateStr = end_date.strftime("%Y%m%d")

    precip = nc.variables[precipVar][:]

    # -- eliminate unnecessary time dimension from precip variable in IMERG
    # dims are lon,lat
    precip = precip.reshape(precip.shape[1], precip.shape[2])

    # globals for precip values and stats by district
    districtPrecip = {}
    districtPrecipStats = {}
    districtPolygons = {}

    #    im = PIL.Image.new(mode="RGB", size=(lon.shape[0], lat.shape[0]), color=(255, 255, 255))

    for district in districts:
        shape = district['geometry']
        coords = district['geometry']['coordinates']
        #       name = district['properties']['name']
        name = district['name']
        dist_id = district['id']

        def handle_subregion(subregion):
            #            poly = Polygon(subregion, edgecolor='k', linewidth=1., zorder=2, label=name)
            poly = Polygon(subregion, edgecolor='k', linewidth=1., zorder=2, label=dist_id)
            return poly

        distPoly = []

        minlat = 90.0
        maxlat = -90.0
        minlon = 180.0
        maxlon = -180.0
        if shape["type"] == "Polygon":
            for subregion in coords:
                distPoly.append(handle_subregion(subregion))
                for coord in subregion:
                    minlat, minlon, maxlat, maxlon = find_maxmin_latlon(coord[1], coord[0], minlat, minlon, maxlat,
                                                                        maxlon)
        elif shape["type"] == "MultiPolygon":
            for subregion in coords:
                #            print("subregion")
                for sub1 in subregion:
                    #                print("sub-subregion")
                    distPoly.append(handle_subregion(sub1))
                    for coord in sub1:
                        minlat, minlon, maxlat, maxlon = find_maxmin_latlon(coord[1], coord[0], minlat, minlon,
                                                                            maxlat, maxlon)
        else:
            print
            "Skipping", dist_id, \
            "because of unknown type", shape["type"]
        # compute statisics
        #        accumPrecipByDistrict(distPoly, precip, lat, lon, districtPrecip,minlat,minlon,maxlat,maxlon,im)
        accumPrecipByDistrict(distPoly, precip, lat, lon, districtPrecip, minlat, minlon, maxlat, maxlon)
        districtPolygons[dist_id] = distPoly

    calcDistrictStats(districtPrecip, districtPrecipStats)
    for district in districts:
        # name = district['properties']['name']
        dist_id = district['id']
        name = district['name']
    #        print("district name ", name)
    #        print("district id", dist_id)
    #        print("mean precip ", districtPrecipStats[dist_id]['mean'])
    #        print("median precip ", districtPrecipStats[dist_id]['median'])
    #        print("max precip ", districtPrecipStats[dist_id]['max'])
    #        print("min precip ", districtPrecipStats[dist_id]['min'])
    #        print("count ", districtPrecipStats[dist_id]['count'])

    #    print("finished file " + key)
    nc.close()
    # output image
    #    im.save('/tmp/sl_img.jpg', quality=95)
    #    s3.Bucket(s3_bucket).upload_file("/tmp/sl_img.jpg", "test/" + "sl_img.jpg")

    # reformat new json structure
    #    outputJson = {'dataValues' : []}
    outputJson = []
    for key in districtPrecipStats.keys():
        value = districtPrecipStats[key][statType]
        jsonRecord = {'dataElement': dataElement, 'period': dateStr, 'orgUnit': key, 'value': value}
        outputJson.append(jsonRecord)

    return outputJson


#    return json.dumps(districtPrecipStats)

def load_json(bucket, key):
    #    print("event key " + key)
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
    statType = 'mean'
    # statType = 'median'
    # reformat new json structure
    outputJson = {'dataValues': []}

    global test_count
    test_count = 20

    test_mode = False
    if 'Records' not in event:
        test_mode = True
        print(event)
        event['Records'] = [event]

    for record in event['Records']:
        if not test_mode:
            bucket = record['s3']['bucket']['name']
            key = unquote_plus(record['s3']['object']['key'])

            #        input_json = load_json(bucket, key)
            input_json = load_json_from_s3(s3.Bucket(bucket), key)
            if "message" in input_json and input_json["message"] == "error":
                update_status_on_s3(s3.Bucket(data_bucket), request_id, "aggregate", "failed",
                                    "load_json_from_s3 could not load " + key)
                sys.exit(1)
        else:
            bucket = data_bucket
            print("record ", record)
            input_json = record

        request_id = input_json['request_id']
        data_element_id = input_json['data_element_id']
        dataset = input_json['dataset']
        org_unit = input_json['org_unit']
        agg_period = input_json['agg_period']
        s3bucket = input_json['s3bucket']
        files = input_json['files']
        variable = input_json['variable']
        creation_time_in = input_json['creation_time']
        if "stat_type" in input_json:
            statType = input_json['stat_type']
        print('stat_type' + statType)

        update_status_on_s3(s3.Bucket(s3bucket), request_id, "aggregate", "working", "loading geometry file...",
                            creation_time=creation_time_in, dataset=dataset)
        #        geometryJson = load_json(bucket, "requests/geometry/" + request_id +"_geometry.json")
        geometryJson = load_json_from_s3(s3.Bucket(bucket), "requests/geometry/" + request_id + "_geometry.json")
        if "message" in geometryJson and geometryJson["message"] == "error":
            update_status_on_s3(s3.Bucket(bucket), request_id, "aggregate", "failed",
                                "aggregate_imerge could not load geometry file " +
                                "requests/geometry/" + request_id + "_geometry.json",
                                creation_time=creation_time_in, dataset=dataset)
            sys.exit(1)

        count = 1
        num_files = len(files)
        for file in files:
            update_status_on_s3(s3.Bucket(s3bucket), request_id, "aggregate", "working", "aggregating file "
                                + str(count) + " of " + str(num_files), creation_time=creation_time_in, dataset=dataset)
            jsonRecords = process_file(geometryJson, data_element_id, statType, variable, s3bucket, file)
            for record in jsonRecords:
                outputJson['dataValues'].append(record)
            count = count + 1
        with open("/tmp/" + request_id + "_result.json", 'w') as result_file:
            json.dump(outputJson, result_file)
        result_file.close()

        s3.Bucket(bucket).upload_file("/tmp/" + request_id + "_result.json", "results/" + request_id + ".json")

        update_status_on_s3(s3.Bucket(s3bucket), request_id, "aggregate", "success", "Successfully processed "
                            + str(num_files) + " files", creation_time=creation_time_in, dataset=dataset)
