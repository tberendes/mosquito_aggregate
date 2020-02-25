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

s3 = boto3.resource(
    's3')

def accumPrecipByDistrict(polylist, precip, lat, lon, districtPrecip):
    #    print('calc stats')
    #    districtPrecip={}
    for poly in polylist:
        if poly.get_label() not in districtPrecip.keys():
            districtPrecip[poly.get_label()] = []
        #        for ptLat,ptLon,val in lat,lon,precip:
        #        print("poly ", poly.get_label())
        for i in range(lon.shape[0]):
            #            print("i ",i)
            for j in range(lat.shape[0]):
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
        else:
            mean = 0.0
            median = 0.0
        #        meadian_high = statistics.median_high(districtPrecip[dist])
        #        meadian_low = statistics.median_low(districtPrecip[dist])
        #        std_dev = statistics.stdev(districtPrecip[dist])
        #        variance = statistics.variance(districtPrecip[dist])
        districtPrecipStats[dist] = dict([
            ('mean', mean),
            ('median', median)
        ])


def process_file(geometry, dataElement, statType, precipVar, s3_bucket, key):

    districts = geometry["boundaries"]
    numDists = len(districts)

#    print("key " + key)
    # strip off directory from key for temp file
    key_split = key.split('/')
    download_fn=key_split[len(key_split) - 1]
    s3.Bucket(s3_bucket).download_file(key, "/tmp/" + download_fn)
    nc = NetCDFFile("/tmp/" +download_fn)

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
        if shape["type"] == "Polygon":
            for subregion in coords:
                distPoly.append(handle_subregion(subregion))
        elif shape["type"] == "MultiPolygon":
            for subregion in coords:
                #            print("subregion")
                for sub1 in subregion:
                    #                print("sub-subregion")
                    distPoly.append(handle_subregion(sub1))
        else:
            print
            "Skipping", dist_id, \
            "because of unknown type", shape["type"]
        # compute statisics
        accumPrecipByDistrict(distPoly, precip, lat, lon, districtPrecip)
        districtPolygons[dist_id] = distPoly

    calcDistrictStats(districtPrecip, districtPrecipStats)
    for district in districts:
       # name = district['properties']['name']
        dist_id = district['id']
#        print("district ", dist_id)
#        print("mean precip ", districtPrecipStats[dist_id]['mean'])
#        print("median precip ", districtPrecipStats[dist_id]['median'])

    print("aggregate_imerg done")
    nc.close()

    # reformat new json structure
#    outputJson = {'dataValues' : []}
    outputJson = []
    for key in districtPrecipStats.keys():
        value = districtPrecipStats[key][statType]
        jsonRecord = {'dataElement':dataElement,'period':dateStr,'orgUnit':key,'value':value}
        outputJson.append(jsonRecord)

    return outputJson

#    return json.dumps(districtPrecipStats)

def load_json(bucket, key):

#    print("event key " + key)
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
        jsonData = {"message": "Error reading json file"}

    return jsonData

def lambda_handler(event, context):

    #    statType='mean'
    statType = 'median'
    # reformat new json structure
    outputJson = {'dataValues' : []}

    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])

        input_json = load_json(bucket, key)

        request_id = input_json['request_id']
        data_element_id = input_json['data_element_id']
        dataset = input_json['dataset']
        org_unit = input_json['org_unit']
        agg_period = input_json['agg_period']
        s3bucket = input_json['s3bucket']
        files = input_json['files']
        variable = input_json['variable']

        geometryJson = load_json(bucket, "requests/geometry/" + request_id +"_geometry.json")

        for file in files:
            jsonRecords = process_file(geometryJson, data_element_id, statType, variable, s3bucket, file)
            for record in jsonRecords:
                outputJson['dataValues'].append(record)

        with open("/tmp/" +request_id+"_result.json", 'w') as result_file:
            json.dump(outputJson, result_file)
        result_file.close()

        s3.Bucket(bucket).upload_file("/tmp/" + request_id+"_result.json", "results/" +request_id+".json")
