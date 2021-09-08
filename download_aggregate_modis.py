import os
import sys
import statistics
import json
from urllib.parse import unquote_plus, urlparse, urljoin
import datetime

import numpy as np
import pickle

import requests
import boto3
import botocore

from numpy import ma
from netCDF4 import Dataset as NetCDFFile

from bs4 import BeautifulSoup
from  mosquito_util import load_json_from_s3, update_status_on_s3

from matplotlib.patches import Polygon
import matplotlib.path as mpltPath
import matplotlib.path as mpltPath
from time import sleep

data_bucket = "mosquito-data"
max_retries = 10
sleep_secs = 5
missing_value = -9999.0
return_missing_values = False

auth = ('mosquito2019', 'Malafr#1')

s3 = boto3.resource(
    's3')
# def accumVariableByDistrict(polylist, variable, mask, lat, lon, districtVariable, minlat, minlon, maxlat, maxlon):
#
#     for poly in polylist:
#         if poly.get_label() not in districtVariable.keys():
#             districtVariable[poly.get_label()] = []
#         #        for ptLat,ptLon,val in lat,lon,Variable:
#         #        print("poly ", poly.get_label())
#         for i in range(lon.shape[0]):
#             for j in range(lon.shape[1]):
#                 if not mask[i][j]:
#                     continue
#                 if lon[i][j] < minlon or lon[i][j] > maxlon:
#                     continue
#                 #            print("i ",i)
#                 if lat[i][j] < minlat or lat[i][j] > maxlat:
#                     continue
#                 #                print("j ",j)
#                 #                print("lat ", lat[i], " lon ", lon[j], " poly ", poly.get_label())
#                 path = mpltPath.Path(poly.xy)
#                 inside = path.contains_point((lon[i][j], lat[i][j]))
#                 if inside:
#                     # add Variable value to district
#                     if variable[i][j] >= 0.0:
#                         districtVariable[poly.get_label()].append(float(variable[i][j]))
#                     else:
#                         districtVariable[poly.get_label()].append(0.0)
# #                    im.putpixel((i,height-1-j),(r, g, b))
# #                    print("lat ", lat[j], " lon ", lon[i], " variable ", variable[i][j], " inside ", poly.get_label())
def accumVariableByDistrict(polylist, variable, mask, lat, lon, districtVariable,
                            minlat, minlon, maxlat, maxlon, valid_min, valid_max,district_i_j_list):

    for poly in polylist:
        if poly.get_label() not in districtVariable.keys():
            districtVariable[poly.get_label()] = []
        if poly.get_label() not in district_i_j_list.keys():
            district_i_j_list[poly.get_label()] = []
        #        for ptLat,ptLon,val in lat,lon,Variable:
        #        print("poly ", poly.get_label())
    #print("lon.shape[0] ", lon.shape[0])
    #print("lon.shape[1] ", lon.shape[1])
    for i in range(lon.shape[0]):
        for j in range(lon.shape[1]):
            # mask is not reliable, used for NDVI, but not for LST, for now we will not use it
            #if not mask[i][j]:
            # if mask[i][j]:
            #     continue
            if lon[i][j] < minlon or lon[i][j] > maxlon:
                continue
            #            print("i ",i)
            if lat[i][j] < minlat or lat[i][j] > maxlat:
                continue
            #                print("j ",j)
            #                print("lat ", lat[i], " lon ", lon[j], " poly ", poly.get_label())
            if variable[i][j] < valid_min or variable[i][j] > valid_max:
                continue
            for poly in polylist:
                path = mpltPath.Path(poly.xy)
                inside = path.contains_point((lon[i][j], lat[i][j]))
                if inside:
                    # add Variable value to district
                    # need to change this to check against a fill value
                    #if variable[i][j] >= valid_min and variable[i][j] <= valid_max:
                    districtVariable[poly.get_label()].append(float(variable[i][j]))
                    district_i_j_list[poly.get_label()].append([i,j])
                    # values of zero or below are missing, cloud contamination in 8day composite, do not use
                    # else:
                    #     districtVariable[poly.get_label()].append(0.0)
                    break # only allow membership in one polygon, doesn't allow for overlapping regions

#                    im.putpixel((i,height-1-j),(r, g, b))
#                    print("lat ", lat[j], " lon ", lon[i], " variable ", variable[i][j], " inside ", poly.get_label())
    return
def accumVariableByDictionary(variable, districtVariable, district_i_j_list,
                            valid_min, valid_max):
    for district, coords in district_i_j_list.items():
        if district not in districtVariable:
            districtVariable[district] = []
        for i_j in coords:
            i = i_j[0]
            j = i_j[1]
            if variable[i][j] >= valid_min and variable[i][j] <= valid_max:
                districtVariable[district].append(float(variable[i][j]))
    return

def calcDistrictStats(districtVariable):
    districtVariableStats = {}
    for dist in districtVariable.keys():
        if dist not in districtVariableStats.keys():
            districtVariableStats[dist] = {}
        if len(districtVariable[dist]) > 0:
            #            print('len ',len(districtVariable[dist]))
            #            print('points ',districtVariable[dist])
            mean = statistics.mean(districtVariable[dist])
            median = statistics.median(districtVariable[dist])
            maxval = max(districtVariable[dist])
            minval = min(districtVariable[dist])
        else:
            mean = missing_value
            median = missing_value
            maxval = missing_value
            minval = missing_value
        #        meadian_high = statistics.median_high(districtVariable[dist])
        #        meadian_low = statistics.median_low(districtVariable[dist])
        #        std_dev = statistics.stdev(districtVariable[dist])
        #        variance = statistics.variance(districtVariable[dist])
        districtVariableStats[dist] = dict([
            ('mean', mean),
            ('median', median),
            ('max', maxval),
            ('min', minval),
            ('count', len(districtVariable[dist]))
        ])
    return districtVariableStats

def find_maxmin_latlon(lat,lon,minlat,minlon,maxlat,maxlon):
    if lat > maxlat:
        maxlat = lat
    if lat < minlat:
        minlat = lat
    if lon > maxlon:
        maxlon = lon
    if lon < minlon:
        minlon = lon
    return minlat,minlon,maxlat,maxlon

def download_opendap(url, filename):
    # Use the requests library to submit the HTTP_Services URLs and write out the results.
    s = requests.Session()
    s.auth = auth

    try:
        r1 = s.request('get', url)
        result = s.get(r1.url)
        result.raise_for_status()
        tmpfn = '/tmp/' + filename
        f = open(tmpfn, 'wb')
        f.write(result.content)
        f.close()
        print("downloaded url: "+ url)

    except Exception as e:
        print('Exception ',e)
        print("Error: failed to download url: "+ url)
        sys.exit(1)

    return tmpfn
def get_variable(nc,var_name, x_start_stride_stop, y_start_stride_stop):
    x_start = 0
    x_stride = 0
    x_stop = 0
    y_start = 0
    y_stride = 0
    y_stop = 0
    if ':' in x_start_stride_stop:
        x_start = int(x_start_stride_stop[1:-1].split(':')[0])
        x_stride = int(x_start_stride_stop[1:-1].split(':')[1])
        x_stop = int(x_start_stride_stop[1:-1].split(':')[2])
    if ':' in y_start_stride_stop:
        y_start = int(y_start_stride_stop[1:-1].split(':')[0])
        y_stride = int(y_start_stride_stop[1:-1].split(':')[1])
        y_stop = int(y_start_stride_stop[1:-1].split(':')[2])
        #subset_string=subset_string+'_'+str(y_start)+'_'+str(y_stride)+'_'+str(y_stop)
    retry = 0
    var = None
    while True:
        try:
            if len(x_start_stride_stop) > 0 and len(y_start_stride_stop) > 0:
                print("subsetting  variable " +var_name + ":"+ x_start_stride_stop + y_start_stride_stop)
                print("dimensions ", ma.getdata(nc.variables[var_name]).shape)
                # variables are indexed y,x
                variable = nc.variables[var_name][y_start:y_stop:y_stride,x_start:x_stop:x_stride]
                print("new dimensions ", ma.getdata(variable).shape)
            else:
                variable = nc.variables[var_name][:]
            success = True
            print("Successfully read ", var_name)
            break
        except Exception as e:
            print("Exception ", e)
            print("Network error reading variable ", var_name)
            retry = retry + 1
            print("retry ", retry, " of ", max_retries, "...")
            if retry < max_retries:
                sleep(sleep_secs)
                continue
            else:
                print("retries: ", retry)
                raise Exception("Network error reading variable" + var_name ) from e
        break
    return variable

def process_files(bucket, geometry, dataElement, statType, var_name, opendapUrls,
                  x_start_stride_stop, y_start_stride_stop, dhis_dist_version):

    # dictionaries for computing stats by district
    districtVariable = {}
    #districtVariableStats = {}
    #districtPolygons = {}

    # subset_str = '_'+x_start_stride_stop.replace('[','').replace(']','').replace(':','_').strip()+'_'+\
    #              y_start_stride_stop.replace('[','').replace(']','').replace(':','_').strip()
    # print('subset_str ',subset_str)
    districts = geometry["boundaries"]
    dateStr = ""
    # all urls are for the same date
    for opendapUrl in opendapUrls:
        # look for district tile mapping files

        # extract data type and tile string from url
        #http://ladsweb.modaps.eosdis.nasa.gov/opendap/allData/6/MOD13A2/2019/001/MOD13A2.A2019001.h16v07.006.2019024152500.hdf?Latitude[0:5:1199][0:5:1199],Longitude[0:5:1199][0:5:1199],_1_km_16_days_NDVI[0:5:1199][0:5:1199]
        base = os.path.basename(opendapUrl)
        data_type = base.split('.')[0]
        tile_str = base.split('.')[2]
        mod_ver = base.split('.')[3]

        district_i_j_list = {}
#        tile_file = data_type+'_'+mod_ver+'_'+tile_str+'_'+dhis_dist_version+subset_str+'.pkl'
        tile_file = data_type+'_'+mod_ver+'_'+tile_str+'_'+dhis_dist_version+'.pkl'
        tile_map_exists = False
        #check for existence of tile file
        print("checking for tile file...")
        try:
            s3.Bucket(bucket).download_file("mod_tile/"+tile_file, "/tmp/"+tile_file)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                # The object does not exist.
                tile_map_exists = False
                print("tile map file " + tile_file + " doesn't exist, creating...")
            else:
                # Something else has gone wrong.
                print("error reading tile map file")
                print("error: ", e)
                raise Exception("error reading tile map file") from e
        else:
            # The object does exist, read it into numpy dictionary and set flag
            print("loading tile map file " + tile_file)
            tile_map_exists = True
            f = open("/tmp/"+tile_file, "rb")
            district_i_j_list = pickle.load(f)
            f.close()

        print("opening url ",opendapUrl)
        #netcdf_file = download_opendap(opendapUrl, str(opendapUrl).split('?')[0].split('/')[-1])
        netcdf_file = opendapUrl
        # add error check and retry to this
        retry = 0
        while True:
            try:
                nc = NetCDFFile(netcdf_file)
                success = True
                print("Successfully opened url ", netcdf_file)
                break
            except Exception as e:
                print("Exception ",e)
                print("Network error opening url ", netcdf_file)
                retry = retry + 1
                print("retry ", retry, " of ", max_retries, "...")
                if retry < max_retries:
                    sleep(sleep_secs)
                    continue
                else:
                    print("retries: ",retry)
                    raise Exception("Network error opening url, maximum retries exceeded") from e
            break


        # tries = 3
        # for i in range(max_retries):
        #     try:
        #         nc = NetCDFFile(opendapUrl)
        #         print("Successfully opened url ", opendapUrl)
        #     except Exception as e:
        #         if i < tries - 1:  # i is zero indexed
        #             continue
        #         else:
        #             raise
        #     break

        # auto scale doesn't seem to work on temp data, so set to false and manually scale
        print("reading variables...")

        retry = 0
        while True:
            try:

                nc.set_auto_scale(False)
                print("reading variable...")
                variable = get_variable(nc,var_name, x_start_stride_stop, y_start_stride_stop)
                # if variable is None:
                #     print("Network error reading variable "+var_name)
                #     raise Exception("Network error reading variable "+var_name)
                #print("variable.data:  " + var_name + " ", variable.data)
                mask = ma.getmask(variable)
                print("reading attributes...")
                scale_factor = getattr(nc.variables[var_name], 'scale_factor')
                #print("scale_factor", scale_factor)
                add_offset = getattr(nc.variables[var_name], 'add_offset')
                #print("add_offset", add_offset)
                #modis_var = ma.getdata(variable)
                modis_var = ma.getdata(variable) * scale_factor+add_offset
                #print("scaled variable:  " + var_name + " ", modis_var)
                valid_range = getattr(nc.variables[var_name], 'valid_range')
                print("variable ", var_name, "valid_range", valid_range)
                valid_min=float(valid_range[0])*scale_factor+add_offset
                valid_max=float(valid_range[1])*scale_factor+add_offset
                if valid_max < valid_min: # unsigned short int interpreted as negative
                    print("valid_max < valid_min, converting from unsigned short...")
                    valid_max = float(int(valid_range[1]& 0xffff))*scale_factor+add_offset

                print("valid_min ", valid_min)
                print("valid_max ", valid_max)

                print("reading lat...")
                lat = get_variable(nc,'Latitude', x_start_stride_stop, y_start_stride_stop)
                # if lat is None:
                #     print("Network error reading Latitude")
                #     raise Exception("Network error reading Latitude ")
                print("reading lon...")
                lon = get_variable(nc,'Longitude', x_start_stride_stop, y_start_stride_stop)
                # if lon is None:
                #     print("Network error reading Longitude")
                #     raise Exception("Network error reading Longitude ")
            except:
                print("Exception ", e)
                print("Network error reading data ", netcdf_file)
                retry = retry + 1
                print("retry ", retry, " of ", max_retries, "...")
                if retry < max_retries:
                    sleep(sleep_secs)
                    continue
                else:
                    print("retries: ", retry)
                    raise Exception("Network error reading file, maximum retries exceeded") from e
            break

        # need to get masked values, and scale using attribute scale_factor
        #print("mask:  " + var_name + " ", mask)

        # strip out yyyyddd from opendap url
        tempStr = os.path.basename(opendapUrl).split('.')[1]
        year = int(tempStr[1:5])
        days = int(tempStr[5:8])
        print("year "+str(year)+ " days "+str(days))
        startTime = datetime.datetime(year, 1, 1) + datetime.timedelta(days - 1)
        dateStr = startTime.strftime("%Y%m%d")

    #    im = PIL.Image.new(mode="RGB", size=(lon.shape[0], lat.shape[0]), color=(255, 255, 255))

        for district in districts:
            if tile_map_exists:
                accumVariableByDictionary(modis_var, districtVariable, district_i_j_list, valid_min, valid_max)
            else:
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
                            minlat, minlon, maxlat, maxlon = find_maxmin_latlon(coord[1], coord[0], minlat, minlon, maxlat, maxlon)
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
        #        accumVariableByDistrict(distPoly, variable, lat, lon, districtVariable,minlat,minlon,maxlat,maxlon,im)
                accumVariableByDistrict(distPoly, modis_var, mask, lat, lon,
                                        districtVariable,minlat,minlon,maxlat,maxlon,
                                        valid_min, valid_max,district_i_j_list)
                #districtPolygons[dist_id] = distPoly


        #    print("finished file " + key)
        nc.close()

        if not tile_map_exists:
            print("saving tile map file " +tile_file)
            f = open("/tmp/"+tile_file, "wb")
            pickle.dump(district_i_j_list, f)
            f.close()
            s3.Bucket(bucket).upload_file("/tmp/" + tile_file, "mod_tile/" + tile_file)
        # output image
    #    im.save('/tmp/sl_img.jpg', quality=95)
    #    s3.Bucket(s3_bucket).upload_file("/tmp/sl_img.jpg", "test/" + "sl_img.jpg")

    # reformat new json structure
#    outputJson = {'dataValues' : []}
    districtVariableStats = calcDistrictStats(districtVariable)
    # for district in districts:
    #    # name = district['properties']['name']
    #     dist_id = district['id']
    #     name = district['name']
    #     print("district name ", name)
    #     print("district id", dist_id)
    #     print("mean Variable ", districtVariableStats[dist_id]['mean'])
    #     print("median Variable ", districtVariableStats[dist_id]['median'])
    #     print("max Variable ", districtVariableStats[dist_id]['max'])
    #     print("min Variable ", districtVariableStats[dist_id]['min'])
    #     print("count ", districtVariableStats[dist_id]['count'])
    outputJson = []
    for key in districtVariableStats.keys():
        value = districtVariableStats[key][statType]
        jsonRecord = {'dataElement': dataElement, 'period': dateStr, 'orgUnit': key, 'value': value}
        # TODO test this...
        if not return_missing_values:
            if value != missing_value:
                outputJson.append(jsonRecord)
        else:
            outputJson.append(jsonRecord)

    return outputJson

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

def get_tile_hv(lon,lat, data):
    in_tile = False
    i = 0
    # find vertical and horizontal tile containing lat/lon point
    while (not in_tile):
        in_tile = lat >= data[i, 4] and lat <= data[i, 5] and lon >= data[i, 2] and lon <= data[i, 3]
        i += 1
    vert = data[i - 1, 0]
    horiz = data[i - 1, 1]
    print('Horizontal Tile: ', horiz,' Vertical Tile: ', vert)
    return int(horiz), int(vert)

def is_valid(url):
    """
    Checks whether `url` is a valid URL.
    """
    parsed = urlparse(url)
    return bool(parsed.netloc) and bool(parsed.scheme)

def get_href(url, substr):
    # all URLs of `url`
    contents = []
    # domain name of the URL without the protocol
    #print("url ", url)
    soup = BeautifulSoup(requests.get(url).content, "html.parser")

    for a_tag in soup.findAll("a"):
        href = a_tag.attrs.get("href")
        if href == "" or href is None:
            # href empty tag
            continue
        # join the URL if it's relative (not absolute link)
        href = urljoin(url, href)
#        print("href ", href)
        parsed_href = urlparse(href)
#        print("parsed href ", parsed_href)
        if not is_valid(href) or str(href).find(substr) < 0 or str(href) == url:
            # not a valid URL
            continue
#        print("added: " + href)
        contents.append(href)
    return contents


def get_dates(url, start_year,end_year):
    """
    Returns all date encoded sub-directories in the url
    """
    # all URLs of `url`
    dates = []

    for year in range(start_year, end_year + 1):
        # domain name of the URL without the protocol
        #print("url ", url)
        content = url+str(year)+"/contents.html"
        #print("content ",content)
        days = get_href(content, "contents.html")
        #print("days ",days)
        for day in days:
            dates.append(day)
    return dates

def get_filenames(url, start_date, end_date, tiles):
    """
    Returns a list of filenames for the horiz and vert indices of the sinusoidal projection for the
    specified list of dates known to have data (returned from get_date_dirs)
    """
    # all URLs of `url`
    # create dictionary of file lists by dates
    files = {}
    #files = []
    # domain name of the URL without the protocol
    start_year = int(start_date[0:4])
    end_year = int(end_date[0:4])
    #print("start year ", start_year, " end year ", end_year)

    dates = get_dates(url, start_year, end_year)

    # strip out yyyyddd from opendap url i.e. 2015-08-01... YYYY-mm-dd
    year = int(start_date[0:4])
    month = int(start_date[5:7])
    day = int(start_date[8:10])
    #print("start date " + str(year) + "-" + str(month)+ "-" + str(day))
    startDatetime = datetime.datetime(year, month, day) # + datetime.timedelta(days - 1)
    year = int(end_date[0:4])
    month = int(end_date[5:7])
    day = int(end_date[8:10])
    #print("end date " + str(year) + "-" + str(month)+ "-" + str(day))
    endDatetime = datetime.datetime(year, month, day) # + datetime.timedelta(days - 1)
    #dateStr = startTime.strftime("%Y%m%d")

    for date in dates:
        # strip year/jday out of file urls
        # i.e. https://ladsweb.modaps.eosdis.nasa.gov/opendap/allData/6/MOD11B2/2018/001/contents.html
        year = int(date.split("/")[-3])
        jday = int(date.split("/")[-2])
        thisDatetime = datetime.datetime(year, 1, 1)  + datetime.timedelta(jday - 1)
        found_tiles = {}
        if thisDatetime >=startDatetime and thisDatetime <= endDatetime:
            # find h and v tiles
            date_str = thisDatetime.strftime("%Y%m%d")
            if date_str not in files:
                files[date_str] = []
            for tile in tiles:
                #date_str = date.replace('-','.',2)
                hv_str = 'h{:02d}v{:02d}'.format(tile[0],tile[1])
                print("hv "+hv_str)
                dayContents = get_href(date, hv_str)
                for tile_file in dayContents:
                    #if str(tile_file).find(hv_str)>=0 and str(tile_file).endswith('hdf.html'):
                    if str(tile_file).endswith('hdf.html'):
                        if not tile_file in found_tiles.keys():
                            print("found file ", tile_file)
                            found_tiles[tile_file]=True
                            files[date_str].append(str(tile_file).split('.html')[0])
                        else:
                            continue
    return files

def get_opendap_urls(var_name, x_start_stride_stop, y_start_stride_stop, filenames):
    # construct opendap url from info in MODIS filenames
    # extract year, jday from filename
    #opendap_urls=[]
    opendap_urls= {}
    for date in filenames.keys():
        if date not in opendap_urls:
            opendap_urls[date]=[]
        for filename in filenames[date]:
            od_url= filename \
                   + '?Latitude'+x_start_stride_stop+y_start_stride_stop\
                   + ',Longitude'+x_start_stride_stop+y_start_stride_stop+',' \
                   + var_name + x_start_stride_stop+y_start_stride_stop
            print("Opendap url: " + od_url)
            opendap_urls[date].append(od_url)

    # i.e. "http://ladsweb.modaps.eosdis.nasa.gov/opendap/hyrax/allData/6/MYD11B2/2020/097/MYD11B2.A2020097.h16v08.006.2020105174027.hdf?LST_Day_6km,LST_Night_6km,Latitude,Longitude"
    return opendap_urls

def lambda_handler(event, context):
    #    product = 'GPM_3IMERGDE_06'
    # use "Late" product
    #product = 'GPM_3IMERGDL_06'
    #varName = 'HQprecipitation'

    test_count = 0
    outputJson = {'dataValues' : []}

    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])

#        input_json = load_json(bucket, key)
        input_json = load_json_from_s3(s3.Bucket(bucket), key)
        if "message" in input_json and input_json["message"] == "error":
            update_status_on_s3(s3.Bucket(data_bucket),request_id, "aggregate", "failed",
                               "load_json_from_s3 could not load " + key)
            sys.exit(1)

        dataset = input_json["dataset"]
        org_unit = input_json["org_unit"]
        agg_period = input_json["agg_period"]
        request_id = input_json["request_id"]
        print("request_id ", request_id)

        start_date = input_json['start_date'].split('T')[0]
        end_date = input_json['end_date'].split('T')[0]
        #begTime = '2015-08-01T00:00:00.000Z'
        #endTime = '2015-08-01T23:59:59.999Z'

        minlon = input_json['min_lon']
        maxlon = input_json['max_lon']
        minlat = input_json['min_lat']
        maxlat = input_json['max_lat']

        # read MODIS Land sinusoidal tile boundaries from data file
        # first seven rows contain header information
        # bottom 3 rows are not data
        data = np.genfromtxt('sn_bound_10deg.txt',
                             skip_header=7,
                             skip_footer=3)

        # find all MODIS Land tiles containing the region of interest
        #tiles = [[16, 8]]
        tiles = []
        min_h, min_v = get_tile_hv(minlon,maxlat, data)
        max_h, max_v = get_tile_hv(maxlon,minlat, data)

        print("min_h ",min_h)
        print("max_h ",max_h)
        print("min_v ",min_v)
        print("max_v ",max_v)
        for i in range(min_h,max_h+1):
            for j in range(min_v,max_v+1):
                tiles.append([i,j])
        print("tiles: ", tiles)

        creation_time_in = input_json['creation_time']
        date_range_in = start_date + " -> "+ end_date

        geometryJson = load_json_from_s3(s3.Bucket(bucket), "requests/geometry/" + request_id +"_geometry.json")
        if "message" in geometryJson and geometryJson["message"] == "error":
            update_status_on_s3(s3.Bucket(bucket),request_id, "aggregate", "failed",
                               "aggregate_imerge could not load geometry file " +
                               "requests/geometry/" + request_id +"_geometry.json",
                                creation_time=creation_time_in, date_range=date_range_in, dataset=dataset)
            sys.exit(1)

        # defaults
        statType = 'mean'
        product = 'MOD11B2'
        varName = 'LST_Day_6km'
        #currently hard coded, could add as parameters to support config file
        modis_version = 6
#        listing_site = 'e4ftl01.cr.usgs.gov'
        opendap_site = 'ladsweb.modaps.eosdis.nasa.gov'
        #opendap_path = 'opendap/hyrax/allData/'
        opendap_path = 'opendap/allData'

        if "stat_type" in input_json:
            statType = input_json['stat_type']
        print('stat_type ' + statType)
        if "product" in input_json:
            product = input_json['product']
        print('product' + product)
        if "var_name" in input_json:
            varName = input_json['var_name']
        print('var_name' + varName)
        dhis_dist_version = 'default'
        if "dhis_dist_version" in input_json:
            dhis_dist_version = input_json['dhis_dist_version']
        print('dhis_dist_version ' + dhis_dist_version)

        data_element_id = input_json['data_element_id']

#        modis_version_string = '{:03d}'.format(modis_version)
        modis_version_string = '{:d}'.format(modis_version)
        print("modis_version_string " + modis_version_string)
        product = input_json['product']
        var_name = input_json['var_name']
        x_start_stride_stop = ""
        if "x_start_stride_stop" in input_json:
            x_start_stride_stop = input_json["x_start_stride_stop"]
        y_start_stride_stop = ""
        if "y_start_stride_stop" in input_json:
            y_start_stride_stop = input_json["y_start_stride_stop"]
        add_string = ""
        if "[" in x_start_stride_stop and "]" in x_start_stride_stop:
            add_string = "_" + x_start_stride_stop[1:len(x_start_stride_stop)-1].replace(':','_',2)
        if "[" in y_start_stride_stop and "]" in y_start_stride_stop:
            add_string = add_string + "_" + y_start_stride_stop[1:len(y_start_stride_stop)-1].replace(':','_',2)
        print("add_string "+add_string)

#https://ladsweb.modaps.eosdis.nasa.gov/opendap/hyrax/allData/6/MOD11B2/2018/
#        listing_url = 'https://' + listing_site + '/' + sat_dir + '/' + product + '.' + modis_version_string + '/'
        listing_url = 'https://' + opendap_site + '/' + opendap_path + '/'+ modis_version_string + '/' + product + '/'
        print("listing_url: " + listing_url)

        # set up opendap urls using filenames from direct access site.  With opendap we can request only the variables
        # we need and we can get corresponding lat/lon as variables and we don't have to deal with sinusoidal projection
        update_status_on_s3(s3.Bucket(data_bucket), request_id,
                            "aggregate", "working", "retrieving filenames",
                            creation_time=creation_time_in, date_range=date_range_in, dataset=dataset)


        #start_year = int(start_date[0:4])
        #end_year = int(end_date[0:4])
        #all_dates = get_dates(listing_url, start_year, end_year)

        #print("all_dates ", all_dates)
        #exit(0)

        filenames = get_filenames(listing_url, start_date, end_date, tiles)

        update_status_on_s3(s3.Bucket(data_bucket), request_id,
                            "aggregate", "working", "Constructing OpenDAP URLs",
                            creation_time=creation_time_in, date_range=date_range_in, dataset=dataset)
        #opendap_urls = get_opendap_urls(var_name,x_start_stride_stop, y_start_stride_stop, filenames)
        opendap_urls = get_opendap_urls(var_name, '', '', filenames)

        print("opendap_urls: ", opendap_urls)
        # use netcdf to directly access the opendap URLS and return the variables we want
        numFiles=0
        for date in opendap_urls.keys():
            numFiles = numFiles + len(opendap_urls[date])
        numDates = len(opendap_urls.keys())
        fileCnt = 1
        for date in opendap_urls.keys():
            update_status_on_s3(s3.Bucket(data_bucket), request_id,
                                "aggregate", "working", "Aggregating file " + str(fileCnt) + " of " + str(numFiles),
                                creation_time=creation_time_in, date_range=date_range_in, dataset=dataset)
 #           for opendap_url in opendap_urls[date]:
                # nc = NetCDFFile(opendap_url)
                # variable = nc.variables[var_name][:]
                # scale_factor = getattr(nc.variables[var_name], 'scale_factor')
                # lat = nc.variables['Latitude'][:]
                # lon = nc.variables['Longitude'][:]
                # print("Variable:  " + var_name + " ", ma.getdata(variable) * scale_factor)
                # # need to get masked values, and scale using attribute scale_factor
                # print("lat ", lat[0][0], "lon", lon[0][0])
                # fileCnt = fileCnt + 1
                # nc.close()
            try:
                jsonRecords = process_files(bucket, geometryJson, data_element_id, statType, var_name,
                                        opendap_urls[date], x_start_stride_stop, y_start_stride_stop,
                                        dhis_dist_version+add_string)
                print("Successfully processed  files for date ", date)
            except Exception as e:
                print("returning exception ",e)
                print("Error reading files for date ", date, " exiting...")
                #continue
                update_status_on_s3(s3.Bucket(data_bucket), request_id, "download", "failed",
                                    "Error reading files for date" + date,
                                    creation_time=creation_time_in, date_range=date_range_in, dataset=dataset)
                exit(-1)
            for record in jsonRecords:
                outputJson['dataValues'].append(record)
            fileCnt = fileCnt + len(opendap_urls[date])
        with open("/tmp/" +request_id+"_result.json", 'w') as result_file:
            json.dump(outputJson, result_file)
        result_file.close()

        s3.Bucket(bucket).upload_file("/tmp/" + request_id+"_result.json", "results/" +request_id+".json")

    update_status_on_s3(s3.Bucket(data_bucket),request_id, "aggregate", "success",
                       "All requested files successfully aggregated", creation_time=creation_time_in,
                        date_range=date_range_in, dataset=dataset)


# if __name__ == '__main__':
#    main()
