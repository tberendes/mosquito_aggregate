import sys
import statistics
import json
from urllib.parse import unquote_plus, urlparse, urljoin
import datetime
from pydap.client import open_url
from pydap.cas.urs import setup_session

import numpy as np
import pickle

import requests
import boto3
import botocore

from numpy import ma
from netCDF4 import Dataset as NetCDFFile

from bs4 import BeautifulSoup
from mosquito_util import load_json_from_s3, update_status_on_s3

from matplotlib.patches import Polygon
import matplotlib.path as mpltPath
import matplotlib.path as mpltPath
from time import sleep

data_bucket = "mosquito-data"
max_retries = 20
sleep_secs = 5
missing_value = -9999.0
return_missing_values = False

s3 = boto3.resource(
    's3')

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
    # print("url ", url)

    # get request for URL
    req = requests.get(url, timeout=10)
    # check for status
    retry = 0
    while req.status_code != 200: # Not "ok"
        print("error code: ", req.status_code)
        print("get_href error in requests.get(), retrying...")
        print("retry ", retry+1, " of ", max_retries, "...")
        #if retry < max_retries and not timeout_is_near():
        if retry < max_retries:
            req = requests.get(url)
            if req.status_code == 200:
                break
            else:
                sleep(sleep_secs)
                retry = retry + 1
        else:
            print("exceeded maximum retries, retries: ", retry)
            print("Error: get_href network error at "+url)
            raise Exception("Error: get_href network error at "+url)
    print("parsing url: "+url)

    soup = BeautifulSoup(req.content, "html.parser")

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

def get_months(url, start_year,start_month, end_year, end_month):
    """
    Returns all date encoded sub-directories in the url
    https://gpm1.gesdisc.eosdis.nasa.gov/opendap/hyrax/GPM_L3/GPM_3IMERGDF.06/2021/09/contents.html
    expect url as base of data type, and expand directories for year/month
    i.e. https://gpm1.gesdisc.eosdis.nasa.gov/opendap/hyrax/GPM_L3/GPM_3IMERGDF.06
    """
    # all URLs of `url`
    dates = []

    try:
        for year in range(start_year, end_year + 1):
            # domain name of the URL without the protocol
            # print("url ", url)
            content = url + str(year) + "/contents.html"
            # print("content ",content)
            days = get_href(content, "contents.html")
            # print("days ",days)
            for day in days:
                dates.append(day)
    except Exception as e:
        raise e

    return dates

def get_filenames(url, start_date, end_date):
    """
    https://gpm1.gesdisc.eosdis.nasa.gov/opendap/hyrax/GPM_L3/GPM_3IMERGDF.06/2021/09/contents.html
    Returns a list of filenames
    """
    # all URLs of `url`
    # create dictionary of file lists by dates
    #files = {}
    files = []
    found_dates = {}
    # domain name of the URL without the protocol
    start_year = int(start_date[0:4])
    end_year = int(end_date[0:4])
    # print("start year ", start_year, " end year ", end_year)

    try:
        # find starting year/month and use date to increment until ending year/month
        # strip out yyyyddd from opendap url i.e. 2015-08-01... YYYY-mm-dd
        start_year = int(start_date[0:4])
        start_month = int(start_date[5:7])
        day = int(start_date[8:10])
        # print("start date " + str(year) + "-" + str(month)+ "-" + str(day))
        startDatetime = datetime.datetime(start_year, start_month, day)  # + datetime.timedelta(days - 1)
        end_year = int(end_date[0:4])
        end_month = int(end_date[5:7])
        day = int(end_date[8:10])
        # print("end date " + str(year) + "-" + str(month)+ "-" + str(day))
        endDatetime = datetime.datetime(end_year, end_month, day)  # + datetime.timedelta(days - 1)
        # dateStr = startTime.strftime("%Y%m%d")

        this_date = startDatetime
        year = this_date.year
        month = this_date.month

        #while this_date >= startDatetime and this_date <= endDatetime:
        while year <= end_year:

            # get listing of files for the month, since opendap files are organized by year/month

            month = 1
            stop_month = 12
            if year == start_year:
                month = this_date.month
            if year == end_year:
                stop_month = end_month

            while month <= stop_month:
                # create url to get listing month by month
                listing_url = url + '/' + '{:04d}/{:02d}/'.format(year, month)
                print(listing_url)
                # get listing of files
                listing = get_href(listing_url, "")
                for fname in listing:
                    if fname.endswith('.nc4.html'):
                        # extract date string to use in map
                        date_str = fname.split('/')[-1].split('.')[-4].split('-')[0]
                        #check within actual date range
                        y = int(date_str[0:4])
                        m = int(date_str[4:6])
                        d = int(date_str[6:8])
                        # print("start date " + str(year) + "-" + str(month)+ "-" + str(day))
                        dt = datetime.datetime(y, m, d)  # + datetime.timedelta(days - 1)
                        if dt < startDatetime or dt > endDatetime:
                            continue
                        # set up map for date to avoid duplicates
                        if date_str not in found_dates.keys():
                            files.append(fname.split('.html')[0])
                            found_dates[date_str] = True
                month = month + 1
                print(files)

            # increment month
            year = year + 1

        #dates = get_dates(url, start_year, end_year)
    except Exception as e:
        print("Error in url: "+url)
        print("cannot find filenames for "+start_year+" - "+ end_year)
        raise e

    return files

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
        path = mpltPath.Path(poly.xy)
        for i in range(lon.shape[0]):
            #flon=float(lon[i])
            #print("lon ", lon)
            if lon[i] < minlon or lon[i] > maxlon:
                continue
            #            print("i ",i)
            for j in range(lat.shape[0]):
                #flat = lat.array[i].data
                #print("lat [",j,"] ", lat[j])
                #print("flat ",flat)
                if lat[j] < minlat or lat[j] > maxlat:
                    continue
                #                print("j ",j)
                #                print("lat ", lat[i], " lon ", lon[j], " poly ", poly.get_label())
                inside = path.contains_point((lon[i], lat[j]))
                if inside:
                    #print("inside ")
                    # add precip value to district
                    if precip[0, i, j] >= 0.0:
                        districtPrecip[poly.get_label()].append(float(precip[0, i, j]))
                        #print("value ",precip[0, i, j])
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

def find_poly_box(district, minlat, minlon, maxlat, maxlon):
    shape = district['geometry']
    coords = district['geometry']['coordinates']
    #       name = district['properties']['name']
    dist_name = district['name']
    dist_id = district['id']

    def handle_subregion(subregion):
        #            poly = Polygon(subregion, edgecolor='k', linewidth=1., zorder=2, label=name)
        poly = Polygon(subregion, edgecolor='k', linewidth=1., zorder=2, label=dist_id)
        return poly

    distPoly = []
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
        print("Skipping ", dist_name, " because of unknown type ", shape["type"])

    return distPoly, minlat, minlon, maxlat, maxlon

def lambda_handler(event, context):
    #    product = 'GPM_3IMERGDE_06'
    # use "Late" product
    # product = 'GPM_3IMERGDL_06'
    # varName = 'HQprecipitation'

    test_count = 0
    outputJson = {'dataValues': []}

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
                print("download_aggregate_imerge failed, load_json_from_s3 could not load payload file " + key)
                sys.exit(1)
        else:
            bucket = data_bucket
            print("record ", record)
            input_json = record

        jsonData = input_json
        statType = 'mean'

        input_dataset = input_json["dataset"]
        request_id = input_json["request_id"]
        print("request_id ", request_id)

        if "stat_type" in jsonData:
            statType = jsonData['stat_type']
        product = jsonData['product']
        start_date = jsonData['start_date']
        end_date = jsonData['end_date']
        var_name = jsonData['var_name']
        data_element_id = jsonData['data_element_id']
        request_id = jsonData["request_id"]

        creation_time_in = input_json['creation_time']
        date_range_in = start_date.split('T')[0] + " -> " + end_date.split('T')[0]

        geometryJson = load_json_from_s3(s3.Bucket(bucket), "requests/geometry/" + request_id + "_geometry.json")
        if "message" in geometryJson and geometryJson["message"] == "error":
            update_status_on_s3(s3.Bucket(bucket), request_id, "aggregate", "failed",
                                "aggregate_imerge could not load geometry file " +
                                "requests/geometry/" + request_id + "_geometry.json",
                                creation_time=creation_time_in, date_range=date_range_in, dataset=input_dataset)
            sys.exit(1)
        districts = geometryJson["boundaries"]

        # eventually make into env variables
        auth_name = jsonData['auth_name']
        auth_pw = jsonData['auth_pw']
        base_url = 'https://gpm1.gesdisc.eosdis.nasa.gov/opendap/hyrax/GPM_L3/'

        listing_url = base_url + product

        # set up opendap urls using filenames from direct access site.  With opendap we can request only the variables
        # we need and we can get corresponding lat/lon as variables and we don't have to deal with sinusoidal projection
        update_status_on_s3(s3.Bucket(data_bucket), request_id,
                            "aggregate", "working", "retrieving filenames",
                            creation_time=creation_time_in, date_range=date_range_in, dataset=input_dataset)
        try:
            filenames = get_filenames(listing_url, start_date, end_date)
        except Exception as e:
            print("Network error: cannot get filename list")
            update_status_on_s3(s3.Bucket(data_bucket), request_id, "aggregate", "failed",
                                "OpenDap file list creation failed: ",
                                creation_time=creation_time_in, date_range=date_range_in, dataset=input_dataset)
            sys.exit(-1)

        update_status_on_s3(s3.Bucket(data_bucket), request_id,
                            "aggregate", "working", "Constructing OpenDAP URLs",
                            creation_time=creation_time_in, date_range=date_range_in, dataset=input_dataset)
        # opendap_urls = get_opendap_urls(var_name,x_start_stride_stop, y_start_stride_stop, filenames)

        # find the max/min lat, lons for the coordinates
        minlat = 90.0
        maxlat = -90.0
        minlon = 180.0
        maxlon = -180.0
        # find overall extent of combined districts, don't use distPoly yet
        for district in districts:
            distPoly, minlat, minlon, maxlat, maxlon = find_poly_box(district, minlat, minlon, maxlat, maxlon)
        # add a 0.2 degree border (~2 IMERG pixels) around image to
        # allow closer cropping by small regions.  There seems to be
        # some rounding errors at the borders that cut data out of
        # regions when the image is cropped tightly to the geojson
        # polygons.  This supports individual image orders for
        # districts and smaller areas
        if (minlon - 0.2) >= -180.0:
            minlon = minlon - 0.2
        if (maxlon + 0.2) <= 180.0:
            maxlon = maxlon + 0.2
        if (minlat - 0.2) >=-90.0:
            minlat = minlat - 0.2
        if (maxlat + 0.2) <= 90.0:
            maxlat = maxlat + 0.2

        # find individual box extents of polygons
        distPolyByName = {}
        minlatByName = {}
        maxlatByName = {}
        minlonByName = {}
        maxlonByName = {}
        for district in districts:
            dist_name = district['name']
            # find the max/min lat, lons for the coordinates in each district
            minlatdist = 90.0
            maxlatdist = -90.0
            minlondist = 180.0
            maxlondist = -180.0
            distPoly, minlatdist, minlondist, maxlatdist, maxlondist = find_poly_box(district, minlatdist, minlondist, maxlatdist, maxlondist)
            # use returned district polygon
            distPolyByName[dist_name] = distPoly
            # use returned lat/lon range box for this district
            minlatByName[dist_name] = minlatdist
            maxlatByName[dist_name] = maxlatdist
            minlonByName[dist_name] = minlondist
            maxlonByName[dist_name] = maxlondist

        # compute starting and ending lat/lon indices in dataset array
        start_lon_ind = int(10 * (minlon + 179.95))
        end_lon_ind = int(10 * (maxlon + 179.95))
        start_lat_ind = int(10 * (minlat + 89.95))
        end_lat_ind = int(10 * (maxlat + 89.95))

        numFiles = len(filenames)
        fileCnt = 1
        for file in filenames:
            update_status_on_s3(s3.Bucket(data_bucket), request_id,
                                "aggregate", "working", "Aggregating file " + str(fileCnt) + " of " + str(numFiles),
                                creation_time=creation_time_in, date_range=date_range_in, dataset=input_dataset)

            url = file
            session = setup_session(username=auth_name, password=auth_pw, check_url=url)
            dataset = open_url(url, session=session)
            # extract date from filename
            #https://gpm1.gesdisc.eosdis.nasa.gov/opendap/hyrax/GPM_L3/GPM_3IMERGDF.06/2021/09/3B-DAY.MS.MRG.3IMERG.20210901-S000000-E235959.V06.nc4
            dateStr = file.split('/')[-1].split('-')[1].split('.')[4]
            fileJson = []
            try:
                data_var = dataset[var_name][0, start_lon_ind:end_lon_ind, start_lat_ind:end_lat_ind].data  # time, lon, lat
                #print(data_var.shape)
                # nc = netCDF4.Dataset(url, **credentials)
                #print(data_var)
                lat = dataset['lat'][start_lat_ind:end_lat_ind].data
                #print(lat.shape)
                #print(lat)
                lon = dataset['lon'][start_lon_ind:end_lon_ind].data
                #print(lon.shape)
                #print(lon)
                success = True
                print("Successfully opened url ", url)
                session.close()

                # variables for precip values and stats by district
                districtPrecip = {}
                districtPrecipStats = {}
                districtPolygons = {}
                for district in districts:
                    shape = district['geometry']
                    coords = district['geometry']['coordinates']
                    #       name = district['properties']['name']
                    name = district['name']
                    dist_id = district['id']
                    # compute statisics
                    #        accumPrecipByDistrict(distPoly, precip, lat, lon, districtPrecip,minlat,minlon,maxlat,maxlon,im)
                    accumPrecipByDistrict(distPolyByName[name], data_var, lat, lon, districtPrecip, minlatByName[name], minlonByName[name], maxlatByName[name], maxlonByName[name])
                    districtPolygons[dist_id] = distPolyByName[name]

                calcDistrictStats(districtPrecip, districtPrecipStats)
            #    for district in districts:
            #        # name = district['properties']['name']
            #        dist_id = district['id']
            #        #name = district['name']
            #        print("district name ", name)
            #        print("district id", dist_id)
            #        print("mean precip ", districtPrecipStats[dist_id]['mean'])
            #        print("median precip ", districtPrecipStats[dist_id]['median'])
            #        print("max precip ", districtPrecipStats[dist_id]['max'])
            #        print("min precip ", districtPrecipStats[dist_id]['min'])
            #        print("count ", districtPrecipStats[dist_id]['count'])

            #    print("finished file " + key)

                # reformat new json structure
                #    outputJson = {'dataValues' : []}
                for key in districtPrecipStats.keys():
                    value = districtPrecipStats[key][statType]
                    jsonRecord = {'dataElement': data_element_id, 'period': dateStr, 'orgUnit': key, 'value': value}
                    fileJson.append(jsonRecord)
                    #print(jsonRecord)

            except Exception as e:
                print("Exception ", e)
                print("Network error opening url ", url)
            fileCnt = fileCnt + 1
            for record in fileJson:
                outputJson['dataValues'].append(record)

        with open("/tmp/" + request_id + "_result.json", 'w') as result_file:
            json.dump(outputJson, result_file)

        s3.Bucket(bucket).upload_file("/tmp/" + request_id + "_result.json", "results/" + request_id + ".json")

        update_status_on_s3(s3.Bucket(data_bucket), request_id, "aggregate", "success",
                            "All requested files successfully aggregated", creation_time=creation_time_in,
                            date_range=date_range_in, dataset=input_dataset)
        print(outputJson)

# if __name__ == '__main__':
#    main()
