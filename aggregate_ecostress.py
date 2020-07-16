import os
import sys
import statistics
import json
from urllib.parse import unquote_plus, urlparse, urljoin
import datetime

import rasterio
import numpy as np
from affine import Affine
from pyproj import Proj, transform
import gdal
from osgeo import gdal, gdal_array

from matplotlib.patches import Polygon
import matplotlib.path as mpltPath
import numpy

def accumVariableByDistrict(polylist, variable, lat, lon, districtVariable,
                            minlat, minlon, maxlat, maxlon, valid_min, valid_max):

    for poly in polylist:
        if poly.get_label() not in districtVariable.keys():
            districtVariable[poly.get_label()] = []

    for i in range(lat.shape[0]):
        for j in range(lat.shape[1]):
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
                    # values of zero or below are missing, cloud contamination in 8day composite, do not use
                    # else:
                    #     districtVariable[poly.get_label()].append(0.0)
                    break # only allow membership in one polygon, doesn't allow for overlapping regions

#                    im.putpixel((i,height-1-j),(r, g, b))
#                    print("lat ", lat[j], " lon ", lon[i], " variable ", variable[i][j], " inside ", poly.get_label())
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
            mean = -9999.0
            median = -9999.0
            maxval = -9999.0
            minval = -9999.0
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

def process_file(filename, districts, dataElement, statType, var_name):
    print('filename ' + filename)
#    with gzip.open(filename) as gz:
#        with NetCDFFile('dummy', mode='r', memory=gz.read()) as nc:
   # dictionaries for computing stats by district
    districtVariable = {}
    #districtVariableStats = {}
    #districtPolygons = {}

    dateStr = ""

    # Open tif file
    ds = gdal.Open(filename)
    raster = ds.GetRasterBand(1)

    valid_min=1
    valid_max=350
    # valid_min=raster.GetMinimum()
    # valid_max=raster.GetMaximum()
    print("valid_min ", valid_min)
    print("valid_max ", valid_max)

    # GDAL affine transform parameters, According to gdal documentation xoff/yoff are image left corner, a/e are pixel wight/height and b/d is rotation and is zero if image is north up.
    xoff, a, b, yoff, d, e = ds.GetGeoTransform()

    def pixel2coord(x, y):
        """Returns global coordinates from pixel x, y coords"""
        xp = a * x + b * y + xoff
        yp = d * x + e * y + yoff
        return (xp, yp)

    # get columns and rows of your image from gdalinfo
    cols = ds.RasterXSize
    rows = ds.RasterYSize

    lon = numpy.zeros(shape=(rows,cols))
    lat = numpy.zeros(shape=(rows,cols))

    variable = ds.ReadAsArray(0, 0, cols, rows).astype(numpy.float)
    for row in range(0, rows):
        for col in range(0, cols):
            lon[row][col], lat[row][col] = pixel2coord(col, row)

    # # Read raster
    # with rasterio.open(filename) as r:
    #     T0 = r.transform  # upper-left pixel corner affine transform
    #     p1 = Proj(r.crs)
    #     variable = r.read()  # pixel values
    #
    # # All rows and columns
    # cols, rows = np.meshgrid(np.arange(variable.shape[2]), np.arange(variable.shape[1]))
    #
    # # Get affine transform for pixel centres
    # T1 = T0 * Affine.translation(0.5, 0.5)
    # # Function to convert pixel row/column index (from 0) to easting/northing at centre
    # rc2en = lambda r, c: (c, r) * T1
    #
    # # All eastings and northings (there is probably a faster way to do this)
    # eastings, northings = np.vectorize(rc2en, otypes=[np.float, np.float])(rows, cols)
    #
    # # Project all longitudes, latitudes
    # p2 = Proj(proj='latlong', datum='WGS84')
    # lon, lat = transform(p1, p2, eastings, northings)


#   lat = nc.variables['Latitude'][:]
#    lon = nc.variables['Longitude'][:]

    print("lat ", lat[0][0], "lon", lon[0][0])
    print("lat.shape[0]", lat.shape[0])
    print("lat.shape[1]", lat.shape[1])

    # filename format:  ECOSTRESS_L2_LSTE_09009_009_20200206T214458_0601_01_LST_GEO.tif
    # parse out date/time from filename
    # strip out yyyyddd from opendap url

    # tempStr = os.path.basename(opendapUrl).split('.')[1]
    # year = int(tempStr[1:5])
    # days = int(tempStr[5:8])
    # print("year "+str(year)+ " days "+str(days))
    # startTime = datetime.datetime(year, 1, 1) + datetime.timedelta(days - 1)
    # dateStr = startTime.strftime("%Y%m%d")

#    im = PIL.Image.new(mode="RGB", size=(lon.shape[0], lat.shape[0]), color=(255, 255, 255))

    for district in districts:
        shape = district['geometry']
        coords = district['geometry']['coordinates']
 #       name = district['properties']['name']
        name = district['name']
        dist_id = district['id']

        print("district: " + name)
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
        accumVariableByDistrict(distPoly, variable, lat, lon,
                                districtVariable,minlat,minlon,maxlat,maxlon,
                                valid_min, valid_max)
        #districtPolygons[dist_id] = distPol

    # reformat new json structure
#    outputJson = {'dataValues' : []}
    districtVariableStats = calcDistrictStats(districtVariable)
    for district in districts:
       # name = district['properties']['name']
        dist_id = district['id']
        name = district['name']
        print("district name ", name)
        print("district id", dist_id)
        print("mean Variable ", districtVariableStats[dist_id]['mean'])
        print("median Variable ", districtVariableStats[dist_id]['median'])
        print("max Variable ", districtVariableStats[dist_id]['max'])
        print("min Variable ", districtVariableStats[dist_id]['min'])
        print("count ", districtVariableStats[dist_id]['count'])
    outputJson = []
    for key in districtVariableStats.keys():
        value = districtVariableStats[key][statType]
        jsonRecord = {'dataElement':dataElement,'period':dateStr,'orgUnit':key,'value':value}
        outputJson.append(jsonRecord)

    return outputJson

def main():

    ECO_DATA_DIR = '/media/sf_berendes/ecostress/data'
    OUT_DIR = '/media/sf_berendes/ecostress/upload'
    CONFIG_FILE = '/media/sf_berendes/ecostress/config/ecostress_geo_config.json'

    input_json = {"message": "error"}
    try:
        with open(CONFIG_FILE) as f:
            input_json = json.load(f)
        f.close()
    except IOError:
        print("Could not read file:" + CONFIG_FILE)
        sys.exit(1)

    outputJson = {'dataValues' : []}

    for root, dirs, files in os.walk(ECO_DATA_DIR, topdown=False):
        for file in files:
            print('file ' + file)
            # only process zipped nc VN files
            if file.endswith('.tif'):
                fileJson = process_file(os.path.join(root, file), input_json['boundaries'], input_json['data_element_id'],
                                        input_json['stat_type'],input_json['var_name'])
            # construct output filename based on date and variable
            for record in fileJson:
                outputJson['dataValues'].append(record)

    with open(OUT_DIR+ "/" + file.split('.')[0]+'.json', 'w') as result_file:
        json.dump(outputJson, result_file)
    result_file.close()

if __name__ == '__main__':
   main()
