import string
import json
import random
import boto3 as boto3

data_bucket = "mosquito-data"

s3 = boto3.resource(
    's3')
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

def lambda_handler(event, context):

    print("event ", event)

    if 'body' in event:
        event = json.loads(event['body'])
    else:
        return dict(statusCode='200', headers={'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'},
        body=json.dumps({'message': "missing json parameters"}), isBase64Encoded='false')

    #    "dataset": "precipitation", "org_unit": "district", "agg_period": "daily", "start_date": "1998-08-21T17:38:27Z",
#    "end_date": "1998-09-21T17:38:27Z", "data_element_id": "fsdfrw345dsd"
    dataset = event['dataset']
    org_unit = event['org_unit']
    period = event['agg_period']
    start_date = event['start_date']
    end_date = event['end_date']
    data_element_id = event['data_element_id']
    boundaries = event['boundaries']
    request_id = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(10))

    # added for new json format
    districts = boundaries

    # find the max/min lat, lons for the coordinates
    minlat = 90.0
    maxlat = -90.0
    minlon = 180.0
    maxlon = -180.0
    for district in districts:
        shape = district['geometry']
        coords = district['geometry']['coordinates']
        #       name = district['properties']['name']
        dist_name = district['name']
        dist_id = district['id']

        if shape["type"] == "Polygon":
            for subregion in coords:
                for coord in subregion:
                    minlat, minlon, maxlat, maxlon = find_maxmin_latlon(coord[1],coord[0],minlat,minlon,maxlat,maxlon)
        elif shape["type"] == "MultiPolygon":
            for subregion in coords:
                #            print("subregion")
                for sub1 in subregion:
                    #                print("sub-subregion")
                    for coord in sub1:
                        minlat, minlon, maxlat, maxlon = find_maxmin_latlon(coord[1], coord[0], minlat, minlon,
                                                                                maxlat, maxlon)
        else:
            print
            "Skipping", dist_name, \
            "because of unknown type", shape["type"]


    # format new json structure
    downloadJson = {"dataset": dataset, "org_unit": org_unit, "agg_period": period, "start_date": start_date,
        "end_date": end_date, "data_element_id": data_element_id, "request_id": request_id,
        "min_lat": minlat, "max_lat": maxlat, "min_lon": minlon, "max_lon": maxlon}

    download_param_pathname = ""
    if dataset.lower() == 'precipitation':
        download_param_pathname="requests/download/"+dataset+ "/"
        #set up download_imerg data
    else:
        return dict(statusCode='200', headers={'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'},
                    body=json.dumps({'message': "illegal dataset: " + dataset}), isBase64Encoded='false')

    with open("/tmp/" +request_id+".json", 'w') as json_file:
        json.dump(downloadJson, json_file)
    #        json.dump(districtPrecipStats, json_file)
    json_file.close()

    s3.Bucket(data_bucket).upload_file("/tmp/" + request_id+".json", download_param_pathname +request_id+".json")

    # write out boundaries json file
    # format new json structure
    geometryJson = {"request_id": request_id, "boundaries": districts}

    geometry_pathname="requests/geometry/"

    with open("/tmp/" +request_id+"_geometry.json", 'w') as geometry_file:
        json.dump(geometryJson, geometry_file)
    #        json.dump(districtPrecipStats, json_file)
    geometry_file.close()

    s3.Bucket(data_bucket).upload_file("/tmp/" + request_id+"_geometry.json",
                                       geometry_pathname +request_id+"_geometry.json")

     # set a random jobID string for use in all subsequent processes
#    return dict(statusCode='200', headers={'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
#                body=json.dumps({'files': download_results}), isBase64Encoded='false')
    return dict(statusCode='200', headers={'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*',
                                           'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'},
                body=json.dumps({'request_id': request_id}), isBase64Encoded='false')


#return dict(statusCode='200', body={'files': download_results}, isBase64Encoded='false')
#    return dict(body={'files': download_results}, isBase64Encoded='false')

    # return {
    #     'files': download_results
    # }
