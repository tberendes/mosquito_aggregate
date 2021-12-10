import cgi
import os
import json
import sys
from csv import DictReader

import requests
from time import sleep

start_url = "https://9t06h5m4bf.execute-api.us-east-1.amazonaws.com/default/start_cloud_workflow"
download_url = "https://n9uowbutv1.execute-api.us-east-1.amazonaws.com/default/get_result"

def post(url, json_payload, hdrs, timeout):
    task_response=requests.post(url, json=json_payload, headers=hdrs, timeout=timeout)
    task_response.raise_for_status()
    return task_response

def get(url, hdrs, timeout):
    task_response = requests.get(url, headers=hdrs, timeout=timeout)
    task_response.raise_for_status()
    return task_response


def main():

    #payload = "sample_payload_imerg_precip.json"
    #payload = "sample_payload_modis_ndvi.json"
    geojson_boundaries = "zimbabwe_shapes.json"

    # main parameters
    start_date = "2019-08-01T00:00:00.000Z"
    end_date =   "2019-08-02T00:00:00.000Z"
    data_type = "precip" # "temp", "ndvi"
    dist_version_string = "zimbabwe_1"

    sdate=start_date.split("T")[0].replace("-","")
    edate=end_date.split("T")[0].replace("-","")
    outfile=dist_version_string+'_'+data_type+'_'+sdate+'_'+edate+'.csv'

    precip_template = {"dataset": "precipitation", "org_unit": "district", "stat_type": "mean", "product": "GPM_3IMERGDF_06",
     "var_name": "precipitationCal", "agg_period": "daily", "start_date": start_date,
     "end_date": end_date, "auth_name": "mosquito2019", "auth_pw": "Malafr#1",
     "data_element_id": "9999", "boundaries": []}

    # NDVI
    ndvi_template = {"dataset":"vegetation","org_unit":"district","stat_type":"mean","product": "MOD13A2",
                      "var_name":"_1_km_16_days_NDVI","x_start_stride_stop": "[0:5:1199]","y_start_stride_stop": "[0:5:1199]",
                      "agg_period":"daily","start_date":start_date,"end_date":end_date,
                      "dhis_dist_version":"zimbabwe","data_element_id":"9999","boundaries":[]}
    # temperature
    temperature_template = {"dataset":"temperature","org_unit":"district","stat_type":"mean","product": "MOD11B2",
                            "var_name":"LST_Day_6km","agg_period":"daily","start_date":start_date,
                            "end_date":end_date,"dhis_dist_version":"zimbabwe","data_element_id":"9999999",
                            "boundaries":[]}
    # boundary = {"name": "Bok'e", "id": "9999", "geometry": {}}
    # select which product to use and set up template
    if data_type.lower() == 'precip':
        jsonData=precip_template
    elif data_type.lower() == 'temp':
        jsonData=temperature_template
    elif data_type.lower() == 'ndvi':
        jsonData=ndvi_template
    else:
        print("error: unknown data type "+data_type)
        exit(-1)

    # process feature collection
    #{"type": "FeatureCollection", "features": [
    #    {"type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[29
    #    .939771739000037,-21.32280515499997],[29.939773738000156,-21.321672072999945],[29.939775736000115,-21.319974948999914],[29 ...
    #    ]]]},
    #"properties": {"Shape_Leng": 3.06924302977, "Shape_Area": 0.214056576251, "province": "Midlands",
    #               "district": "Zvishavane", "2017cases": 83}}
    #]}

    # load geojson file
    with open(geojson_boundaries) as f:
        geojsonData = json.load(f)

    # set up all boundaries in geojson config file
    config = jsonData
    config['dhis_dist_version'] = dist_version_string
    for feature in geojsonData['features']:
        geometry = feature['geometry']
        properties = feature['properties']
        district = properties['district']
        province = properties['province']
        boundary = {"level":2,"name":province+'-'+district,"id":province+'-'+district, "geometry":geometry}
        config['boundaries'].append(boundary)

    #with open(geojson_boundaries+".sav", 'w') as f:
    #    json.dump(jsonData, f)
    #sys.exit()

    # submit job
    try:
        # Post json to the API task service, return response as json
        #hdrs = {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*', 'Content-Encoding': 'gzip'}
        hdrs = {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'}
        print("Starting AWS cloud workflow...")
        print("dataset: ", jsonData['dataset'])
        print("product: ", jsonData['product'])
        print("var_name: ", jsonData['var_name'])

        # start workflow and return request_id
        task_response = post(start_url, config, hdrs, 120.0)
        # print("task response", task_response.json())
        resp = task_response.json()

        if 'request_id' not in resp:
            print("error starting aws workflow: ", resp)
            exit(-1)

        request_id = resp['request_id']
        print('submitted request id '+resp['request_id'])

    except Exception as e:
        print("Exception: ", e)

    if os.path.isfile(outfile):
        os.remove(outfile)
    with open(outfile, 'w') as f:
        f.write('date,province,district,'+datatype+'\n')


    # check job status
    print('checking request_id ' + request_id)
    try:
        # check status in loop, loop until job is finished
        count = 0
        while True:
            task_response = post(download_url, {'request_id':request_id}, hdrs, 120.0)
            # print("task response", task_response.json())
            statusJson = task_response.json()
            if 'error' in statusJson:
                print("error checking AWS order status: ", statusJson['error'])
                exit(-1)
            print(statusJson)

            if statusJson["status"] == "failed":
                print("AWS order failed: ", statusJson)
                exit(-1)
            elif statusJson["status"] == "success":
                result = statusJson["result"]
                break
            else:
                print("order status: ", statusJson["status"], ' Message: ',statusJson["message"])
                sleep(5)
            count = count+1
            print("count ", count)
            if count > 180: # 15 minute maximum for AWS lambda
                print("request timed out ")
                exit(-1)
        print("request "+request_id+" finished")

        print(result)
        with open(outfile, 'a') as f:
            for datavalue in result["dataValues"]:
                #process data value record
                f.write(datavalue['period']+','+datavalue['orgUnit'].replace('-',',')+','+str(datavalue['value'])+'\n')
           #json.dump(result, f)

        # process csv file and create output records, if csv stats file not found, data is missing

        #resp = logout(token)
    except Exception as e:
        print("Exception: ",e)
    print("AWS cloud workflow complete!")

if __name__ == '__main__':
   main()
