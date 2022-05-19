import cgi
import os
import json
from csv import DictReader

import requests
from time import sleep

start_url = "https://9t06h5m4bf.execute-api.us-east-1.amazonaws.com/default/neoh_start_cloud_workflow"
download_url = "https://n9uowbutv1.execute-api.us-east-1.amazonaws.com/default/neoh_get_result"

#start_url = "https://9t06h5m4bf.execute-api.us-east-1.amazonaws.com/default/start_cloud_workflow"
#download_url = "https://n9uowbutv1.execute-api.us-east-1.amazonaws.com/default/get_result"

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
    #payload = "sample_payload_modis_temp.json"
    payload = "test_aws_temp.json"

    with open(payload) as f:
        jsonData = json.load(f)
    f.close()

    try:

        # Post json to the API task service, return response as json
        hdrs = {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'}
        print("Starting AWS cloud workflow...")
        print("dataset: ", jsonData['dataset'])
        print("product: ", jsonData['product'])
        print("var_name: ", jsonData['var_name'])

        # start workflow and return request_id
        task_response = post(start_url, jsonData, hdrs, 120.0)
        # print("task response", task_response.json())
        resp = task_response.json()

        if 'request_id' not in resp:
            print("error starting aws workflow: ", resp)
            exit(-1)

        request_id = resp['request_id']

        # check status in loop
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
            if count > 100:
                print("request timed out ")
                break;

        print(result)
            # process csv file and create output records, if csv stats file not found, data is missing
        print("AWS cloud workflow complete!")

        #resp = logout(token)
    except Exception as e:
        print("Exception: ",e)

if __name__ == '__main__':
   main()
