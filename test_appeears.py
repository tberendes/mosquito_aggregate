import cgi
import os
import json
from csv import DictReader

import requests
from time import sleep

appeears_url = "https://lpdaacsvc.cr.usgs.gov/appeears/api/"

auth = ('mosquito2019', 'Malafr#1')

# make token global
token = ''

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

    # can be read out of JSON file instead
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
    payload = "sample_payload_MOD11A2.json"
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

    try:
        # use earthdata login and get bearer token
        token = login()
    except Exception as e:
        print("Exception: ", e)

    try:

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
                # only download csv statistic files
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
# structure of CSV file
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

        # only do this if you are done with order, invalidates the token before expiration (48hrs)
        # can no longer check status or retrieve bundles
        resp = logout(token)
    except Exception as e:
        print("Exception: ", e)

if __name__ == '__main__':
   main()
