#!/bin/sh
curl --header "Content-Type: application/json" \
  --header "Accept: application/json" \
  -X POST -H "Origin: http://example.com" \
  -d @sample_payload.json \
  'https://9t06h5m4bf.execute-api.us-east-1.amazonaws.com/default/start_cloud_workflow' > request.json
./run_status.sh

