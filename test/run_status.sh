#!/bin/sh
curl --header "Content-Type: application/json" \
  --header "Accept: application/json" \
  -X POST -H "Origin: http://example.com" \
  -d @request.json \
  'https://n9uowbutv1.execute-api.us-east-1.amazonaws.com/default/get_result'
