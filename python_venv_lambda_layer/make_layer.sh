#!/bin/bash
#mkdir folder
#cd folder
virtualenv v-env
source ./v-env/bin/activate
#pip install pandas
pip3 install -r requirements.txt
deactivate

#    Then type the following code line by line to create your layer
#
mkdir python
cd python
cp -r ../v-env/lib/python3.8/site-packages/* .
cd ..
zip -r neoh_layer.zip python
# can just upload zip file to layer in AWS web interface, or use CLI as follows:
#aws lambda publish-layer-version --layer-name pandas --zip-file fileb://neoh_layer.zip --compatible-runtimes python3.8
