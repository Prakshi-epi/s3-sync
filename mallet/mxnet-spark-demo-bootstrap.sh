#!/bin/bash

sudo yum update -y
sudo yum install -y python-pip
sudo pip install boto3
sudo pip install pillow
sudo pip install pandas
sudo pip install -U spacy