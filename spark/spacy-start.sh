#!/bin/bash

sudo yum update -y
sudo yum install -y gcc gcc-c++ make openssl-devel
sudo yum groupinstall -y 'Development Tools'
sudo yum install -y postgresql-libs postgresql-devel python-devel libevent-devel
sudo yum install -y python-pip
sudo pip install boto3
sudo pip install pillow
sudo pip install pandas
sudo pip install lxml
sudo pip install text_unidecode
sudo pip install nltk
sudo python -m nltk.downloader -d /usr/share/nltk_data wordnet punkt
sudo rm /usr/bin/cc /usr/bin/gcc /usr/bin/c++ /usr/bin/g++
sudo ln -s /usr/bin/gcc48 /usr/bin/cc
sudo ln -s /usr/bin/gcc48 /usr/bin/gcc
sudo ln -s /usr/bin/c++48 /usr/bin/c++
sudo ln -s /usr/bin/g++48 /usr/bin/g++
sudo pip install -U spacy
