FROM ubuntu:14.04

ADD updatedbatchsqs.py updatedbatchsqs.py

RUN apt-get update

RUN sudo apt-get install -y python 
RUN sudo apt-get install -y python-pip 
RUN sudo pip install boto3

CMD ["python", "updatedbatchsqs.py"]
