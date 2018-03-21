#!/bin/bash
~/.local/bin/aws s3 sync mallet/  s3://cypher-models/test/ --region=us-east-1 --sse aws:kms