#!/bin/bash
~/.local/bin/aws s3 sync sprak/  s3://cypher-models/spark/ --region=us-east-1 --sse aws:kms
