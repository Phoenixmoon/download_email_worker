#!/bin/bash

echo "Building docker image"
docker build -t 851633384945.dkr.ecr.us-east-1.amazonaws.com/email_rag --platform=linux/amd64 -f docker/Dockerfile .

# must be run from parent (root) folder