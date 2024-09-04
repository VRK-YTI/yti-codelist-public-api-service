#!/bin/bash

set -e
mvn clean package
docker build -f docker/Dockerfile -t yti-codelist-public-api-service .
