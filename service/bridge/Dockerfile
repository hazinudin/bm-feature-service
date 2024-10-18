FROM python:3.11-slim

# FROM ghcr.io/esri/arcgis-python-api-notebook:2.4.0-python3.11

WORKDIR /grpc_server

COPY . /grpc_server

EXPOSE 50051

USER root

RUN apt-get update -y \
    && apt-get upgrade -y \
    && apt-get install -y \
    libkrb5-dev \
    gcc

# RUN yum install -y krb5-libs krb5-workstation krb5-devel gcc

RUN pip install arcgis --no-deps

RUN pip install --no-cache-dir -r /grpc_server/requirements.txt

CMD ["python", "server.py"]
