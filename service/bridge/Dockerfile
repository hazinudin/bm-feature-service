FROM python:3.11-slim

WORKDIR /grpc_server

COPY . /grpc_server

EXPOSE 50051

USER root

RUN apt-get update -y \
    && apt-get upgrade -y \
    && apt-get install -y \
    libkrb5-dev \
    gcc

RUN pip install arcgis --no-deps

RUN pip install --no-cache-dir -r /grpc_server/requirements.txt

CMD ["python", "server.py"]
