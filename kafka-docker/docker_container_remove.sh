#!/bin/bash

sudo rm -rf ./visualization/prometheus/volume
sudo rm -rf ./visualization/prometheus/grafana

mkdir ./visualization/prometheus/volume
mkdir ./visualization/grafana

sudo chmod -R 777 ./visualization/prometheus/config
sudo chmod -R 777 ./visualization/prometheus/volume
sudo chmod -R 777 ./visualization/grafana

sudo rm -rf ./kafkalogging
sudo docker rm -f $(sudo docker ps -aq)


sudo docker compose -f kafka-compose.yml up --build