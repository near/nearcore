#!/bin/bash

sudo docker stop $(sudo docker ps -q)
sudo docker rm $(sudo docker ps -q --all)
