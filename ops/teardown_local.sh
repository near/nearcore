#!/bin/bash

sudo docker stop $(sudo docker ps | grep near-testnet | awk '{print $(NF)}')
sudo docker stop near-studio
sudo docker rm $(sudo docker ps | grep near-testnet | awk '{print $(NF)}')
sudo docker rm near-studio

