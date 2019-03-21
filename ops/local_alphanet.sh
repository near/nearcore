#!/bin/bash

docker run -d --name alphanet1 -p 3000:3000 -e "BOOT_NODE_IP=127.0.0.1" -e "NODE_NUM=0" -e "TOTAL_NODES=2" alphanet
docker run -d --name alphanet2 --add-host=alphanet1:172.17.0.2 -e "BOOT_NODE_IP=172.17.0.2" -e "NODE_NUM=1" -e "TOTAL_NODES=2" alphanet

