#!/bin/bash

sudo docker stop $(sudo docker ps | grep near-testnet | awk '{print $15}')
sudo docker stop near-studio
