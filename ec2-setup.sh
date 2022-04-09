#!/bin/bash

# enable ssh root
# see: https://stackoverflow.com/questions/7407333/amazon-ec2-root-login

# allocate 2GB swap
fallocate -l 2G /swapfile && chmod 600 /swapfile && mkswap /swapfile && swapon /swapfile
echo "/swapfile swap swap defaults 0 0" >> /etc/fstab
swapon --show && free -h

# OS updates
apt update && \
apt -y upgrade && \
apt autoremove
