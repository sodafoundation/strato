#!/bin/bash

# Install dependencies
echo Installing dependencies
apt-get install -y curl wget libltdl7 libseccomp2 libffi-dev apt-transport-https ca-certificates gnupg gnupg-agent lsb-release software-properties-common sshpass pv gawk 

# Enable docker repository
echo Enabling docker repository
mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Update local repositories
echo Updating local repositories
apt-get update

# Install python dependencies
echo Installing Python dependencies
apt-get install -y python3-pip

# Install ansible if not present
if [ "`which ansible`" != "" ]; then
    echo ansible already installed, skipping.
else
    echo Installing ansible
    python3 -m pip install ansible
fi

# Install docker if not present
if [ "`which docker`" != "" ]; then
    echo Docker already installed, skipping.
else
    echo Installing docker
    apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
fi

# Install Go if not present
if [ "`which go`" != "" ]; then
    IFS=' '
    v=`go version | { read _ _ v _; echo ${v#go}; }`
    IFS='.'
    read -ra v <<< "$v"
    if (( ${v[0]} == 1 && ${v[1]} >= 17 )); then
        echo Go 1.17+ already installed, skipping.
    else
        echo Removing existing Go installation
        rm -rf /usr/local/go
        echo Installing Go 1.17.9
        wget https://storage.googleapis.com/golang/go1.17.9.linux-amd64.tar.gz
        tar -C /usr/local -xzf go1.17.9.linux-amd64.tar.gz
    fi
    unset IFS v
else
    echo Installing Go 1.17.9
    wget https://storage.googleapis.com/golang/go1.17.9.linux-amd64.tar.gz
    tar -C /usr/local -xzf go1.17.9.linux-amd64.tar.gz
fi

# Ensure /usr/local/bin is in path
export PATH=$PATH:/usr/local/bin

# Ensure usr/local/go/bin is in path, create GOPATH and source it
echo 'export PATH=$PATH:/usr/local/go/bin' >> /etc/profile
echo 'export GOPATH=$HOME/gopath' >> /etc/profile
source /etc/profile
