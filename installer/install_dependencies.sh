#!/bin/bash

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

# Install dependencies
echo Installing dependencies
apt-get install -y apt-transport-https ca-certificates gnupg gnupg-agent lsb-release software-properties-common sshpass pv gawk

# Install python dependencies
echo Installing Python dependencies
apt-get install -y python3-pip

# Install ansible if not present
if [ "`which ansible`" != ""  ]; then
    echo ansible already installed, skipping.
else
    echo Installing ansible
    python3 -m pip install ansible
fi

# Install docker if not present
if [ "`which docker`" != ""  ]; then
    echo Docker already installed, skipping.
else
    echo Installing docker
    apt-get install -y docker-ce docker-ce-cli containerd.io
fi

# Install docker-compose if not present
if [ "`which docker-compose`" != ""  ]; then
    echo docker-compose already installed, skipping.
else
    echo Installing docker-compose
    curl -L "https://github.com/docker/compose/releases/download/1.25.5/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
    chmod +x /usr/local/bin/docker-compose
    ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose
fi

# Install Go if not present
if [ "`which go`" != ""  ]; then
    echo Go already installed, skipping.
else
    echo Installing Go
    wget https://storage.googleapis.com/golang/go1.17.9.linux-amd64.tar.gz
    tar -C /usr/local -xzf go1.17.9.linux-amd64.tar.gz
fi

# Ensure /usr/local/bin is in path
export PATH=$PATH:/usr/local/bin

# Ensure usr/local/go/bin is in path, create GOPATH and source it
echo 'export PATH=$PATH:/usr/local/go/bin' >> /etc/profile
echo 'export GOPATH=$HOME/gopath' >> /etc/profile
source /etc/profile
