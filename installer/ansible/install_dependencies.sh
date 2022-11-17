#!/bin/bash

echo Enabling universe repository
sudo add-apt-repository universe

echo Enabling docker repository
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"

# Update local repositories
echo Updating local repositories
sudo apt-get update

# Install ansible dependencies
echo Installing dependencies
sudo apt-get install -y apt-transport-https ca-certificates curl gnupg-agent software-properties-common sshpass pv gawk

# Install docker if not present
if hash docker 2>/dev/null; then
    echo Docker already installed, skipping.
else
    echo Installing docker
    sudo apt-get install -y docker-ce docker-ce-cli containerd.io
fi

# Install docker compose
echo Installing docker-compose
sudo curl -L "https://github.com/docker/compose/releases/download/1.25.5/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
sudo ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose

# Ensure /usr/local/bin is in path
PATH=$PATH:~/usr/local/bin
