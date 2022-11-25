# Multicloud Installation Guide

## Ansible Installer
* Supported OS: **Ubuntu 18.04, 20.04**
* Prerequisite: **Python 3.6 or above** should be installed

**Note**: Ensure root user access while performing the installation.

### Install required packages
```bash
apt-get update && apt-get install -y git curl wget libltdl7 libseccomp2 libffi-dev
```

### Clone the Multicloud repository
```bash
git clone https://github.com/sodafoundation/multi-cloud.git
```
### Install required dependencies for Multicloud (Docker, Docker-Compose, Ansible, Go)
```bash
cd multi-cloud/installer
chmod +x install_dependencies.sh && . install_dependencies.sh
```
### Set Host IP
```bash
cd ansible
export HOST_IP=<your_host_ip>
```
### Run the Ansible Playbook to install Multicloud
```bash
ansible-playbook site.yml -i local.hosts
```

### Uninstall and Purge
```bash
ansible-playbook clean.yml -i local.hosts
```
