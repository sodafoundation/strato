# Multi-cloud Installation Guide

## Ansible Installer
* Supported OS: **Ubuntu 18.04, 20.04**
* Prerequisite: **Python 3.6+** and **Go 1.17+**  should be installed

**Note**: Ensure root user access while performing the installation.

### Install Steps
```bash
apt-get update && apt-get install -y git curl wget libltdl7 libseccomp2 libffi-dev
git clone https://github.com/sodafoundation/multi-cloud.git
cd multi-cloud/installer
chmod +x install_dependencies.sh && . install_dependencies.sh
```

### Set Host IP
```bash
cd ansible
export HOST_IP=<your_host_ip>
```

### Run the Ansible Playbook to install Multi-cloud
```bash
ansible-playbook site.yml -i local.hosts
```

### Uninstall and Purge
```bash
ansible-playbook clean.yml -i local.hosts
```
