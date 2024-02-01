# Multi-cloud Installation Guide

## Ansible Installer
* Supported OS: **Ubuntu 18.04, 20.04**.
* Prerequisite: **Python 3.6+** and **Go 1.17+**  should be installed.

**Note**: Ensure root user access while performing the installation.

### Install Steps
```bash
apt-get update && apt-get install -y git
git clone https://github.com/sodafoundation/multi-cloud.git
cd multi-cloud/installer
chmod +x install_dependencies.sh && . install_dependencies.sh
```

### Set Host IP
```bash
cd ansible
export HOST_IP=<your_host_ip> #note: make sure HOST_IP is either public ip or 192.x.x.x series ip
```

### Run the Ansible Playbook to install Multi-cloud
```bash
ansible-playbook site.yml -i local.hosts
```
Ensure that the playbook finishes the installation without any failed steps. 

In case of any failure, run the following command to clear the installation:
```bash
ansible-playbook clean.yml -i local.hosts
```
Now, retry again:
```bash
ansible-playbook site.yml -i local.hosts
```

### Verify installation of Multi-cloud
Run the below command to list the containers running:
```bash
docker ps
```
![Screenshot from 2023-04-13 10-34-50](https://user-images.githubusercontent.com/45416272/231659142-f8f1153b-c77a-4874-82c5-7d97510a13c0.png)

Ensure that all the above mentioned containers are up and running successfully. This confirms that multi-cloud has been installed successfully.

### Uninstall and Purge
```bash
ansible-playbook clean.yml -i local.hosts
```
### Demo video of Installation steps
Demo [video](https://www.youtube.com/watch?v=Jhmx3aiZuU0).

<img width="1242" alt="strato-installation-guide" src="https://github.com/sodafoundation/strato/assets/41313813/b400b8e9-07c7-45b8-8123-bdf72edc3d4c">

### Using Multi-cloud with dashboard
* Install multi-cloud with dashboard by following the installation guide [here](https://github.com/sodafoundation/installer/blob/master/README.md).
* For experiencing the features of multi-cloud, follow the user guide [here](https://docs.sodafoundation.io/guides/user-guides/multi-cloud/).

If you are interested in developing, follow the developer guide [here](https://docs.sodafoundation.io/guides/developer-guides/multi-cloud/).
