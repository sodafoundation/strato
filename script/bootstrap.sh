#!/usr/bin/env bash
# Copyright 2019 The OpenSDS Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

OPT_DIR=/opt/opensds
mkdir -p $OPT_DIR

# Configuration
MINIMUM_GO_VERSION=${MINIMUM_GO_VERSION:-go1.11.1}
MINIMUM_DOCKER_VERSION=${MINIMUM_DOCKER_VERSION:-18.03.1-ce}
DEV_STACK_DIR=${DEV_STACK_DIR:-/opt/stack/devstack}
GOENV_PROFILE=${GOENV_PROFILE:-/etc/profile.d/goenv.sh}
MULTICLOUD_VERSION=${MULTICLOUD_VERSION:-v1}
MULTICLOUD_SERVER_NAME=${MULTICLOUD_SERVER_NAME:-multicloud}
MULTICLOUD_PASSWORD=${MULTICLOUD_PASSWORD:-opensds@123}
HOST_IP=${HOST_IP:-}

# Log file
LOG_DIR=/var/log/opensds
LOGFILE=${LOGFILE:-/var/log/opensds/multi-cloud-bootstrap.log}
mkdir -p $LOG_DIR

# Log function
log() {
    DATE=`date "+%Y-%m-%d %H:%M:%S"`
    USER=$(whoami)
    echo "${DATE} [INFO] $@"
    echo "${DATE} ${USER} execute $0 [INFO] $@" > $LOGFILE
}

log_error ()
{
    DATE=`date "+%Y-%m-%d %H:%M:%S"`
    USER=$(whoami)
    echo "${DATE} [ERROR] $@" 2>&1
    echo "${DATE} ${USER} execute $0 [ERROR] $@" > $LOGFILE
}

get_default_host_ip() {
    local host_ip=$1
    local af=$2
    # Search for an IP unless an explicit is set by ``HOST_IP`` environment variable
    if [ -z "$host_ip" ]; then
        host_ip=""
        # Find the interface used for the default route
        host_ip_iface=${host_ip_iface:-$(ip -f $af route | awk '/default/ {print $5}' | head -1)}
        local host_ips
        host_ips=$(LC_ALL=C ip -f $af addr show ${host_ip_iface} | sed /temporary/d |awk /$af'/ {split($2,parts,"/");  print parts[1]}')
        local ip
        for ip in $host_ips; do
            host_ip=$ip
            break;
        done
    fi
    echo $host_ip
}

test_host_port_free() {
  local port=$1
  local success=0
  local fail=1

  which lsof >/dev/null || {
    log_error "lsof isn't installed, can't verify if ${port} is free, skipping the check..."
    return ${success}
  }

  if ! lsof -i ":${port}" &>/dev/null; then
    return ${success}
  else
    log_error "port ${port} is already used"
    return ${fail}
  fi
}

# load profile
source /etc/profile
function install_golang() {
    if [[ -z "$(which go)" ]]; then
        log "Golang does not exist, installing ..."
        wget https://storage.googleapis.com/golang/${MINIMUM_GO_VERSION}.linux-amd64.tar.gz -O $OPT_DIR/${MINIMUM_GO_VERSION}.linux-amd64.tar.gz > /dev/null
        log "tar xzf $OPT_DIR/${MINIMUM_GO_VERSION}.linux-amd64.tar.gz -C /usr/local/"
        tar xzf $OPT_DIR/${MINIMUM_GO_VERSION}.linux-amd64.tar.gz -C /usr/local/
        echo 'export GOROOT=/usr/local/go' > $GOENV_PROFILE
        echo 'export GOPATH=$HOME/gopath' >> $GOENV_PROFILE
        echo 'export PATH=$PATH:$GOROOT/bin:$GOPATH/bin' >> $GOENV_PROFILE
        source $GOENV_PROFILE
    fi
    local go_version
    # verify go version
    IFS=" " read -ra go_version <<< "$(go version)"
    if [[ "${MINIMUM_GO_VERSION}" != $(echo -e "${MINIMUM_GO_VERSION}\n${go_version[2]}" | sort -s -t. -k 1,1 -k 2,2n -k 3,3n | head -n1) && "${go_version[2]}" != "devel" ]]; then
        log_error "Detected go version: ${go_version[*]}, Multi-Cloud requires ${MINIMUM_GO_VERSION} or greater."
        log_error "Please remove golang old version ${go_version[2]}, bootstrap will install ${MINIMUM_GO_VERSION} automatically"
        return 2
    fi
}

function install_docker() {
if [[ -z "$(which docker)" ]]; then
    log "Docker does not exist, installing ..."
    wget https://download.docker.com/linux/ubuntu/dists/xenial/pool/stable/amd64/docker-ce_18.03.1~ce-0~ubuntu_amd64.deb  -O $OPT_DIR/docker-ce_18.03.1~ce-0~ubuntu_amd64.deb
    sudo apt-get install -y libltdl7
    dpkg -i $OPT_DIR/docker-ce_18.03.1~ce-0~ubuntu_amd64.deb
fi
    local docker_version
    # verify go version
    IFS=" " read -ra docker_version <<< "$(docker --version)"
    if [[ "${MINIMUM_DOCKER_VERSION}" != $(echo -e "${MINIMUM_DOCKER_VERSION}\n${docker_version[2]}" | sort -s -t. -k 1,1 -k 2,2n -k 3,3n | head -n1) ]]; then
        log_error "Detected go version: ${docker_version[*]}, Multi-Cloud requires ${MINIMUM_DOCKER_VERSION} or greater."
        log_error "Please remove docker old version ${docker_version[2]} and re-execute this script, bootstrap will install ${MINIMUM_DOCKER_VERSION} automatically"
        return 2
    fi
}

function install_docker_compose() {
    if [[ -z "$(which docker-compose)" ]]; then
        log "Docker does not exist, installing ..."
        apt-get install -y docker-compose
    fi
}

function config_keystone() {
    . "$DEV_STACK_DIR/openrc" admin admin
    if openstack user show $MULTICLOUD_SERVER_NAME &>/dev/null; then
        return
    fi
    openstack user create --domain default --password "$MULTICLOUD_PASSWORD" "$MULTICLOUD_SERVER_NAME"
    openstack role add --project service --user "$MULTICLOUD_SERVER_NAME" admin
    # service group my be created before, so just ignore it the duplication error complaint.
    openstack group create service
    openstack group add user service "$MULTICLOUD_SERVER_NAME"
    openstack role add service --project service --group service
    openstack group add user admins admin
    openstack service create --name "multicloud$MULTICLOUD_VERSION" --description "Multi-cloud Block Storage" "multicloud$MULTICLOUD_VERSION"
    openstack endpoint create --region RegionOne "multicloud$MULTICLOUD_VERSION" public "http://$HOST_IP:8089/$MULTICLOUD_VERSION/%(tenant_id)s"
    openstack endpoint create --region RegionOne "multicloud$MULTICLOUD_VERSION" internal "http://$HOST_IP:8089/$MULTICLOUD_VERSION/%(tenant_id)s"
    openstack endpoint create --region RegionOne "multicloud$MULTICLOUD_VERSION" admin "http://$HOST_IP:8089/$MULTICLOUD_VERSION/%(tenant_id)s"
}

function install_multicloud() {
    source /etc/profile
    GOPATH=${GOPATH:-$HOME/gopath}
    local opensds_root=${GOPATH}/src/github.com/opensds
    local multicloud_root=${GOPATH}/src/github.com/opensds/multi-cloud
    mkdir -p ${opensds_root}

    cd ${opensds_root}
    if [ ! -d ${multicloud_root} ]; then
        log "Downloading the MultiCloud source code..."
        git clone https://github.com/opensds/multi-cloud.git
    fi

    if [[ -z "$(which make)" ]]; then
        log "Installing make ..."
        sudo apt-get install make -y
    fi

    cd ${multicloud_root}
    if [ ! -d ${multicloud_root}/build ]; then
        log "Building MultiCloud ..."
        make docker
    fi

    local compose_file=$multicloud_root/docker-compose.yml

    if [[ -d /opt/stack/devstack ]]; then
        config_keystone
        sed -i "s,OS_AUTH_AUTHSTRATEGY=.*$,OS_AUTH_AUTHSTRATEGY=keystone," $compose_file
        sed -i "s,OS_AUTH_URL=.*$,OS_AUTH_URL=http://$HOST_IP/identity," $compose_file
        sed -i "s,OS_USERNAME=.*$,OS_USERNAME=$MULTICLOUD_SERVER_NAME," $compose_file
        sed -i "s,OS_PASSWORD=.*$,OS_PASSWORD=$MULTICLOUD_PASSWORD," $compose_file
    else
        sed -i "s,OS_AUTH_AUTHSTRATEGY=.*$,OS_AUTH_AUTHSTRATEGY=noauth," $compose_file
    fi
    log "Starting MultiCloud service ..."
    cd $multicloud_root
    docker-compose up -d
    return $?
}

log Multi-Cloud bootstrap starting ...

HOST_IP=$(get_default_host_ip "$HOST_IP" "inet")
if [ "$HOST_IP" == "" ]; then
    log_error "Could not determine host ip address."
    exit 1
fi

# zookeeper: 2181
# kafka: 9092
# multi-cloud-api: 8089
# mongodb: 27017
for port in 2181 8089 9092 27017; do
    test_host_port_free $port || exit 1
done

for service in golang docker docker_compose multicloud; do
    if ! install_$service; then
        log_error "Install $service failed!"
        exit 1
    fi
done

log MultiCloud bootstrapped successfully.
