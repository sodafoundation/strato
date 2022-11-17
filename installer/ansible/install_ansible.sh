#!/bin/bash

# Copyright 2018 The OpenSDS Authors.
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

# This step is needed to upgrade ansible to version 2.4.x which is required for
# the ceph backend installation using ceph-ansible.

REQUIRED_ANSIBLE_VER=${REQUIRED_ANSIBLE_VER:-}

SCRIPT_PATH=$(cd $(dirname $0); pwd)

if [ "`which ansible`" != ""  ]; then
# ansible installed
    CURRENT_VER=`ansible --version | egrep ^ansible | awk '{print $2;}' | cut -b -3`
    if [ "${CURRENT_VER}" != "${REQUIRED_ANSIBLE_VER}" ]; then
        echo ansible version error
        echo removing ansible
        sudo apt-get remove -y ansible
        sudo apt-get purge -y ansible
    else
        # correct version installed. bailout
        ansible --version
        exit 0
    fi
fi

# incorrect version removed, or no ansible found.
echo Installing ansible ${REQUIRED_ANSIBLE_VER} required for ceph-ansible ${CEPH_ANSIBLE_BRANCH} branch.
sudo add-apt-repository -y ppa:ansible/ansible-${REQUIRED_ANSIBLE_VER}
echo "Update the repository list"
sudo apt-get update
echo "Installing required ansible version"
sudo apt-get install -y ansible
sleep 3
echo "Removing the ansible apt repository"
sudo add-apt-repository -y -r ppa:ansible/ansible-${REQUIRED_ANSIBLE_VER}

ansible --version
