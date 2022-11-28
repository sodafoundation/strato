#!/usr/bin/env bash

# Copyright 2022 The SODA Authors.
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

openssl version >& /dev/null
if [ $? -ne 0 ];then
    echo "Failed to run openssl. Please ensure openssl is installed."
    exit 1
fi

CUR_DIR=$(cd "$(dirname "$0")" || exit; pwd)

export OPENSSL_CONF="${CUR_DIR}"/openssl.cnf

COMPONENT=("opensds" "nbp")

OPENSDS_CERT_DIR=$1

if [ -z "${OPENSDS_CERT_DIR}" ];then
    OPENSDS_CERT_DIR="/opt/opensds-security"
fi

# Clean up installation context
if [ -d "${OPENSDS_CERT_DIR}" ];then
    rm -rf "${OPENSDS_CERT_DIR}"
fi

# Generate root ca cert
ROOT_CERT_DIR=${ROOT_CERT_DIR:-"${OPENSDS_CERT_DIR}"/ca}
mkdir -p "${ROOT_CERT_DIR}"
mkdir -p "${ROOT_CERT_DIR}"/demoCA/
mkdir -p "${ROOT_CERT_DIR}"/demoCA/newcerts
touch "${ROOT_CERT_DIR}"/demoCA/index.txt
echo "01" > "${ROOT_CERT_DIR}"/demoCA/serial
echo "unique_subject = no" > "${ROOT_CERT_DIR}"/demoCA/index.txt.attr

cd "${ROOT_CERT_DIR}"
openssl genrsa -passout pass:xxxxx -out "${ROOT_CERT_DIR}"/ca-key.pem -aes256 2048
openssl req -new -x509 -sha256 -key "${ROOT_CERT_DIR}"/ca-key.pem -out "${ROOT_CERT_DIR}"/ca-cert.pem -days 365 -subj "/CN=CA" -passin pass:xxxxx

# Generate component cert
for com in ${COMPONENT[*]};do
	openssl genrsa -aes256 -passout pass:xxxxx -out "${ROOT_CERT_DIR}"/"${com}"-key.pem 2048
	openssl req -new -sha256 -key "${ROOT_CERT_DIR}"/"${com}"-key.pem -out "${ROOT_CERT_DIR}"/"${com}"-csr.pem -days 365 -subj "/CN=${com}" -passin pass:xxxxx
	openssl ca -batch -in "${ROOT_CERT_DIR}"/"${com}"-csr.pem -cert "${ROOT_CERT_DIR}"/ca-cert.pem -keyfile "${ROOT_CERT_DIR}"/ca-key.pem -out "${ROOT_CERT_DIR}"/"${com}"-cert.pem -md sha256 -days 365 -passin pass:xxxxx
	
	# Cancel the password for the private key
    openssl rsa -in "${ROOT_CERT_DIR}"/"${com}"-key.pem -out "${ROOT_CERT_DIR}"/"${com}"-key.pem -passin pass:xxxxx

	mkdir -p "${OPENSDS_CERT_DIR}"/"${com}"
	mv "${ROOT_CERT_DIR}"/"${com}"-key.pem "${OPENSDS_CERT_DIR}"/"${com}"/
	mv "${ROOT_CERT_DIR}"/"${com}"-cert.pem "${OPENSDS_CERT_DIR}"/"${com}"/
	rm -rf "${ROOT_CERT_DIR}"/"${com}"-csr.pem
done

rm -rf "${ROOT_CERT_DIR}"/demoCA
