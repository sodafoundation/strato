#!/bin/bash

# Copyright 2017 The OpenSDS Authors.
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

# Echo text to the log file, summary log file and stdout
# echo_summary "something to say"
function echo_summary {
    echo -e "$@"
}

wait_for_url() {
  local url=$1
  local prefix=${2:-}
  local wait=${3:-1}
  local times=${4:-30}

  which curl >/dev/null || {
    echo_summary "curl must be installed"
    exit 1
  }

  local i
  for i in $(seq 1 "$times"); do
    local out
    if out=$(curl --max-time 1 -gkfs "$url" 2>/dev/null); then
      echo_summary "On try ${i}, ${prefix}: ${out}"
      return 0
    fi
    sleep "${wait}"
  done
  echo_summary "Timed out waiting for ${prefix} to answer at ${url}; tried ${times} waiting ${wait} between each"
  return 1
}

# Prints line number and "message" in error format
# err $LINENO "message"
err() {
    local exitcode=$?
    local xtrace
    xtrace=$(set +o | grep xtrace)
    set +o xtrace
    local msg="[ERROR] ${BASH_SOURCE[2]}:$1 $2"
    echo "$msg"
    $xtrace
    return $exitcode
}

# Prints line number and "message" then exits
# die $LINENO "message"
die() {
    local exitcode=$?
    set +o xtrace
    local line=$1; shift
    if [ $exitcode == 0 ]; then
        exitcode=1
    fi
    err "$line" "$*"
    # Give buffers a second to flush
    sleep 1
    exit $exitcode
}

get_default_host_ip() {
    local host_ip=$1
    local af=$2
    # Search for an IP unless an explicit is set by ``HOST_IP`` environment variable
    if [ -z "$host_ip" ]; then
        host_ip=""
        # Find the interface used for the default route
        host_ip_iface=${host_ip_iface:-$(ip -f "$af" route | awk '/default/ {print $5}' | head -1)}
        local host_ips
        host_ips=$(LC_ALL=C ip -f "$af" addr show "${host_ip_iface}" | sed /temporary/d |awk /$af'/ {split($2,parts,"/");  print parts[1]}')
        local ip
        for ip in $host_ips; do
            host_ip=$ip
            break;
        done
    fi
    echo "$host_ip"
}
