#!/usr/bin/env bash

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

ansiblever=$(ansible --version |grep -Eow '^ansible [^ ]+' |gawk '{ print $2 }')
echo "The actual version of ansible is $ansiblever"

if [[ "$ansiblever" < '2.4.2' ]]; then
  echo "Ansible version 2.4.2 or higher is required"
  exit 1
fi

exit 0
