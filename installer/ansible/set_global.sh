#!/bin/bash

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

## This file is used to set global variables. 
## Before executing this file, please set the variables you need in the script/vars.sh file.

source ./script/global_vars.sh
var_path=./group_vars

# commom
sed -i 's/^host_ip: .*/host_ip: '"$host_ip"'/g' $var_path/common.yml
sed -i 's/^install_from: .*/install_from: '"$install_from"'/g' $var_path/common.yml
