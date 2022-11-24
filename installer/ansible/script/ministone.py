#!/usr/bin/env python

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
#
import sys
import os
import requests
import json

def token_issue():
    body = {
        'auth': {
            'identity': {'methods': ['password'],
                         'password': {
                'user': {
                    'name': OS_USERNAME,
                    'domain': {'name': OS_USER_DOMAIN_NAME},
                    'password':  OS_PASSWORD
                }
            }
            },
            'scope': {
                'project': {
                    'name':  OS_PROJECT_NAME,
                    'domain': {'name':  OS_USER_DOMAIN_NAME}
                }
            }
        }
    }

    headers = {'Content-Type': 'application/json'}
    r_post = ''
    try:
        r_post = requests.post(OS_AUTH_URL + '/v3/auth/tokens',
                               headers=headers, data=json.dumps(body))
    except:
        print('ERROR: %s' % (body))
        return None

    if debug:
        print('DEBUG: POST /v3/auth/tokens status_code = %s' %
              (r_post.status_code))
        print('DEBUG: token: %s' % (r_post.headers['X-Subject-Token']))
    if r_post.status_code == 201:
        return r_post.headers['X-Subject-Token']
    else:
        return None

def service_list(token):
    headers = {
        'Content-Type': 'application/json',
        'X-Auth-Token': token
    }
    result_dict = dict()
    try:
        r_get = requests.get(OS_AUTH_URL + '/v3/services', headers=headers)
        if debug:
            print('DEBUG: GET /v3/services status_code = %s' %
                  (r_get.status_code))
        if r_get.status_code != 200:
            return None

        result_list = json.loads(r_get.text)['services']

        for s in result_list:
            result_dict[s['name']] = s['id']
    except:
        return None

    return result_dict

def endpoint_list(token, service):

    headers = {
        'Content-Type': 'application/json',
        'X-Auth-Token': token
    }
    r_get = ''
    try:
        r_get = requests.get(OS_AUTH_URL + '/v3/endpoints', headers=headers)
        if debug:
            print('DEBUG: GET /v3/endpoints - status_code = %s' %
                  (r_get.status_code))
    except:
        return None

    if r_get.status_code != 200:
        return None

    response = r_get.text
    service_dict = service_list(token)

    ep_list = []
    for ep in json.loads(response)['endpoints']:
        if service in service_dict.keys() and (ep['service_id'] == service_dict[service]):
            if debug:
                print('DEBUG: %s %s' % (ep['id'], ep['interface']))
                print('DEBUG:   url %s' % (ep['url']))
            ep_list.append([ep['id'], ep['interface']])

    return ep_list

def endpoint_bulk_update(token, service, url):
    headers = {
        'Content-Type': 'application/json',
        'X-Auth-Token': token
    }

    ep_list = endpoint_list(token, service)
    if not ep_list:
        sys.exit(1)

    if debug:
        print("DEBUG: ep_list: %s %s" % (ep_list, url))

    for ep in ep_list:
        body = {"endpoint": {"url": url}}
        endpoint_id = ep[0]
        if debug:
            print("DEBUG: %s / %s" %
                  (OS_AUTH_URL + '/v3/endpoints/' + endpoint_id, body))
        r_patch = ''
        try:
            r_patch = requests.patch(OS_AUTH_URL + '/v3/endpoints/' +
                                     endpoint_id,
                                     headers=headers, data=json.dumps(body))
        except:
            print('ERROR: endpoint update for id: %s failed.' % (endpoint_id))
            # continue for all the given endpoints
        if r_patch.status_code != 200:
            print('ERROR: endpoint update for id: %s failed. HTTP %s' %
                  (endpoint_id, r_patch.status_code))
        if debug:
            print('DEBUG: PATCH /endpoints/XXXX - status_code = %s' %
                  (r_patch.status_code))

#
# ministone.py - A simple stupid keystone client with almost no dependencies.
#
# Usage:
#   1) ministone.py token_issue
#      Gets a new project scope token, and exit with 0 if successful.
#      Exit with non zero (1) if unsuccessful.
#      Can be used both wating for keystone service start up and
#      getting a token (inernally).
#   2) ministone.py endoint_bulk_update SERVICE_NAME URL
#      Updates URL portion of keystone endpoints of given SERVICE_NAME
#      in one action.
#
if __name__ == '__main__':

    debug = False

    OS_AUTH_URL = os.environ['OS_AUTH_URL']
    OS_PASSWORD = os.environ['OS_PASSWORD']
    OS_PROJECT_DOMAIN_NAME = os.environ['OS_PROJECT_DOMAIN_NAME']
    OS_PROJECT_NAME = os.environ['OS_PROJECT_NAME']
    OS_USERNAME = os.environ['OS_USERNAME']
    OS_USER_DOMAIN_NAME = os.environ['OS_USER_DOMAIN_NAME']
    #OS_USER_DOMAIN_ID = os.environ['OS_USER_DOMAIN_ID']

    # token_issue
    #   used for keystone process start up check.
    token = ''
    if len(sys.argv) == 2 and sys.argv[1] == 'token_issue':
        token = token_issue()
        if not token:
            sys.exit(1)
        else:
            sys.exit(0)

    # endpoint_bulk_update
    #   used for overwriting keystone endpoints
    if not ((len(sys.argv) == 4) and (sys.argv[1] == 'endpoint_bulk_update')):
        print('Specify service_name and url for bulk update. Exiting...')
        sys.exit(1)

    token = token_issue()
    if not token:
        sys.exit(1)
    endpoint_bulk_update(token, sys.argv[2], sys.argv[3])
