#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file connection
@date 05/24/12 09:29
@description Connection Class for elasticpy
'''
import requests
import json

class ElasticConnection(object):
    def __init__(self, timeout=None):
        self.status_code = 0
        self.timeout=timeout

    def get(self, url):
        headers = {'Content-Type' : 'Application/json'}
        try:
            response = requests.get(url,headers=headers,timeout=self.timeout)
        except requests.ConnectionError as e:
            self.status_code = 0
            return {'error':e.message}
        self.status_code = response.status_code
        return json.loads(response.text)
    def post(self, url, data):
        headers = {'Content-Type' : 'Application/json'}
        body = json.dumps(data)
        try:
            response = requests.post(url,data=body,headers=headers,timeout=self.timeout)
        except requests.ConnectionError as e:
            self.status_code = 0
            return {'error' : e.message}
        self.status_code = response.status_code
        return json.loads(response.text)

    def put(self, url, data):
        headers = {'Content-Type' : 'Application/json'}
        body = json.dumps(data)
        try:
            response = requests.post(url,data=body,headers=headers,timeout=self.timeout)
        except requests.ConnectionError as e:
            self.status_code = 0
            return {'error' : e.message}
        self.status_code = response.status_code
        return json.loads(response.text)

    def delete(self,url):
        headers = {'Content-Type' : 'Application/json'}
        try:
            response = requests.delete(url,headers=headers,timeout=self.timeout)
        except requests.ConnectionError as e:
            self.status_code = 0
            return {'error' : e.message}
        self.status_code = response.status_code
        return json.loads(response.text)
