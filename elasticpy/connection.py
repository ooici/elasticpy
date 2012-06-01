#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file connection
@date 05/24/12 09:29
@description Connection Class for elasticpy
'''
import requests
import json

_use_gevent = False
try:
    import gevent
    from gevent.coros import RLock
    _use_gevent = True
except ImportError:
    pass

class ElasticConnection(object):
    if _use_gevent:
        session = None
        session_lock = RLock()


    def __init__(self, timeout=None, **params):
        self.status_code = 0
        self.timeout=timeout
        if _use_gevent:
            if ElasticConnection.session is None:
                ElasticConnection.session_lock.acquire()
                ElasticConnection.session = requests.Session(**params)
                ElasticConnection.session_lock.release()
        else:
            self.session = requests.Session(timeout=timeout, **params) 
    def get(self, url):
        headers = {'Content-Type' : 'Application/json'}
        try:
            response = self.session.get(url,headers=headers,timeout=self.timeout)
        except requests.ConnectionError as e:
            self.status_code = 0
            return {'error':e.message}
        self.status_code = response.status_code
        return json.loads(response.text)
    def post(self, url, data):
        headers = {'Content-Type' : 'Application/json'}
        body = json.dumps(data)
        try:
            response = self.session.post(url,data=body,headers=headers,timeout=self.timeout)
        except requests.ConnectionError as e:
            self.status_code = 0
            return {'error' : e.message}
        self.status_code = response.status_code
        return json.loads(response.text)

    def put(self, url, data):
        headers = {'Content-Type' : 'Application/json'}
        body = json.dumps(data)
        try:
            response = self.session.post(url,data=body,headers=headers,timeout=self.timeout)
        except requests.ConnectionError as e:
            self.status_code = 0
            return {'error' : e.message}
        self.status_code = response.status_code
        return json.loads(response.text)

    def delete(self,url):
        headers = {'Content-Type' : 'Application/json'}
        try:
            response = self.session.delete(url,headers=headers,timeout=self.timeout)
        except requests.ConnectionError as e:
            self.status_code = 0
            return {'error' : e.message}
        self.status_code = response.status_code
        return json.loads(response.text)
