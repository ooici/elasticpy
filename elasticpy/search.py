#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file search
@date 05/24/12 09:30
@description Class for searching and interfacing with ElasticSearch
'''

from connection import ElasticConnection


class ElasticSearch(object):
    '''
    ElasticSearch wrapper for python.
    Uses simple HTTP queries (RESTful) with json to provide the interface.
    '''

    def __init__(self, host='localhost',port='9200',timeout=None,verbose=False, encoding=None):
        self.host = host
        self.port = port
        self.params = None
        self.verbose = verbose
        self.timeout = timeout
        self.session = ElasticConnection(timeout=timeout,encoding=encoding)

    def timeout(self, value):
        '''
        Specifies a timeout on the search query
        '''
        if not self.params:
            self.params = dict(timeout=value)
            return self
        self.params['timeout'] = value
        return self


    def filtered(self, efilter):
        '''
        Applies a filter to the search
        '''
        if not self.params:
            self.params={'filter' : efilter}
            return self
        if not self.params.has_key('filter'):
            self.params['filter'] = efilter
            return self
        self.params['filter'].update(efilter)
        return self

    def size(self,value):
        '''
        The number of hits to return. Defaults to 10
        '''
        if not self.params:
            self.params = dict(size=value)
            return self
        self.params['size'] = value
        return self

    def from_offset(self, value):
        '''
        The starting from index of the hits to return. Defaults to 0.
        '''
        if not self.params:
            self.params = dict({'from':value})
            return self
        self.params['from'] = value
        return self

    def sort(self, *args, **kwargs):
        '''
        http://www.elasticsearch.org/guide/reference/api/search/sort.html
        Allows to add one or more sort on specific fields. Each sort can be reversed as well. The sort is defined on a per field level, with special field name for _score to sort by score.

        standard arguments are ordered ascending, keyword arguments are fields and you specify the order either asc or desc
        '''
        if not self.params:
            self.params = dict()
        self.params['sort'] = list()
        for arg in args:
            self.params['sort'].append(arg)
        for k,v in kwargs.iteritems():
            self.params['sort'].append({k : v})

        return self

    def sorted(self, fsort):
        '''
        Allows to add one or more sort on specific fields. Each sort can be reversed as well. The sort is defined on a per field level, with special field name for _score to sort by score.
        '''
        if not self.params:
            self.params = dict()
        self.params['sort'] = fsort

        return self

    @staticmethod
    def search(index,itype,key,query,host='localhost',port='9200'):
        return ElasticSearch(host=host,port=port).search_simple(index,itype,key,query)

    def search_all(self, query):
        request = self.session
        url = 'http://%s:%s/_search' %(self.host, self.port)
        query_header = {'query':query}
        if self.params:
            query_header.update(self.params)
        if self.verbose:
            print query_header
        response = request.post(url,query_header)
        return response

    def search_simple(self, index,itype, key, search_term):
        '''
        ElasticSearch.search_simple(index,itype,key,search_term)
        Usage:
        > es = ElasticSearch()
        > es.search_simple('twitter','users','name','kim')
        '''
        request = self.session
        url = 'http://%s:%s/%s/%s/_search?q=%s:%s' % (self.host,self.port,index,itype,key,search_term)
        response = request.get(url)

        return response

    def search_advanced(self, index, itype, query):
        '''
        Advanced search interface using specified query
        > query = ElasticQuery().term(user='kimchy')
        > ElasticSearch().search_advanced('twitter','posts',query)
         ... Search results ...

        '''
        request = self.session
        url = 'http://%s:%s/%s/%s/_search' % (self.host,self.port,index,itype)
        if self.params:
            query_header = dict(query=query, **self.params)
        else:
            query_header = dict(query=query)
        if self.verbose:
            print query_header
        response = request.post(url,query_header)

        return response

    def doc_create(self,index,itype,value):
        '''
        Creates a document
        '''
        request = self.session
        url = 'http://%s:%s/%s/%s/' % (self.host, self.port, index, itype)
        if self.verbose:
            print value
        response = request.post(url,value)
        return response


    def search_index_simple(self,index,key,search_term):
        '''
        Search the index using a simple key and search_term
        @param index Name of the index
        @param key Search Key
        @param search_term The term to be searched for
        '''
        request = self.session
        url = 'http://%s:%s/%s/_search?q=%s:%s' % (self.host,self.port,index,key,search_term)
        response = request.get(url)
        return response

    def search_index_advanced(self, index, query):
        '''
        Advanced search query against an entire index

        > query = ElasticQuery().query_string(query='imchi')
        > search = ElasticSearch()
        '''
        request = self.session
        url = 'http://%s:%s/%s/_search' % (self.host, self.port, index)
        if self.params:
            content = dict(query=query, **self.params)
        else:
            content = dict(query=query)
        if self.verbose:
            print content
        response = request.post(url,content)
        return response


    def index_create(self, index, number_of_shards=5,number_of_replicas=1):
        '''
        Creates the specified index
        > search = ElasticSearch()
        > search.index_create('twitter')
          {"ok":true,"acknowledged":true}
        '''
        request = self.session
        content = {'settings' : dict(number_of_shards=number_of_shards, number_of_replicas=number_of_replicas)}
        if self.verbose:
            print content
        url = 'http://%s:%s/%s' % (self.host, self.port, index)
        response = request.put(url,content)
        return response

    def index_delete(self, index):
        '''
        Delets the specified index
        > search = ElasticSearch()
        > search.index_delete('twitter')
          {"ok" : True, "acknowledged" : True }
        '''
        request = self.session
        url = 'http://%s:%s/%s' % (self.host, self.port, index)
        response = request.delete(url)
        return response

    def index_open(self, index):
        '''
        Opens the speicified index.
        http://www.elasticsearch.org/guide/reference/api/admin-indices-open-close.html

        > ElasticSearch().index_open('my_index')
        '''
        request = self.session
        url = 'http://%s:%s/%s/_open' % (self.host, self.port, index)
        response = request.post(url,None)
        return response

    def index_close(self, index):
        '''
        Closes the speicified index.
        http://www.elasticsearch.org/guide/reference/api/admin-indices-open-close.html

        > ElasticSearch().index_close('my_index')
        '''
        request = self.session
        url = 'http://%s:%s/%s/_close' % (self.host, self.port, index)
        response = request.post(url,None)
        return response

    def river_couchdb_create(self, index_name,index_type='',couchdb_db='', river_name='',couchdb_host='localhost', couchdb_port='5984',couchdb_user=None, couchdb_password=None, couchdb_filter=None,script=''):
        '''
        https://github.com/elasticsearch/elasticsearch-river-couchdb

        Creates a river for the specified couchdb_db.

        > search = ElasticSearch()
        > search.river_couchdb_create('feeds','feeds','feeds')
          {u'_id': u'_meta',
         u'_index': u'_river',
         u'_type': u'test_db',
         u'_version': 1,
         u'ok': True}
        '''
        request = self.session
        if not index_type:
            index_type = index_name
        if not couchdb_db:
            couchdb_db = index_name
        content = {
            'type' : 'couchdb',
            'couchdb' : {
                'host' : couchdb_host,
                'port' : couchdb_port,
                'db' : couchdb_db,
                'filter' : couchdb_filter
            },
            'index' : {
                'index' : index_name,
                'type' : index_type
            }
        }
        if couchdb_user and couchdb_password:
            content['couchdb']['user'] = couchdb_user
            content['couchdb']['password'] = couchdb_password
        if script:
            content['couchdb']['script'] = script
        if self.verbose:
            print content
        url = 'http://%s:%s/_river/%s/_meta' %(self.host, self.port, river_name or index_name)
        response = request.post(url,content)
        return response

    def river_couchdb_delete(self, index_name):
        '''
        https://github.com/elasticsearch/elasticsearch-river-couchdb

        Delete's a river for the specified index
        WARNING: It DOES NOT delete the index, only the river, so the only effects of this are that the index will no longer poll CouchDB for updates.
        '''
        request = self.session
        url = 'http://%s:%s/_river/%s' % (self.host, self.port, index_name)
        response = request.delete(url)
        return response


    @staticmethod
    def list_indexes(host='localhost',port='9200'):
        '''
        Lists indices
        '''
        return ElasticSearch(host,port).index_list()


    def index_list(self):
        '''
        Lists indices
        '''
        request = self.session
        url = 'http://%s:%s/_cluster/state/' % (self.host, self.port)
        response = request.get(url)
        if request.status_code==200:
            return response.get('metadata',{}).get('indices',{}).keys()
        else:
            return response

    def map(self,index_name, index_type, map_value):
        '''
        Enable a specific map for an index and type
        '''
        request = self.session
        url = 'http://%s:%s/%s/%s/_mapping' % (self.host, self.port, index_name, index_type)
        content = { index_type : { 'properties' : map_value } }
        if self.verbose:
            print content
        response = request.put(url,content)
        return response

    @staticmethod
    def list_types(index_name, host='localhost',port='9200'):
        '''
        Lists the context types available in an index
        '''
        return ElasticSearch(host=host, port=port).type_list(index_name)

    def type_list(self, index_name):
        '''
        List the types available in an index
        '''
        request = self.session
        url = 'http://%s:%s/%s/_mapping' % (self.host, self.port, index_name)
        response = request.get(url)
        if request.status_code == 200:
            return response[index_name].keys()
        else:
            return response

    @staticmethod
    def raw_query(module, method='GET', data=None, host='localhost',port='9200'):
        return ElasticSearch(host=host,port=port).raw(module,method,data)

    def raw(self, module, method='GET', data=None):
        '''
        Submits or requsts raw input
        '''
        request = self.session
        url = 'http://%s:%s/%s' % (self.host, self.port, module)
        if self.verbose:
            print data
        if method=='GET':
            response = request.get(url)
        elif method=='POST':
            response = request.post(url,data)
        elif method=='PUT':
            response = request.put(url,data)
        elif method=='DELETE':
            response = request.delete(url)
        else:
            return {'error' : 'No such request method %s' % method}

        return response
