#!/usr/bin/env python

'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file elasticpy.py
@description Wrapper for elastic search
'''

import json
import urllib2
import sys

class ElasticSearch(object):
    '''
    ElasticSearch wrapper for python.
    Uses simple HTTP queries (RESTful) with json to provide the interface.
    '''
    def __init__(self, host='localhost',port='9200'):
        self.host = host
        self.port = port

    def search_simple(self, index,itype, key, search_term):
        '''
        ElasticSearch.search_simple(index,itype,key,search_term)
        Usage: 
        > es = elasticpy.ElasticSearch()
        > es.search_simple('twitter','users','name','kim')
        '''
        headers = {
            'Content-Type' : 'application/json'
        }
        url = 'http://%s:%s/%s/%s/_search?q=%s:%s' % (self.host,self.port,index,itype,key,search_term)
        url_request = urllib2.Request(url,None,headers)
        s = urllib2.urlopen(url_request).read()
        return json.loads(s)

    def search_advanced(self, index, itype, query):
        '''
        Advanced search interface
        @param index Name of the index
        @param itype Index type
        @param query Query (dictionary)
        '''
        headers = {
            'Content-Type' : 'application/json'
        }
        url = 'http://%s:%s/%s/%s/_search' % (self.host,self.port,index,itype)
        query_header = {'query' : query }
        content = json.dumps(query_header)

        url_request = urllib2.Request(url,content,headers)
        s = urllib2.urlopen(url_request).read()
        
        return json.loads(s)

    def search_index_simple(index,key,search_term):
        '''
        Search the index using a simple key and search_term
        @param index Name of the index
        @param key Search Key
        @param search_term The term to be searched for
        '''
        headers = {
            'Content-Type' : 'application/json'
        }
        url = 'http://%s:%s/%s/_search?q=%s:%s' % (self.host,self.port,index,key,search_term)
        url_request = urllib2.Request(url,None,headers)
        s = urllib2.urlopen(url_request).read()
        return json.loads(s)

    def create(self,index,itype,name,data):

        if not isinstance(data, dict): return
        headers = {
            'Content-Type' : 'application/json'
        }
        content = json.dumps(data)
        if itype:
            url = 'http://%s:%s/%s/%s/%s' % (self.host, self.port, index, itype, name)
        else:
            url = 'http://%s:%s/%s/%s' % (self.host, self.port, index, name)
        
        url_request = urllib2.Request(url,content,headers)

        s = urllib2.urlopen(url_request).read()

        return json.loads(s)
            

class ElasticQuery(dict):
    def query(self):
        return dict(self)
    
    def term(self,**kwargs):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/term-query.html
        Matches documents that have fields that contain a term (not analyzed). The term query maps to Lucene TermQuery
        The following matches documents where the user field contains the term 'kimchy':
        > term = ElasticQuery.term(user='kimchy')
        > term.query()
          {'term' : {'user':'kimchy'}}

        '''
        if len(kwargs) > 1:
            self['terms'] = dict(**kwargs)
        else:
            self['term'] = dict(**kwargs)

    def text(self, field, query, operator):
        '''
        ElasticQuery.text(field,query,[operator=('and'|'or')])
        http://www.elasticsearch.org/guide/reference/query-dsl/text-query.html

        field - Field name
        query - The search query
        operator - 'and'|'or'

        A family of text queries that accept text, analyzes it, and constructs a query out of it.
        > eq = elasticpy.ElasticQuery()
        > eq.text('message', 'this is a test')
        > eq.query
          {
            'text' : {
              'message' : 'this is a test'
             }
          }
        Note: field represents the name of the field, you can use _all instead.
        '''
        self['text'] = {field : query}
        if operator and (operator=='and' or operator=='or'): self['text']['operator'] = operator

    def bool(self,must=None, should=None, must_not=None,minimum_number_should_match=-1, boost=-1):
        '''
        ElasticQuery.bool([must=ElasticQuery, should=ElasticQuery, must_not=ElasticQuery, minimum_number_should_match=-1, boost=-1]) 
        http://www.elasticsearch.org/guide/reference/query-dsl/bool-query.html
        A query that matches documents matching boolean combinations of other queris. The bool query maps to Lucene BooleanQuery. It is built using one of more boolean clauses, each clause with a typed occurrenc. The occurrence types are:
        'must' - The clause(query) must appear in matching documents.
        'should' - The clause(query) should appear in the matching document. A boolean query with no 'must' clauses, one or more 'should' clauses must match a document. The minimum number of 'should' clauses to match can be set using 'minimum_number_should_match' parameter.
        'must_not' - The clause(query) must not appear in the matching documents. Note that it is not possible to search on documents that only consists of a 'must_not' clause(s).

        'minimum_number_should_match' - Minimum number of documents that should match
        'boost' - boost value
        > term = ElasticQuery()
        > term.term(user='kimchy')
        > query = ElasticQuery()
        > query.bool(should=term)
        > query.query()
          { 'bool' : { 'should' : { 'term' : {'user':'kimchy'}}}}


        '''
        self['bool'] = dict()
        if must and isinstance(must,ElasticQuery):
            self['bool']['must'] = must.query()
        if should and isinstance(should,ElasticQuery):
            self['bool']['should'] = should.query()
        if must_not and isinstance(must_not,ElasticQuery):
            self['bool']['must_not'] = must_not.query()
        if minimum_number_should_match > 0:
            self['bool']['minimum_number_should_match'] = minimum_number_should_match
        if boost > 0:
            self['bool']['boost'] = boost
    
    def ids(self, values=None, itype=''):
        '''
        ElasticQuery.ids(values=["1","4",..."n"], itype='my_type')
        http://www.elasticsearch.org/guide/reference/query-dsl/ids-query.html
        Filters documents that only have the provided ids. Note, this filter does not require the _id field to be indexed since it works using the _uid field.
        > query = elasticpy.ElasticQuery()
        > query.ids(['1','2'], type='tweets')
        > query.query()
          {
            'ids' : {
              'type' : 'tweets',
              'values' : ['1','2']
            }
          }
        '''
        if not values: return
        
        self['ids'] = dict(values=values)
       
        if itype:
            self['ids']['type'] = itype

    def filter(self, **kwargs):
        '''
        and - A filter that matches documents using AND boolean operator on other queries. This filter is more performant then bool filter. Can be placed within queries that accept a filter. (http://www.elasticsearch.org/guide/reference/query-dsl/and-filter.html)
        bool - A filter that matches documents matching boolean combinations of other queries. Similar in concept to Boolean query, except that the clauses are other filters. Can be placed within queries that accept a filter. (http://www.elasticsearch.org/guide/reference/query-dsl/bool-filter.html)
        exists - Filters documents where a specific field has a value in them. (http://www.elasticsearch.org/guide/reference/query-dsl/exists-filter.html)
        ids - Filters documents that only have the provided ids. Note, this filter does not require the _id field to be indexed since it works using the _uid field. (http://www.elasticsearch.org/guide/reference/query-dsl/ids-filter.html)
        limit - A limit filter limits the number of documents (per shard) to execute on. (http://www.elasticsearch.org/guide/reference/query-dsl/limit-filter.html)
        type - Filters documents matching the provided document / mapping type. Note, this filter can work even when the _type field is not indexed (using the _uid field). (http://www.elasticsearch.org/guide/reference/query-dsl/type-filter.html)
        geo_bbox - A filter allowing to filter hits based on a point location using a bounding box. Assuming the following indexed document: (http://www.elasticsearch.org/guide/reference/query-dsl/geo-bounding-box-filter.html)
        Example:
        > 


        '''
        filters = [
            'and',
            'bool',
            'exists',
            'ids',
            'limit',
            'type',
            'geo_bbox',
            'geo_distance',
            'geo_distance_range',
            'geo_polygon',
            'has_child',
            'match_all',
            'missing',
            'not',
            'numeric_range',
            'or',
            'prefix',
            'query',
            'range',
            'script',
            'term',
            'terms',
            'nested'
        ]
        if not 'filter' in self:
            self['filter'] = dict()
        for key,val in kwarg.iteritems():
            if key not in filters: continue
            self['filter'][key] = val


    def filtered(self, query, qfilter):
        '''
        ElasticQuery.filtered(query, qfilter)
        http://www.elasticsearch.org/guide/reference/query-dsl/filtered-query.html 
        A query that applies a filter to the results of another query. This query maps to Lucene FilteredQuery.
        > 
        '''
        pass


class ElasticFilter(dict):
    def query(self):
        return self

    def and(self, query=None):
        '''
        and - A filter that matches documents using AND boolean operator on other queries. This filter is more performant then bool filter. Can be placed within queries that accept a filter. (http://www.elasticsearch.org/guide/reference/query-dsl/and-filter.html)
        '''
        if query:
            self['and'] = query

    def bool(self,**kwargs):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/bool-filter.html
        A filter that matches documents matching boolean combinations of other queries. Similar in concept to Boolean query, except that the clauses are other filters. Can be placed within queries that accept a filter.
        '''

        self['bool'] = dict()
        keywords= ['must', 'must_not', 'should']
        for key,val in kwargs.iteritems():
            if key in keywords:
                self['bool'][key] = val

    def exists(self, field=''):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/exists-filter.html
        Filters documents where a specific field has a value in them.
        > filter = elasticpy.ElasticFilter().exists(field='user')
        > filter.query()
          {'exists' : {'field' : 'user' } }
        '''
        if field:
            self['field'] = field

    def ids(self, values=[], itype=''):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/ids-filter.html
Filters documents that only have the provided ids. Note, this filter does not require the _id field to be indexed since it works using the _uid field.

        '''
        if not values: return
        self['ids'] = dict(values=values)
        if itype:
            self['ids']['itype'] = itype
    
    def limit(self, value=0):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/limit-filter.html
        A limit filter limits the number of documents (per shard) to execute on.
        '''
        if value>0:
            self['limit'] = value

    def type(self,value=None):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/type-filter.html
Filters documents matching the provided document / mapping type. Note, this filter can work even when the _type field is not indexed (using the _uid field).
        '''

        if value: self['type'] = dict(value=value)

    

