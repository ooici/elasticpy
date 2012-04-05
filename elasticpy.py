#!/usr/bin/env python

'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file elasticpy.py
@license Apache 2.0
@description Wrapper for elastic search

Copyright 2012 Lucas Campbell
Apache License 2.0
See COPYING for more information.
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

        return self
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

        return self
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
    
        return self
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

        return self
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


        return self
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

    def and_filter(self, query=None):
        '''
        and - A filter that matches documents using AND boolean operator on other queries. This filter is more performant then bool filter. Can be placed within queries that accept a filter. (http://www.elasticsearch.org/guide/reference/query-dsl/and-filter.html)
        '''
        if query:
            self['and'] = query
        return self
    def bool_filter(self,**kwargs):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/bool-filter.html
        A filter that matches documents matching boolean combinations of other queries. Similar in concept to Boolean query, except that the clauses are other filters. Can be placed within queries that accept a filter.
        '''

        self['bool'] = dict()
        keywords= ['must', 'must_not', 'should']
        for key,val in kwargs.iteritems():
            if key in keywords:
                self['bool'][key] = val

        return self
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

        return self
    def ids(self, values=[], itype=''):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/ids-filter.html
Filters documents that only have the provided ids. Note, this filter does not require the _id field to be indexed since it works using the _uid field.

        '''
        if not values: return
        self['ids'] = dict(values=values)
        if itype:
            self['ids']['itype'] = itype
    
        return self
    def limit(self, value=0):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/limit-filter.html
        A limit filter limits the number of documents (per shard) to execute on.
        '''
        if value>0:
            self['limit'] = value

        return self
    def type(self,value=None):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/type-filter.html
Filters documents matching the provided document / mapping type. Note, this filter can work even when the _type field is not indexed (using the _uid field).
        '''

        if value: self['type'] = dict(value=value)

    
        return self
    def geo_bounding_box(self, field, top_left, bottom_right):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/geo-bounding-box-filter.html

        > bounds = ElasticFilter().geo_bounding_box('pin.location', [40.73, -74.1], [40.717, -73.99])
        > bounds = ElasticFilter().geo_bounding_box('pin.location', dict(lat=40.73, lon=-74.1), dict(lat=40.717, lon=-73.99))
        > bounds = ElasticFilter().geo_bounding_box('pin.location', "40.73, -74.1", "40.717, -73.99")
        And geohash
        > bounds = ElasticFilter().geo_bounding_box('pin.location', "drm3btev3e86", "drm3btev3e86")

        '''
        if not field and top_left and bottom_right: return
        self['geo_bounding_box'] = {field: {'top_left' : top_left, 'bottom_right' : bottom_right }}

        return self
    def geo_distance(self, field, center, distance, distance_type='arc'):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/geo-distance-filter.html
        Filters documents that include only hits that exists within a specific distance from a geo point.
        field - Field name
        center - Center point (Geo point)
        distance - String for the distance
        distance_type - (arc | plane) How to compute the distance. Can either be arc (better precision) or plane (faster). Defaults to arc
        > bounds = ElasticFilter().geo_distance('pin.location', [40.73, -74.1], '300km')
        '''

        if not (field and center and distance): return

        self['geo_distance'] = dict(distance=distance, distance_type=distance_type)
        self['geo_distance'][field] = center

        return self

    def geo_distance_range(self, field, center, from_distance, to_distance, distance_type='arc'):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/geo-distance-range-filter.html
        Filters documents that exists within a range from a specific point


        '''

        if not (field and center and from_distance and to_distance): return
        self['geo_distance_range'] = dict(to=to_distance, distance_type=distance_type)
        self['geo_distance_range']['from'] = from_distance
        self['geo_distance_range'][field] = center
        return self

    def geo_polygon(self, field, points=[]):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/geo-polygon-filter.html
        A filter allowing to include hits that only fall within a polygon of points.
        Points are in geo-point format

        > filter = ElasticFilter().geo_polygon('pin.location', [[40, -70], [30, -80], [20, -90]])
        '''
        if not (field and points): return

        self['geo_polygon'] = {field : dict(points=points)}

        return self

    def has_child(self, child_type, query):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/has-child-filter.html
        The has_child filter accepts a query and the child type to run against, and results in parent documents that have child docs matching the query.
        
        > query = ElasticQuery().term(tag='something')
        > filter = ElasticFilter().has_child('blog_tag', query)

        '''

        if not (child_type and query): return

        self['has_child'] = dict(query=query)
        self['has_child']['type'] = child_type
        return self

    def match_all(self):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/match-all-filter.html
        A filter that matches on all documents.
        > filter = ElasticFilter().match_all()
        '''

        self['match_all'] = dict()
        return self

    def missing(self, field):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/missing-filter.html
        Filters documents where a specific field has no value in them.

        '''
        self['missing'] = dict(field=field)
        return self

    def not_filter(self, query):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/not-filter.html
        A filter that filters out matched documents using a query. This filter is more performant then bool filter. Can be placed within queries that accept a filter.
            
        '''
        if not query: return
        self['not'] = query
        return self

    def numeric_range(self,field, from_value, to_value, include_lower=True, include_upper=False):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/numeric-range-filter.html
        Filters documents with fields that have values within a certain numeric range. Similar to range filter, except that it works only with numeric values, and the filter execution works differently.
        '''
        if not (field, from_value and to_value): return

        self['numeric_range'] = {field : { 'from' : from_value, 'to' : to_value, 'include_lower' : include_lower, 'include_upper' : include_upper } } 
        return self

    def or_filter(self, query):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/or-filter.html

        A filter that matches documents using OR boolean operator on other queries. This filter is more performant then bool filter. Can be placed within queries that accept a filter.

        > term1 = ElasticQuery().term(lastname='Campbell')
        > term2 = ElasticQuery().term(firstname='Luke')
        > filter = ElasticFilter().or_filter([term1, term2])
        '''

        self['or'] = query
        return self


    def prefix(self, field, pre):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/prefix-filter.html
        Filters documents that have fields containing terms with a specified prefix (not analyzed). Similar to phrase query, except that it acts as a filter. Can be placed within queries that accept a filter.
        '''

        if not (field and pre): return
        self['prefix'] = {field : pre}
        return self

    def query(self, query):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/query-filter.html
        Wraps any query to be used as a filter. Can be placed within queries that accept a filter.
        '''
        if not query: return
        self['query'] = dict(query_string=query)
        return self

    def range(self, field, from_value, to_value, include_lower=True, include_upper=False):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/range-filter.html

        Filters documents with fields that have terms within a certain range. Similar to range query, except that it acts as a filter. Can be placed within queries that accept a filter.
        '''

        if not (field and from_value and to_value): return
        self['range'] = {field : { 'from' : from_value, 'to' : to_value, 'include_lower' : include_lower, 'include_upper' : include_upper}}

        return self

    def script(self, script):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/script-filter.html
        A filter allowing to define scripts as filters. 

        > script = 'doc["num1"].value > 1'
        > filter = ElasticFilter().script(script)
        '''
        if not script: return

        self['script'] = dict(script=script)
        return self

    def term(self, field, value):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/term-filter.html
        Filters documents that have fields that contain a term (not analyzed). Similar to term query, except that it acts as a filter.
        '''

        if not (field and value): return
        if isinstance(value, list):
            self['terms'] = {field : value}
        else:
            self['term'] = {field : value}
        return self



