#!/usr/bin/env python

'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file elasticpy.py
@license Apache 2.0
@description Wrapper for elastic search

Copyright 2012 UC Regents
Apache License 2.0
See COPYING for more information.
'''
__author__ = 'Luke Campbell'
__version__ = '0.5'


import json
import urllib2
import sys

class ElasticSearch(object):
    '''
    ElasticSearch wrapper for python.
    Uses simple HTTP queries (RESTful) with json to provide the interface.
    '''
    
    def __init__(self, host='localhost',port='9200',verbose=False):
        self.host = host
        self.port = port
        self.params = None
        self.verbose = verbose

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
        self.params['filter'] = efilter
        return self

    def size(self,value):
        '''
        The number of hits to return. Defaults to 10
        '''
        if not self.params:
            self.params = dict(size=value)
            return self
        self.params['timeout'] = value
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

    def search_simple(self, index,itype, key, search_term):
        '''
        ElasticSearch.search_simple(index,itype,key,search_term)
        Usage: 
        > es = ElasticSearch()
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
        Advanced search interface using specified query
        > query = ElasticQuery().term(user='kimchy')
        > ElasticSearch().search_advanced('twitter','posts',query)
         ... Search results ...

        '''
        headers = {
            'Content-Type' : 'application/json'
        }
        url = 'http://%s:%s/%s/%s/_search' % (self.host,self.port,index,itype)
        if self.params:
            query_header = dict(query=query, **self.params)
        else:
            query_header = dict(query=query)
        content = json.dumps(query_header)
        if self.verbose:
            print content
        url_request = urllib2.Request(url,content,headers)
        s = urllib2.urlopen(url_request).read()
        
        return json.loads(s)

    def doc_create(self,index,itype,value):
        '''
        Creates a document
        '''
        url = 'http://%s:%s/%s/%s/' % (self.host, self.port, index, itype)
        content = json.dumps(value)
        if self.verbose:
            print content
        url_request = urllib2.Request(url,content)
        url_request.add_header('Content-Type','application/json')
        url_request.get_method = lambda : 'POST'
        s = urllib2.urlopen(url_request).read()
        return json.loads(s)

    
    def search_index_simple(self,index,key,search_term):
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

    def search_index_advanced(self, index, query):
        '''
        Advanced search query against an entire index

        > query = ElasticQuery().query_string(query='imchi')
        > search = ElasticSearch()
        '''
        url = 'http://%s:%s/%s/_search' % (self.host, self.port, index)
        if self.params:
            content = dict(query=query, **self.params)
        else:
            content = dict(query=query)
        content = json.dumps(content)
        if self.verbose:
            print content
        url_request = urllib2.Request(url,content)
        s = urllib2.urlopen(url_request).read()
        return json.loads(s)


    def index_create(self, index, number_of_shards=5,number_of_replicas=1):
        '''
        Creates the specified index
        > search = ElasticSearch()
        > search.index_create('twitter')
          {"ok":true,"acknowledged":true}
        '''
        content = {'settings' : dict(number_of_shards=number_of_shards, number_of_replicas=number_of_replicas)}
        content = json.dumps(content)
        if(self.verbose):
            print content
        url = 'http://%s:%s/%s' % (self.host, self.port, index)
        url_request = urllib2.Request(url,content)
        url_request.add_header('Content-Type','application/json')
        url_request.get_method = lambda : 'PUT'
        s = urllib2.urlopen(url_request).read()
        return json.loads(s)

    def index_delete(self, index):
        '''
        Delets the specified index
        > search = ElasticSearch()
        > search.index_delete('twitter')
          {"ok" : True, "acknowledged" : True }
        '''
        url = 'http://%s:%s/%s' % (self.host, self.port, index)
        url_request = urllib2.Request(url,None)
        url_request.add_header('Content-Type','application/json')
        url_request.get_method = lambda : 'DELETE'
        s = urllib2.urlopen(url_request).read()
        return json.loads(s)

    def index_open(self, index):
        '''
        Opens the speicified index.
        http://www.elasticsearch.org/guide/reference/api/admin-indices-open-close.html

        > ElasticSearch().index_open('my_index')
        '''
        url = 'http://%s:%s/%s/_open' % (self.host, self.port, index)
        url_request = urllib2.Request(url,None)
        url_request.add_header('Content-Type','application/json')
        url_request.get_method = lambda : 'DELETE'
        s = urllib2.urlopen(url_request).read()
        return json.loads(s)
    
    def index_close(self, index):
        '''
        Closes the speicified index.
        http://www.elasticsearch.org/guide/reference/api/admin-indices-open-close.html

        > ElasticSearch().index_close('my_index')
        '''
        url = 'http://%s:%s/%s/_close' % (self.host, self.port, index)
        url_request = urllib2.Request(url,None)
        url_request.add_header('Content-Type','application/json')
        url_request.get_method = lambda : 'DELETE'
        s = urllib2.urlopen(url_request).read()
        return json.loads(s)

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
        content_json = json.dumps(content)
        if self.verbose:
            print content_json
        url = 'http://%s:%s/_river/%s/_meta' %(self.host, self.port, river_name or index_name)

        url_request = urllib2.Request(url, content_json)

        url_request.add_header('Content-Type', 'application/json')
        url_request.get_method = lambda : 'PUT'
        s = urllib2.urlopen(url_request).read()
        return json.loads(s)

    def river_couchdb_delete(self, index_name):
        '''
        https://github.com/elasticsearch/elasticsearch-river-couchdb

        Delete's a river for the specified index
        WARNING: It DOES NOT delete the index, only the river, so the only effects of this are that the index will no longer poll CouchDB for updates.
        '''
        url = 'http://%s:%s/_river/%s' % (self.host, self.port, index_name)
        url_request = urllib2.Request(url)
        url_request.add_header('Content-Type', 'application/json')
        url_request.get_method = lambda : 'DELETE'
        s = urllib2.urlopen(url_request).read()
        return json.loads(s)


    def geo_map(self,index,itype,field):
        '''
        Sets up the map for geotype
        See: https://gist.github.com/2352968
        for a raw example

        > ElasticSearch().geo_map('map','points','pin.location')
        Adds a map for the element pin.location in map/points to geo_point
        '''
        url = 'http://%s:%s/%s/%s/_mapping' % (self.host, self.port, index, itype)
        # This is gonna be ugly
        levels = field.split('.')
        levels.reverse()
        content = {}
        for i in xrange(len(levels)):
            if i == 0:
                content = {'properties' : { levels[i] : {'type' : 'geo_point' } } }
                continue
            content = {'properties' : { levels[i] : content }}
        content = { itype : content }
        content = json.dumps(content)
        if self.verbose:
            print url
            print content
        url_request = urllib2.Request(url,content)
        url_request.add_header('Content-Type','application/json')
        url_request.get_method = lambda : 'PUT'
        s = urllib2.urlopen(url_request).read()
        return json.loads(s)


    def index_list(self):
        '''
        Lists indices
        '''

        url = 'http://%s:%s/_status' % (self.host, self.port)
        request = urllib2.Request(url,None)
        request.add_header('Content-Type','json')
        response = json.loads(urllib2.urlopen(request).read())
        return response['indices'].keys()
            
    def map(self,index_name, index_type, map_value):
        '''
        Enable a specific map for an index and type
        '''
        url = 'http://%s:%s/%s/%s/_mapping' % (self.host, self.port, index_name, index_type)
        content = { index_type : { 'properties' : map_value } }
        content = json.dumps(content)
        if self.verbose:
            print url
            print content
        request = urllib2.Request(url,content)
        request.add_header('Content-Type','application/json')
        request.get_method = lambda : 'PUT'
        s = urllib2.urlopen(request).read()
        return json.loads(s)

    def type_list(self, index_name):
        '''
        List the types available in an index
        '''
        url = 'http://%s:%s/%s/_mapping' % (self.host, self.port, index_name)
        request = urllib2.Request(url, None)
        request.add_header('Content-Type','application/json')
        if self.verbose:
            print url
        response = json.loads(urllib2.urlopen(request).read())
        return response[index_name].keys()

    def raw(self, module, method, data):
        '''
        Submits or requsts raw input
        '''
        url = 'http://%s:%s/%s' % (self.host, self.port, module)
        content = json.dumps(data)
        if self.verbose:
            print content
        request = urllib2.Request(url,content)
        request.add_header('Content-Type','application/json')
        request.get_method = lambda : method
        response = json.loads(urllib2.urlopen(request).read())
        return response

class ElasticQuery(dict):
    '''
    Wrapper for ElasticSearch queries.
    '''
    
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
        http://www.elasticsearch.org/guide/reference/query-dsl/text-query.html

        A family of text queries that accept text, analyzes it, and constructs a query out of it.
        > eq = ElasticQuery().text('message','this is a test')
        > eq
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
        http://www.elasticsearch.org/guide/reference/query-dsl/ids-query.html
        Filters documents that only have the provided ids. Note, this filter does not require the _id field to be indexed since it works using the _uid field.
        > query = ElasticQuery().ids(['1','2'],type='tweets')
        > query
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

    def fuzzy(self, field, value, boost=1.0, min_similarity=0.5, prefix_length=0):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/fuzzy-query.html
        A fuzzy based query that uses similarity based on Levenshtein (edit distance) algorithm.
        '''
        if not (field and value): return
        self['fuzzy'] = { field : dict(value=value, boost=boost, min_similarity=min_similarity, prefix_length=prefix_length)}
        return self

    def fuzzy_like_this(self, like_text, fields='_all', ignore_tf=False, max_query_terms=25, min_similarity=0.5, prefix_length=0, boost=1.0, analyzer=None):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/flt-query.html
        Fuzzy like this query find documents that are "like" provided text by running it against one or more fields.

        > query = ElasticQuery().fuzzy_like_this('text like this one', fields=['name.first', 'name.last'], max_query_terms=12)
        > query
            {'fuzze_like_this': {'boost': 1.0,
              'fields': ['name.first', 'name.last'],
              'ifgnore_tf': False,
              'like_text': 'text like this one',
              'max_query_terms': 12,
              'min_similarity': 0.5,
              'prefix_length': 0}}

        '''
        self['fuzze_like_this'] = dict(fields=fields, like_text=like_text, ifgnore_tf=ignore_tf, max_query_terms=max_query_terms, min_similarity=min_similarity, prefix_length=prefix_length, boost=boost)
        if analyzer:
            self['fuzzy_like_this']['analyzer'] = analyzer

        return self
        

    def has_child(self, child_type, query):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/has-child-query.html
        The has_child query accepts a query and the child type to run against, and results in parent documents that have child docs matching the query.
        
        > child_query = ElasticQuery().term(tag='something')
        > query = ElasticQuery().has_Child('blog_tag', child_query)
        '''

        if not (child_type and query): return

        self['has_child'] = dict(query=query)
        self['has_child']['type'] = child_type
        return self
    
    def match_all(self):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/match-all-query.html
        A query that matches all documents. Maps to Lucene MatchAllDocsQuery
        '''

        self['match_all'] = dict()
        return self

    def mlt(self, like_text, fields='_all',percent_terms_to_match=0.3, min_term_freq=2, max_query_terms=25, stop_words=[], min_doc_freq=5, max_doc_freq=0, min_word_len=0, max_word_len=0, boost_terms=1, boost=1, analyzer=None):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/mlt-query.html
        More like this query find documents that are "like" provided text by running it against one or more fields.
        > query = ElasticQuery().mlt('text like this one', fields=['post.content'])
        '''

        if not like_text: return

        self['more_like_this'] = dict(
            like_text=like_text,
            fields=fields,
            percent_terms_to_match=percent_terms_to_match,
            min_term_freq=min_term_freq,
            max_query_terms=max_query_terms,
            min_doc_freq=min_doc_freq, 
            max_doc_freq=max_doc_freq,
            min_word_len=min_word_len,
            max_word_len=max_word_len,
            boost_terms=boost_terms,
            boost=boost
        )
        if analyzer:
            self['more_like_this']['analyzer'] = analyzer

        if stop_words:
            self['more_like_this']['stop_words'] = stop_words

        return self

    def prefix(self, field, value):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/prefix-query.html
        Matches documents that have fields containing terms with a specified prefix (not analyzed). The prefix query maps to Lucene PrefixQuery. 
        Example, the following finds a document where the user field contains a temr that starts with lu
        > query = ElasticQuery().prefix('user', 'lu')
        '''
        if not (field and value): return

        self['prefix'] = {field : value}
        return self

    def query_string(self,
            query,
            default_field='_all',
            default_operator='OR',
            analyzer=None,
            allow_leading_wildcard=True, 
            lowercase_expanded_terms=True,
            enable_position_increments=True,
            fuzzy_prefix_length=0,
            fuzzy_min_sim=0.5,
            phrase_slop=0,
            boost=1.0,
            analyze_wildcard=None,
            auto_generate_phase_queries=False,
            minimum_should_match=None):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/query-string-query.html
        A query that uses a query parser in order to parse its content.
        
        > query = ElasticQuery().query_string('this AND that OR thus', default_field='content')
        '''
        if not query: return
        self['query_string'] = dict(
            query=query,
            default_field=default_field,
            default_operator=default_operator,
            allow_leading_wildcard=allow_leading_wildcard,
            lowercase_expanded_terms=lowercase_expanded_terms,
            enable_position_increments=enable_position_increments,
            fuzzy_prefix_length=fuzzy_prefix_length,
            fuzzy_min_sim=fuzzy_min_sim,
            phrase_slop=phrase_slop,
            boost=boost,
            analyze_wildcard=analyze_wildcard,
            auto_generate_phase_queries=auto_generate_phase_queries,
        )
        if analyzer:
            self['query_strict']['analyzer'] = analyzer
            if analyze_wildcard:
                self['query_strict']['analyze_wildcard'] = analyze_wildcard

        return self

    def range(self, 
            field, 
            from_value, 
            to_value, 
            include_lower=True,
            include_upper=True,
            boost=1.0):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/range-query.html
        Matches documents with fields that have terms within a certain range. The type of the Lucene query depends on the field type, for string fields, the TermRangeQuery, while for number/date fields, the query is a NumericRangeQuery. The following example returns all documents where age is between 10 and 20:

        > query = ElasticQuery().range('age', from_value=10, to_value=20, boost=2.0)
        '''

        if not (field and from_value and to_value): return
        self['range'] = {field : { 'from' : from_value, 'to' : to_value, 'include_lower' : include_lower, 'include_upper' : include_upper, 'boost' : boost}}
        return self

    def wildcard(self, field, value):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/wildcard-query.html
        Matches documents that have fields matching a wildcard expression (not analyzed). Supported wildcards are *, which matches any character sequence (including the empty one), and ?, which matches any single character. Note this query can be slow, as it needs to iterate over many terms. In order to prevent extremely slow wildcard queries, a wildcard term should not start with one of the wildcards * or ?. The wildcard query maps to Lucene WildcardQuery.

        > query = ElasticQuery.wildcard('user', 'ki*y')
        '''
        if not (field and value): return

        self['wildcard'] = {field : value}
        return self

    


class ElasticFilter(dict):
    '''
    Wrapper for ElasticSearch filters
    '''

    def and_filter(self, query=None):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/and-filter.html
        A filter that matches documents using AND boolean operator on other queries. This filter is more performant then bool filter. Can be placed within queries that accept a filter. 
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



class ElasticFacet(dict):
    '''
    Facets for search
    http://www.elasticsearch.org/guide/reference/api/search/facets/

    The usual purpose of a full-text search engine is to return a small number of documents matching your query.

    Facets provide aggregated data based on a search query. In the simplest case, a terms facet can return facet counts for various facet values for a specific field. ElasticSearch supports more facet implementations, such as statistical or date histogram facets.

    The field used for facet calculations must be of type numeric, date/time or be analyzed as a single token - see the Mapping guide for details on the analysis process.
    You can give the facet a custom name and return multiple facets in one request.
    '''

    def terms(self, facet_name, field, size=10, order=None, all_terms=False, exclude=[], regex='', regex_flags=''):
        '''
        Allow to specify field facets that return the N most frequent terms.

        Ordering: Allow to control the ordering of the terms facets, to be ordered by count, term, reverse_count or reverse_term. The default is count.
        All Terms: Allow to get all the terms in the terms facet, ones that do not match a hit, will have a count of 0. Note, this should not be used with fields that have many terms.
        Excluding Terms: It is possible to specify a set of terms that should be excluded from the terms facet request result.
        Regex Patterns: The terms API allows to define regex expression that will control which terms will be included in the faceted list.
        '''

        self[facet_name] = dict(terms=dict(field=field,size=size))
        if order:
            self[facet_name][terms]['order'] = order
        if all_terms:
            self[facet_name][terms]['all_terms'] = True
        if exclude:
            self[facet_name][terms]['exclude'] = exclude
        if regex:
            self[facet_name][terms]['regex'] = regex
        if regex_flags:
            self[facet_name][terms]['regex_flags'] = regex_flags

        return self

    def range(self,facet_name, field, ranges=[]):
        '''
        Range facet allow to specify a set of ranges and get both the number of docs (count) that fall within each range, and aggregated data either based on the field, or using another field.
        http://www.elasticsearch.org/guide/reference/api/search/facets/range-facet.html

        > ElasticFacet().range('range1', 'field_name', [ slice(50), slice(20,70), slice(50,-1) ])
        {
          "range1" : {
            "range" : {
                "field" : "field_name",
                "ranges" : [
                    { "to" : 50 },
                    { "from" : 20, "to" : 70 },
                    { "from" : 70, "to" : 120 },
                    { "from" : 150 }
                ]
            }
           }
        }
        '''

        self[facet_name] = {'range'  : { 'field' : field,'ranges' : [] }}
        for s in ranges:
            if not isinstance(s, slice):
                continue
            entry = dict()
            if s.start:
                entry['from'] = s.start
            if s.stop != -1:
                entry['to'] = s.stop
            self[facet_name]['range']['ranges'].append(entry)

        return self


class ElasticMap(dict):
    '''
    Mapping is the process of defining how a document should be mapped to the Search Engine, including its searchable characteristics such as which fields are searchable and if/how they are tokenized. In ElasticSearch, an index may store documents of different "mapping types". ElasticSearch allows one to associate multiple mapping definitions for each mapping type.

Explicit mapping is defined on an index/type level. By default, there isn't a need to define an explicit mapping, since one is automatically created and registered when a new type or new field is introduced (with no performance overhead) and have sensible defaults. Only when the defaults need to be overridden must a mapping definition be provided.
    '''
    def __init__(self, field):
        self.field = field
        self[self.field] = dict()

    def type(self,type_name):
        '''
        Assigns a particular type to a field in the mapped properties.
        Available types are: string, integer/long, float/double, boolean and null
        '''
        self[self.field]['type'] = type_name
        return self

    def analyzed(self,should=True):
        '''
        Specifies to the map that the field should be analyzed when indexed.
        '''
        self[self.field]['index'] = 'analyzed' if should else 'not_analyzed'
        return self

    def ignore(self):
        '''
        Specifies that the field shouuld be ignored in the index
        '''
        self[self.field] = {'index' : 'no'}
        return self

    def null_value(self,value):
        '''
        Specifies the null value to use when indexing.
        '''
        self[self.field]['null_value'] = value
        return self

    def term_vector(self, params):
        '''
        params are either True/False, 'with_offsets', 'with_positions', 'with_positions_offsets'
        '''
        if params == True:
            self[self.field]['term_vector'] = 'yes'
        elif params == False:
            self[self.field]['term_vector'] = 'no'
        else:
            self[self.field]['term_vector'] = params
        return self

    def boost(self, value=1.0):
        '''
        The boost value. Defaults to 1.0
        '''
        self[self.field]['boost'] = value
        return self

    def omit_norms(self, should=False):
        '''
        Boolean value if norms should be ommitted or not. Defaults to False
        '''
        self[self.field]['omit_norms'] = should
        return self

    def omit_term_freq_and_positions(self, should=False):
        '''
        Boolean value if term freq and positions should be ommitted. Defaults to false.
        '''
        self[self.field]['omit_term_freq_and_positions'] = shuold
        return self

    def analyzer(self, value):
        '''
        The analyzer used to analyze the text contents when analyzed during indexing and when searching using a query string. Defaults to the globally configured analyzer.
        '''
        self[self.field]['analyzer'] = value
        return self

    def search_analyzer(self, value):
        '''
        The analyzer used to analyze the field when part of a query string
        '''
        self[self.field]['search_analyzer'] = value
        return self

    def include_in_all(self, should=True):
        '''
        Should the field be included in the _all field (self,if enabled). Defaults to true or to the parent object type setting
        '''
        self[self.field]['include_in_all'] = should
        return self

