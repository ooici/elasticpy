elasticpy
===========

Python wrapper for the elasticsearch indexing utility. 

Author: Luke Campbell <luke.s.campbell@gmail.com>

HOWTO
-----

To begin using elasticpy start by importing.
    import elasticpy

To interface with the elasticsearch server use the ElasticSearch object.

    search = elasticpy.ElasticSearch()

To form a query use the ElasticQuery objects.

    query = elasticpy.ElasticQuery().term('users':'luke')
    # and then pass it to the search object
    search.search_advanced('twitter','feeds',query)
    >   {u'_shards': {u'failed': 0, u'successful': 5, u'total': 5},
         u'hits': {u'hits': [{u'_id': u'1',
            u'_index': u'twitter',
            u'_score': 0.30685282,
            u'_source': {u'content': u'This is an example.', u'user': u'luke'},
            u'_type': u'feeds'}],
          u'max_score': 0.30685282,
          u'total': 1},
         u'timed_out': False,
         u'took': 3}

USAGE
-----

* Simple Searching, queries ElasticSearch using GET on a url based query.
    
        search_simple(index, type, key, search_term)

* Advanced Searching, queries to ElasticSearch using a GET method and passing a JSON object containing the detailed query parameters, typically assembled by using an ElasticQuery object.

        search_advanced(index, type, query)

* Searching an index, the entails searching the entire index and all the types within.

        search_index_simple(index, key, search_term)
        search_index_advanced(index, query)

* Queries closely match the query types specified by [QueryDSL](http://www.elasticsearch.org/guide/reference/query-dsl/) used in ElasticSearch. They are wrapped in python methods to make them creation of the objects easier to manage than JSON strings.

        query = elasticpy.ElasticQuery().query_string(query='any')
        query
        >  {'query_string': {'allow_leading_wildcard': True,
          'analyze_wildcard': None,
          'auto_generate_phase_queries': False,
          'boost': 1.0,
          'default_field': '_all',
          'default_operator': 'OR',
          'enable_position_increments': True,
          'fuzzy_min_sim': 0.5,
          'fuzzy_prefix_length': 0,
          'lowercase_expanded_terms': True,
          'phrase_slop': 0,
          'query': 'any'}}

* Filters also closely match the [QueryDSL](http://www.elasticsearch.org/guide/reference/query-dsl/) just like query.

        filter = elasticpy.ElasticFilter().term('user','luke').range('age',21,26)
        filter
        > {'range': {'age': {'from': 18,
           'include_lower': True,
           'include_upper': False,
           'to': 25}},
         'term': {'user': 'luke'}}


Copying
-----------

   elasticpy - Python Wrapper for ElasticSearch
   
   Copyright 2012 UC Regents

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
