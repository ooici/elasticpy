elasticpy
===========
_"ElasticSearch for the rest of us"_

Python wrapper for the elasticsearch indexing utility. 

Author: [Luke Campbell](http://lukecampbell.github.com/) [<luke.s.campbell@gmail.com>](mailto:luke.s.campbell@gmail.com)

About
-----
The goal of this module is to provide an intuitive interface between Python APIs and the ElasticSearch API.  Connections are provided through the `requests` library so the connections are inherently thread safe and pooled.  

Writing complex queries in JSON can be tedius and time consuming if you need to constantly reference the Query DSL guide, so we've wrapped most of the operations in classes with methods corresponding to the parameters of the operation.  For example:

    sorts = ElasticSort()
    sorts.sort('name',order='asc')
    sorts.sort('_score')
    ----
    [{"name": {"order": "asc"}}, {"_score": {"order": "asc"}}]

Queries, Filters, Sorts, Maps and Facets are wrapped in convenience classes.  Searching is done through the ElasticSearch class.  

HOWTO
-----

To begin using elasticpy start by importing.

    import elasticpy as ep

To interface with the elasticsearch server use the ElasticSearch object.

    search = ep.ElasticSearch() # Defaults to localhost:9200

To form a query use the ElasticQuery wrapper objects.

    query = ep.ElasticQuery().term('users':'luke')

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
* Queries - `ElasticQuery`

        query = ElasticQuery()

  * Term Searches 

            query.term(name='luke')

  * Text Searches

            query.text('message', 'this is a test')

  * Text Phrases

            query.text_phrase('message', 'this is a test')

  * Fuzzy

            query.fuzzy('name','luke',boost=1.0)

  * Fuzzy Like This

            query.fuzzy_like_this('luke',fields='_all')

  * **Match All**

            query.match_all()

  * Wildcard

            query.wildcard('name','lu*')

* Filters - `ElasticFilter`

        filters = ElasticFilter()

  * And

            filters.and_filter(query)

  * Bool

            filters.bool_filter(must=query1, must_not=query2, should=query3)

  * Geo

     * Geo Distance

                filters.geo_distance('location',{'lat':30,'lon':30}, '20km')

     * Geo Bounding Box

                filters.geo_bounding_box('location', {'lat':60, 'lon':60}, {'lat':30, 'lon':30})

  * Match All

            filters.match_all()

  * Range

            filters.numeric_range('price',8.0, 9.9)



Copying
-----------

**elasticpy** - Python Wrapper for ElasticSearch

Copyright 2012 UC Regents

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
