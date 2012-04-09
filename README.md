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
* ElasticSearch

  - search_simple 


Copying
-----------
   Copyright 2012 Lucas Campbell

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
