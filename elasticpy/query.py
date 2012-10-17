#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file query
@date 05/24/12 09:31
@description Query class for ElasticSearch
'''


class ElasticQuery(dict):
    '''
    Wrapper for ElasticSearch queries.
    '''

    @classmethod
    def term(cls,**kwargs):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/term-query.html
        Matches documents that have fields that contain a term (not analyzed). The term query maps to Lucene TermQuery
        The following matches documents where the user field contains the term 'kimchy':
        > term = ElasticQuery.term(user='kimchy')
        > term.query()
          {'term' : {'user':'kimchy'}}

        '''
        return cls(term=dict(**kwargs))

    @classmethod
    def terms(cls, tags, minimum_match=None):
        '''
        A query that match on any (configurable) of the provided terms. This is a simpler syntax query for using a bool query with several term queries in the should clauses. For example:

        {
            "terms" : {
                "tags" : [ "blue", "pill" ],
                "minimum_match" : 1
            }
        }'''
        instance = cls(terms={'tags':tags})
        if minimum_match is not None: instance['terms']['minimum_match'] = minimum_match
        return instance

   
    @classmethod
    def field(cls, field, query, boost=None, enable_position_increments=None):
        '''
        A query that executes a query string against a specific field. It is a simplified version of query_string query (by setting the default_field to the field this query executed against). In its simplest form:

        {
            "field" : { 
                "name.first" : "+something -else"
            }
        }
        Most of the query_string parameters are allowed with the field query as well, in such a case, the query should be formatted as follows:

        {
            "field" : { 
                "name.first" : {
                    "query" : "+something -else",
                    "boost" : 2.0,
                    "enable_position_increments": false
                }
            }
        }
        '''
        instance = cls(field={field:{'query':query}})
        if boost is not None: instance['field']['boost'] = boost
        if enable_position_increments is not None: instance['field']['enable_position_increments'] = enable_position_increments
        return instance

    @classmethod
    def text(cls, field, query, operator='or'):
        '''
        text query has been deprecated (effectively renamed) to match query since 0.19.9, please use it. text is still supported.
        '''

        raise NotImplementedError('Deprecated')

    @classmethod
    def match(cls,field, query, operator=None):
        '''
        A family of match queries that accept text/numerics/dates, analyzes it, and constructs a query out of it. For example:

        {
            "match" : {
                "message" : "this is a test"
            }
        }
        Note, message is the name of a field, you can subsitute the name of any field (including _all) instead.
        '''
        instance = cls(match={field:{'query':query}})
        if operator is not None: instance['match'][field]['operator'] = operator


        return instance
    
    @classmethod
    def bool(cls, must=None, should=None, must_not=None, minimum_number_should_match=None, boost=None):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/bool-query.html
        A query that matches documents matching boolean combinations of other queris. The bool query maps to Lucene BooleanQuery. It is built using one of more boolean clauses, each clause with a typed occurrence. The occurrence types are:
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
        instance = cls(bool={})
        if must is not None: instance['bool']['must']=must
        if should is not None: instance['bool']['should']=should
        if must_not is not None: instance['bool']['must_not']=must_not
        if minimum_number_should_match is not None: instance['bool']['minimum_number_should_match']=minimum_number_should_match
        if boost is not None: instance['bool']['boost']=boost

        return instance

    @classmethod
    def ids(cls, values=None, itype=None):
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
        instance = cls(ids={})
        if values is not None: instance['ids']['values']=values
        if itype is not None: instance['ids']['type']=itype
        return instance

    @classmethod
    def fuzzy(cls, field, value, boost=None, min_similarity=None, prefix_length=None):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/fuzzy-query.html
        A fuzzy based query that uses similarity based on Levenshtein (edit distance) algorithm.
        '''
        instance = cls(fuzzy={field:{'value':value}})
        if boost is not None: instance['fuzzy'][field]['boost'] = boost
        if min_similarity is not None: instance['fuzzy'][field]['min_similarity'] = min_similarity
        if prefix_length is not None: instance['fuzzy'][field]['prefix_length'] = prefix_length
        return instance

    @classmethod
    def fuzzy_like_this(cls, like_text, fields=None, ignore_tf=None, max_query_terms=None, min_similarity=None, prefix_length=None, boost=None, analyzer=None):
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
        instance = cls(fuzzy_like_this={'like_text':like_text})
        if fields is not None: instance['fuzzy_like_this']['fields'] = fields
        if ignore_tf is not None: instance['fuzzy_like_this']['ignore_tf'] = ignore_tf
        if max_query_terms is not None: instance['fuzzy_like_this']['max_query_terms'] = max_query_terms
        if min_similarity is not None: instance['fuzzy_like_this']['min_similarity'] = min_similarity
        if prefix_length is not None: instance['fuzzy_like_this']['prefix_length'] = prefix_length
        if boost is not None: instance['fuzzy_like_this']['boost'] = boost
        if analyzer is not None: instance['fuzzy_like_this']['analyzer'] = analyzer

        return instance

    @classmethod
    def has_child(cls, child_type, query):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/has-child-query.html
        The has_child query accepts a query and the child type to run against, and results in parent documents that have child docs matching the query.

        > child_query = ElasticQuery().term(tag='something')
        > query = ElasticQuery().has_Child('blog_tag', child_query)
        '''

        instance = cls(has_child={'type':child_type, 'query':query})
        return instance

    @classmethod
    def match_all(cls):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/match-all-query.html
        A query that matches all documents. Maps to Lucene MatchAllDocsQuery
        '''

        return cls(match_all={})
    
    @classmethod
    def mlt(cls, like_text, fields=None,percent_terms_to_match=None, min_term_freq=None, max_query_terms=None, stop_words=None, min_doc_freq=None, max_doc_freq=None, min_word_len=None, max_word_len=None, boost_terms=None, boost=None, analyzer=None):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/mlt-query.html
        More like this query find documents that are "like" provided text by running it against one or more fields.
        > query = ElasticQuery().mlt('text like this one', fields=['post.content'])
        '''

        instance = cls(more_like_this={'like_text':like_text})
        if fields is not None: instance['more_like_this']['fields'] = fields
        if percent_terms_to_match is not None: instance['more_like_this']['percent_terms_to_match'] = percent_terms_to_match
        if min_term_freq is not None: instance['more_like_this']['min_term_freq'] = min_term_freq
        if max_query_terms is not None: instance['more_like_this']['max_query_terms'] = max_query_terms
        if stop_words is not None: instance['more_like_this']['stop_words'] = stop_words
        if min_doc_freq is not None: instance['more_like_this']['min_doc_freq'] = min_doc_freq
        if max_doc_freq is not None: instance['more_like_this']['max_doc_freq'] = max_doc_freq
        if min_word_len is not None: instance['more_like_this']['min_word_len'] = min_word_len
        if max_word_len is not None: instance['more_like_this']['max_word_len'] = max_word_len
        if boost_terms is not None: instance['more_like_this']['boost_terms'] = boost_terms
        if boost is not None: instance['more_like_this']['boost'] = boost
        if analyzer is not None: instance['more_like_this']['analyzer'] = analyzer
        return instance

    @classmethod
    def prefix(cls, **kwargs):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/prefix-query.html
        Matches documents that have fields containing terms with a specified prefix (not analyzed). The prefix query maps to Lucene PrefixQuery.
        Example, the following finds a document where the user field contains a temr that starts with lu
        > query = ElasticQuery().prefix('user', 'lu')
        '''
        return cls(prefix=dict(**kwargs))
    
    @classmethod
    def query_string(cls,
                     query,
                     default_field=None,
                     default_operator=None,
                     analyzer=None,
                     allow_leading_wildcard=None,
                     lowercase_expanded_terms=None,
                     enable_position_increments=None,
                     fuzzy_prefix_length=None,
                     fuzzy_min_sim=None,
                     phrase_slop=None,
                     boost=None,
                     analyze_wildcard=None,
                     auto_generate_phrase_queries=None,
                     minimum_should_match=None):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/query-string-query.html
        A query that uses a query parser in order to parse its content.

        > query = ElasticQuery().query_string('this AND that OR thus', default_field='content')
        '''
        instance = cls(query_string={'query':query})
        if default_field is not None: instance['query_string']['default_field'] = default_field
        if default_operator is not None: instance['query_string']['default_operator'] = default_operator
        if analyzer is not None: instance['query_string']['analyzer'] = analyzer
        if allow_leading_wildcard is not None: instance['query_string']['allow_leading_wildcard'] = allow_leading_wildcard
        if lowercase_expanded_terms is not None: instance['query_string']['lowercase_expanded_terms'] = lowercase_expanded_terms
        if enable_position_increments is not None: instance['query_string']['enable_position_increments'] = enable_position_increments
        if fuzzy_prefix_length is not None: instance['query_string']['fuzzy_prefix_length'] = fuzzy_prefix_length
        if fuzzy_min_sim is not None: instance['query_string']['fuzzy_min_sim'] = fuzzy_min_sim
        if phrase_slop is not None: instance['query_string']['phrase_slop'] = phrase_slop
        if boost is not None: instance['query_string']['boost'] = boost
        if analyze_wildcard is not None: instance['query_string']['analyze_wildcard'] = analyze_wildcard
        if auto_generate_phrase_queries is not None: instance['query_string']['auto_generate_phrase_queries'] = auto_generate_phrase_queries
        if minimum_should_match is not None: instance['query_string']['minimum_should_match'] = minimum_should_match
        return instance

    @classmethod
    def range(cls,
              field,
              from_value=None,
              to_value=None,
              include_lower=None,
              include_upper=None,
              boost=None):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/range-query.html
        Matches documents with fields that have terms within a certain range. The type of the Lucene query depends on the field type, for string fields, the TermRangeQuery, while for number/date fields, the query is a NumericRangeQuery. The following example returns all documents where age is between 10 and 20:

        > query = ElasticQuery().range('age', from_value=10, to_value=20, boost=2.0)
        '''
        instance = cls(range={field:{}})
        if from_value is not None: instance['range'][field]['from'] = from_value
        if to_value is not None: instance['range'][field]['to'] = to_value
        if include_lower is not None: instance['range'][field]['include_lower'] = include_lower
        if include_upper is not None: instance['range'][field]['include_upper'] = include_upper
        if boost is not None: instance['range'][field]['boost'] = boost
        return instance

    @classmethod
    def wildcard(cls, field, value):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/wildcard-query.html
        Matches documents that have fields matching a wildcard expression (not analyzed). Supported wildcards are *, which matches any character sequence (including the empty one), and ?, which matches any single character. Note this query can be slow, as it needs to iterate over many terms. In order to prevent extremely slow wildcard queries, a wildcard term should not start with one of the wildcards * or ?. The wildcard query maps to Lucene WildcardQuery.

        > query = ElasticQuery.wildcard('user', 'ki*y')
        '''
        return cls(wildcard={field:value})


