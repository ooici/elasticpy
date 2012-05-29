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

        if field is None or from_value is None or to_value is None: return
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


