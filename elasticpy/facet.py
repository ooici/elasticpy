#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file facet
@date 05/24/12 09:32
@description facets
'''



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

