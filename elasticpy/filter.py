#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file filter
@date 05/24/12 09:32
@description Filters for searching
'''


class ElasticFilter(dict):

    '''
    Wrapper for ElasticSearch filters
    '''

    @classmethod
    def and_filter(cls, query):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/and-filter.html
        A filter that matches documents using AND boolean operator on other queries. This filter is more performant then bool filter. Can be placed within queries that accept a filter.
        '''
        return cls({'and': query})

    @classmethod
    def bool_filter(cls, query):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/bool-filter.html
        A filter that matches documents matching boolean combinations of other queries. Similar in concept to Boolean query, except that the clauses are other filters. Can be placed within queries that accept a filter.
        '''

        return cls({'bool': query})

    @classmethod
    def exists(cls, field):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/exists-filter.html
        Filters documents where a specific field has a value in them.
        > filter = elasticpy.ElasticFilter().exists(field='user')
        > filter.query()
          {'exists' : {'field' : 'user' } }
        '''
        return cls(exists={'field': field})

    @classmethod
    def ids(cls, values, itype=None):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/ids-filter.html
Filters documents that only have the provided ids. Note, this filter does not require the _id field to be indexed since it works using the _uid field.

        '''
        instance = cls(ids={'values': values})
        if itype is not None:
            instance['ids']['type'] = itype

        return instance

    @classmethod
    def limit(cls, value):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/limit-filter.html
        A limit filter limits the number of documents (per shard) to execute on.
        '''
        return cls(limit={'value': value})

    @classmethod
    def type(cls, value):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/type-filter.html
Filters documents matching the provided document / mapping type. Note, this filter can work even when the _type field is not indexed (using the _uid field).
        '''
        return cls(type={'value': value})

    @classmethod
    def geo_bounding_box(cls, field, top_left, bottom_right):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/geo-bounding-box-filter.html

        > bounds = ElasticFilter().geo_bounding_box('pin.location', [40.73, -74.1], [40.717, -73.99])
        > bounds = ElasticFilter().geo_bounding_box('pin.location', dict(lat=40.73, lon=-74.1), dict(lat=40.717, lon=-73.99))
        > bounds = ElasticFilter().geo_bounding_box('pin.location', "40.73, -74.1", "40.717, -73.99")
        And geohash
        > bounds = ElasticFilter().geo_bounding_box('pin.location', "drm3btev3e86", "drm3btev3e86")

        '''
        return cls(geo_bounding_box={field: {'top_left': top_left, 'bottom_right': bottom_right}})

    @classmethod
    def geo_distance(cls, field, center, distance, distance_type=None):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/geo-distance-filter.html
        Filters documents that include only hits that exists within a specific distance from a geo point.
        field - Field name
        center - Center point (Geo point)
        distance - String for the distance
        distance_type - (arc | plane) How to compute the distance. Can either be arc (better precision) or plane (faster). Defaults to arc
        > bounds = ElasticFilter().geo_distance('pin.location', [40.73, -74.1], '300km')
        '''

        instance = cls(geo_distance={'distance': distance, field: center})
        if distance_type is not None:
            instance['geo_distance']['distance_type'] = distance_type
        return instance

    @classmethod
    def geo_distance_range(cls, field, center, from_distance, to_distance, distance_type=None):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/geo-distance-range-filter.html
        Filters documents that exists within a range from a specific point


        '''
        instance = cls(geo_distance_range={'from': from_distance, 'to': to_distance, field: center})
        if distance_type is not None:
            instance['geo_distance_range']['distance_type'] = distance_type
        return instance

    @classmethod
    def geo_polygon(cls, field, points):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/geo-polygon-filter.html
        A filter allowing to include hits that only fall within a polygon of points.
        Points are in geo-point format

        > filter = ElasticFilter().geo_polygon('pin.location', [[40, -70], [30, -80], [20, -90]])
        '''
        return cls(geo_polygon={field: {'points': points}})

    @classmethod
    def has_child(cls, child_type, query):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/has-child-filter.html
        The has_child filter accepts a query and the child type to run against, and results in parent documents that have child docs matching the query.

        > query = ElasticQuery().term(tag='something')
        > filter = ElasticFilter().has_child('blog_tag', query)

        '''

        return cls(has_child={'type': child_type, 'query': query})

    @classmethod
    def match_all(cls):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/match-all-filter.html
        A filter that matches on all documents.
        > filter = ElasticFilter().match_all()
        '''

        return cls(match_all={})

    @classmethod
    def missing(cls, field):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/missing-filter.html
        Filters documents where a specific field has no value in them.

        '''
        return cls(missing={'field': field})

    @classmethod
    def not_filter(cls, query):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/not-filter.html
        A filter that filters out matched documents using a query. This filter is more performant then bool filter. Can be placed within queries that accept a filter.

        '''
        return cls({'not': query})

    @classmethod
    def numeric_range(cls, field, from_value, to_value, include_lower=None, include_upper=None):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/numeric-range-filter.html
        Filters documents with fields that have values within a certain numeric range. Similar to range filter, except that it works only with numeric values, and the filter execution works differently.
        '''
        instance = cls(numeric_range={field: {'from': from_value, 'to': to_value}})
        if include_lower is not None:
            instance['numeric_range'][field]['include_lower'] = include_lower
        if include_upper is not None:
            instance['numeric_range'][field]['include_upper'] = include_upper
        return instance

    @classmethod
    def or_filter(cls, query):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/or-filter.html

        A filter that matches documents using OR boolean operator on other queries. This filter is more performant then bool filter. Can be placed within queries that accept a filter.

        > term1 = ElasticQuery().term(lastname='Campbell')
        > term2 = ElasticQuery().term(firstname='Luke')
        > filter = ElasticFilter().or_filter([term1, term2])
        '''

        return cls({'or': query})

    @classmethod
    def prefix(cls, field, pre):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/prefix-filter.html
        Filters documents that have fields containing terms with a specified prefix (not analyzed). Similar to phrase query, except that it acts as a filter. Can be placed within queries that accept a filter.
        '''

        return cls(prefix={field: pre})

    @classmethod
    def query(cls, query):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/query-filter.html
        Wraps any query to be used as a filter. Can be placed within queries that accept a filter.
        '''
        return cls(query=query)

    @classmethod
    def range(cls, field, from_value=None, to_value=None, include_lower=None, include_upper=None):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/range-filter.html

        Filters documents with fields that have terms within a certain range. Similar to range query, except that it acts as a filter. Can be placed within queries that accept a filter.
        '''

        instance = cls({'range': {field: {}}})
        if from_value is not None:
            instance['range'][field]['from'] = from_value
        if to_value is not None:
            instance['range'][field]['to'] = to_value
        if include_lower is not None:
            instance['range'][field]['include_lower'] = include_lower
        if include_upper is not None:
            instance['range'][field]['include_upper'] = include_upper

        return instance

    @classmethod
    def script(cls, script):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/script-filter.html
        A filter allowing to define scripts as filters.

        > script = 'doc["num1"].value > 1'
        > filter = ElasticFilter().script(script)
        '''
        return cls(script=script)

    @classmethod
    def term(cls, field, value):
        '''
        http://www.elasticsearch.org/guide/reference/query-dsl/term-filter.html
        Filters documents that have fields that contain a term (not analyzed). Similar to term query, except that it acts as a filter.
        '''

        return cls(term={field: value})
