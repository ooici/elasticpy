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

        if field is None or from_value is None or to_value is None: return
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

