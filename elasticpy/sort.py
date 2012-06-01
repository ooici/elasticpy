#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file connection
@date 05/24/12 09:29
@description Connection Class for elasticpy
'''

class ElasticSort(list):
    def sort(self, field, order='asc'):
        self.append({field: { 'order' : order }})
        return self

    def missing(self, field, last=True):
        '''
        Numeric fields support specific handling for missing fields in a doc. The missing value can be _last, _first, or a custom value (that will be used for missing docs as the sort value).

        missing('price')
        > {"price" : {"missing": "_last" } }
        missing('price',False)
        > {"price" : {"missing": "_first"} }
        '''
        if last:
            self.append({field : {'missing':'_last'}})
        else:
            self.append({field:{'missing':'_first'}})

        return self

    def ignore_unmapped(self, field):
        self.append({field : { 'ignore_unmapped' : True }})
        return self

    def geo_distance(self, field, location, unit, order='asc'):
        self.append( { '_geo_distance' : {
            field : location,
            'unit' : unit,
            'order' : order
        }})
        return self

    def script(self, script, field_type, params={}, order='asc'):

        self.append({'_script': {
            'script' : script,
            'type' : field_type,
            'params' : params,
            'order' : order
        }})
        return self

    def track_scores(self):
        self.append({'track_scores' : True })

        return self
