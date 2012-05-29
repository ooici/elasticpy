#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file map
@date 05/24/12 09:33
@description DESCRIPTION
'''


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
        Specifies that the field should be ignored in the index
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
        Boolean value if norms should be omitted or not. Defaults to False
        '''
        self[self.field]['omit_norms'] = should
        return self

    def omit_term_freq_and_positions(self, should=False):
        '''
        Boolean value if term freq and positions should be omitted. Defaults to false.
        '''
        self[self.field]['omit_term_freq_and_positions'] = should
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

