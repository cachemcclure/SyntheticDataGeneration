import os


os.chdir('..')


import pandas as pd
from utils import generate_tables
from utils import get_schema
from pyspark.sql.types import IntegerType, FloatType, StringType, DoubleType, BooleanType


test = generate_tables.generate_test_hail_table()

test.show()

primary_row_key = {'field_name':'row_id',
                   'data_type':StringType(),
                   'value_list':['pig','cat','dog','cow','horse','sheep','duck','axolotl']}

primary_col_key = {'field_name':'col_id',
                   'data_type':StringType(),
                   'value_list':['Lincoln','Washington','Jackson','Madison','Monroe','Jefferson']}

#row_fields = [{'field_name':'row_1',
#               'data_type':FloatType()},
#              {'field_name':'row_2',
#               'data_type':IntegerType(),
#               'value_list':[0,1,2]},
#              {'field_name':'row_3',
#               'data_type':BooleanType()},
#              {'field_name':'row_4',
#               'data_type':DoubleType()}]

#col_fields = [{'field_name':'col_1',
#               'data_type':StringType(),
#               'value_list':['m','f']},
#              {'field_name':'col_2',
#               'data_type':IntegerType(),
#               'field_params':{'distribution':'exponential',
#                               'scale':25,
#                               'minValue':0,
#                               'maxValue':1000}},
#              {'field_name':'col_3',
#               'data_type':IntegerType()}]

entry_fields = [{'field_name':'entry_1',
                 'data_type':FloatType(),
                 'field_params':{'distribution':'normal',
                                 'mean':25.0,
                                 'stddev':17,
                                 'minValue':0,
                                 'maxValue':100}},
                {'field_name':'entry_2',
                 'data_type':FloatType(),
                 'field_params':{'distribution':'Gamma',
                                 'shape':1.0,
                                 'scale':2.0,
                                 'minValue':25.0,
                                 'maxValue':2500.0}},
                {'field_name':'entry_3',
                 'data_type':FloatType(),
                 'field_params':{'weight_list':[1,5,25],
                                 'value_list':[2,1,0]}},
                {'field_name':'entry_4',
                 'data_type':FloatType()},
                {'field_name':'entry_5',
                 'data_type':IntegerType(),
                 'value_list':[0,1,2]},
                {'field_name':'entry_6',
                 'data_type':BooleanType()},
                {'field_name':'entry_7',
                 'data_type':DoubleType()},
                {'field_name':'entry_8',
                 'data_type':StringType(),
                 'value_list':['m','f']},
                {'field_name':'entry_9',
                 'data_type':IntegerType(),
                 'field_params':{'distribution':'exponential',
                                 'scale':25,
                                 'minValue':0,
                                 'maxValue':1000}},
                {'field_name':'entry_0',
                 'data_type':IntegerType()}]

#test = generate_tables.generate_test_hail_table(primary_row_key=primary_row_key,
#                                                primary_col_key=primary_col_key,
#                                                row_fields=row_fields,
#                                                col_fields=col_fields,
#                                                entry_fields=entry_fields)

test = generate_tables.generate_test_hail_table(primary_row_key=primary_row_key,
                                                primary_col_key=primary_col_key,
                                                entry_fields=entry_fields)

test.show(n_cols=2)
vl = dict(test.entry)
print(vl)
