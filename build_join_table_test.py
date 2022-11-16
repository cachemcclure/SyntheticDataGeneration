import os


os.chdir('..')


import pandas as pd
from utils import generate_tables
from utils import get_schema
from pyspark.sql.types import IntegerType, FloatType, StringType, DoubleType


class test_case():
    def __init__(self):
        self.alt_id = 1
        self.fk = 'apple'
        self.bool_test = True
        self.int_fk = 10000
        self.str_fk = 'invoke'
        self.float_test = 1.0515
        self.dbl_test = 99999999999999999999999999999999
        self.int_test = 1
        self.str_test = 'a'


test_data = test_case()

schema_object = get_schema.return_schema(schema_object=test_data)

primary_key = {'field_name':'alt_id','data_type':StringType(),'value_list':['a','b','c','d']}
foreign_keys = [
    {
        'field_name':'str_fk',
        'data_type':StringType(),
        'value_list':['alpha',
                      'beta',
                      'delta',
                      'epsilon',
                      'gamma']
    }
]

test = generate_tables.generate_test_data_table(table_name='test',
                                                primary_key=primary_key,
                                                foreign_keys=foreign_keys,
                                                no_of_rows=100,
                                                cols=schema_object)
test.show()

join_table_schema = [
    {
        'field_name':'join_1',
        'data_type':StringType()
    },
    {
        'field_name':'join_2',
        'data_type':IntegerType()
    },
    {
        'field_name':'str_fk',
        'data_type':StringType()
    }
    ]

temp = generate_tables.generate_test_data_table(table_name='test',
                                                primary_key=foreign_keys[0],
                                                no_of_rows=1000,
                                                cols=join_table_schema)
temp.show()

out = test.join(temp,test.str_fk == temp.str_fk,how='inner')
out.show()
