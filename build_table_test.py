import pandas as pd
from utils import generate_tables
from utils import get_schema
from pyspark.sql.types import IntegerType, FloatType, StringType, DoubleType

test_data = pd.DataFrame([[1,'string',False,1000,'active',1.0515,99999999999999999999999999999999,1,'a'],
                          [2,'abc',True,1000,'active',2.00,99999999999999999999999999999999,1,'a']],
                         columns=['alt_id','fk','bool_test','int_fk','str_fk','float_test','dbl_test',
                                  'int_test','str_test'])

schema_object = get_schema.return_schema(schema_object=test_data)
##print(schema_object)

primary_key = {'field_name':'alt_id','data_type':StringType(),'value_list':['a','b','c','d']}
foreign_keys = [
    {
        'field_name':'fk',
        'data_type':StringType(),
        'value_list':['chicken',
                      'bear',
                      'pig',
                      'horse',
                      'elk',
                      'moose']
        },
    {
        'field_name':'int_fk',
        'data_type':IntegerType(),
        'value_list':[1,
                      2,
                      3,
                      4,
                      5]
    },
    {
        'field_name':'str_fk',
        'data_type':StringType()
        }
    ]

test = generate_tables.generate_test_data_table(table_name='test',
                                                primary_key=primary_key,
                                                foreign_keys=foreign_keys,
                                                no_of_rows=100,
                                                cols=schema_object)
test.show()

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
##print(schema_object)

test = generate_tables.generate_test_data_table(table_name='test2',
                                                primary_key=primary_key,
                                                foreign_keys=foreign_keys,
                                                no_of_rows=10,
                                                cols=schema_object)
test.show()

schema_object = get_schema.return_schema(schema_object=test)
##print(schema_object)

test = generate_tables.generate_test_data_table(table_name='test3',
                                                primary_key=primary_key,
                                                foreign_keys=foreign_keys,
                                                no_of_rows=1000000,
                                                cols=schema_object)

test.show()

#test = generate_tables.generate_test_hail_table()

#test.show()
