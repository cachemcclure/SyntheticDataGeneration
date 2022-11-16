import dbldatagen as dg
import dbldatagen.distributions as dist
from pyspark.sql.types import IntegerType, FloatType, StringType, BooleanType, DoubleType
from pyspark.sql import SparkSession
import hail as hl


def generate_identity_table(spark_session=None,
                            identity_name='id',
                            identity_type=StringType(),
                            identity_values=[],
                            no_of_values=0):
    """
    DEPRECATED
    :param spark_session: PySpark session (default None)
    :param identity_name: String name of column
    :param identity_type: PySpark datatype e.g., StringType()
    :param identity_values: Distinct list of values e.g., ['abc1','xyz2','emt99']
    :param no_of_values: Number of values to be generated
    :return: Returns a PySpark dataframe
    """
    if spark_session is None:
        spark_session = SparkSession.builder.master("local[1]").appName("Test Spark Session").getOrCreate()
    if (len(identity_values) == no_of_values) and (no_of_values == 0):
        raise Exception ('INPUT ERROR: Either number of values OR a list of identity values must be provided')
    elif (len(identity_values) != 0) and (no_of_values == 0):
        no_of_values = len(identity_values)
        ident_data = (dg.DataGenerator(sparkSession=spark_session,
                                       name=identity_name,
                                       rows=no_of_values).withColumn(colName=identity_name,
                                                                     colType=identity_type,
                                                                     values=identity_values)
                      )
    elif (no_of_values != 0) and (len(identity_values) == 0):
        ident_data = (dg.DataGenerator(sparkSession=spark_session,
                                       name=identity_name,
                                       rows=no_of_values).withColumn(colName=identity_name,
                                                                     colType=identity_type)
                      )
    else:
        raise Exception('OUTPUT ERROR: something somewhere is missing')
    df = ident_data.build()
    return df


def add_column_to_dg(df_spec,field):
    """
    :param df_spec: dbldatagen table spec
    :param field: field definition
    :return: returns dbldatagen table spec
    """
    if ('field_name' in field) and ('data_type' in field):
        if 'value_list' in field:
            df_spec.withColumn(colName=field['field_name'],
                               colType=field['data_type'],
                               values=field['value_list'],
                               random=True)
        elif 'field_params' in field:
            if 'distribution' in field['field_params']:
                if field['field_params']['distribution'].lower() == 'normal':
                    if ('mean' not in field['field_params']) or ('stddev' not in field['field_params']) or \
                            ('maxValue' not in field['field_params']) or ('minValue' not in field['field_params']):
                        v_mean = 0
                        v_stddev = 1
                    else:
                        v_mean = field['field_params']['mean']
                        v_stddev = field['field_params']['stddev']
                    df_spec.withColumn(colName=field['field_name'],
                                       colType=field['data_type'],
                                       minValue=field['field_params']['minValue'],
                                       maxValue=field['field_params']['maxValue'],
                                       random=True,
                                       distribution=dist.Normal(mean=v_mean,stddev=v_stddev))
                #                distribution = dist.Normal(mean=v_mean,stddev=v_stddev)
                elif field['field_params']['distribution'].lower() == 'beta':
                    if ('alpha' not in field['field_params']) or ('beta' not in field['field_params']) or \
                            ('maxValue' not in field['field_params']) or ('minValue' not in field['field_params']):
                        v_alpha = 0
                        v_beta = 1
                    else:
                        v_alpha = field['field_params']['alpha']
                        v_beta = field['field_params']['beta']
                    df_spec.withColumn(colName=field['field_name'],
                                       colType=field['data_type'],
                                       minValue=field['field_params']['minValue'],
                                       maxValue=field['field_params']['maxValue'],
                                       random=True,
                                       distribution=dist.Beta(alpha=v_alpha,beta=v_beta))
                #                distribution = dist.Beta(alpha=v_alpha,beta=v_beta)
                elif field['field_params']['distribution'].lower() == 'gamma':
                    if ('shape' not in field['field_params']) or ('scale' not in field['field_params']) or \
                            ('maxValue' not in field['field_params']) or ('minValue' not in field['field_params']):
                        v_shape = 0
                        v_scale = 1
                    else:
                        v_shape = field['field_params']['shape']
                        v_scale = field['field_params']['scale']
                    df_spec.withColumn(colName=field['field_name'],
                                       colType=field['data_type'],
                                       minValue=field['field_params']['minValue'],
                                       maxValue=field['field_params']['maxValue'],
                                       random=True,
                                       distribution=dist.Gamma(shape=v_shape,scale=v_scale))
                #                distribution = dist.Gamma(shape=v_shape,scale=v_scale)
                elif field['field_params']['distribution'].lower() == 'exponential':
                    if ('rate' not in field['field_params']) or \
                            ('maxValue' not in field['field_params']) or ('minValue' not in field['field_params']):
                        v_rate = 10
                    else:
                        v_rate = field['field_params']['rate']
                    df_spec.withColumn(colName=field['field_name'],
                                       colType=field['data_type'],
                                       minValue=field['field_params']['minValue'],
                                       maxValue=field['field_params']['maxValue'],
                                       random=True,
                                       distribution=dist.Exponential(rate=v_rate))
                #                distribution = dist.Exponential(rate=v_rate)
                else:
                    raise Exception('INPUT ERROR: Unrecognized distribution type')
            elif ('weight_list' in field['field_params']) and ('value_list' in field['field_params']):
                weight_list = field['field_params']['weight_list']
                value_list = field['field_params']['value_list']
                if len(weight_list) != len(value_list):
                    raise Exception('VALUE ERROR: number of weights must match number of values')
                df_spec.withColumn(colName=field['field_name'],
                                   colType=field['data_type'],
                                   values=value_list,
                                   weights=weight_list)
        else:
            df_spec.withColumn(colName=field['field_name'],
                               colType=field['data_type'],
                               random=True)
    return df_spec


def generate_test_data_table(table_name:str,
                             spark_session=None,
                             cols=[{'field_name':'id',
                                    'data_type':StringType()}],
                             no_of_rows:int=0,
                             primary_key:dict={
                                 'field_name':'id',
                                 'data_type':StringType(),
                                 'value_list':[]
                             },
                             foreign_keys:list=[]):
    """
    :param table_name: String, name of table
    :param spark_session: PySpark session (default None)
    :param cols: List of dictionaries (JSON format), one dictionary per column in the test data ***NOTE***:
                            use the field_params sub-dictionary for an individual field to define a value distribution.
                            Options are:
                                - Normal (reqs are mean and stddev; also known as Gaussian distribution)
                                - Beta (reqs are alpha and beta)
                                - Gamma (reqs are shape and scale)
                                - Exponential (req is rate)
    :param no_of_rows: Number of rows of test data to be generated
    :param primary_key: Primary (join) key for this table
    :param foreign_keys: Foreign (join) keys to join this table to others
    :return: Returns a PySpark dataframe
    """
    if spark_session is None:
        try:
            hl.init()
        except Exception as err:
            print('Hail session already initialized')
#        sc = hl.spark_context()
        spark_session = SparkSession.builder.getOrCreate()
#        spark_session = SparkSession.builder.master("local[1]").appName("Test Spark Session").getOrCreate()
    if len(foreign_keys) > 0:
        foreign_key_lst = [col['field_name'] for col in foreign_keys]
    else:
        foreign_key_lst = []
    non_key_cols = [col for col in cols if (col['field_name'] not in foreign_key_lst)
                    and (col['field_name'] != primary_key['field_name'])]
##    print(non_key_cols)
    if len(primary_key['value_list']) > 0:
        df_spec = dg.DataGenerator(sparkSession=spark_session,
                                   name=f'test_{table_name}').withColumn(colName=primary_key['field_name'],
                                                                         colType=primary_key['data_type'],
                                                                         values=primary_key['value_list'])
    else:
        df_spec = dg.DataGenerator(sparkSession=spark_session,
                                   name=f'test_{table_name}').withColumn(colName=primary_key['field_name'],
                                                                         colType=primary_key['data_type'])
    for col in foreign_keys:
        add_column_to_dg(df_spec,col)
    for col in non_key_cols:
        add_column_to_dg(df_spec,col)
    if no_of_rows > 0:
        df = df_spec.build().limit(int(no_of_rows))
    else:
        df = df_spec.build()
    return df


def generate_test_hail_table(table_name:str='hail_table',
                             no_of_rows=0,
                             primary_row_key:dict={'field_name':'row_id',
                                                   'data_type':StringType(),
                                                   'value_list':['a','b','c','d']},
                             primary_col_key:dict={'field_name':'col_id',
                                                   'data_type':StringType(),
                                                   'value_list':['z','y','x','w']},
                             spark_session=None,
                             row_fields:list=[],
                             col_fields:list=[],
                             entry_fields:list=[{'field_name':'entry_field',
                                                 'data_type':FloatType(),
                                                 'field_params':{'distribution':'normal',
                                                                 'mean':50,
                                                                 'stddev':7,
                                                                 'maxValue':100,
                                                                 'minValue':0.0}}]):
    """
    :param table_name: not really necessary; holdover from other fx
    :param no_of_rows: number of rows to be generated in the Hail table prior to matrix generation
    :param primary_row_key: field definition for generation of the primary row key for the Hail matrix
    :param primary_col_key: field definition for generation of the primary row key for the Hail matrix
    :param spark_session: PySpark session; if None is provided, then a Hail session is instantiated
    :param row_fields: DEPRECATED - please use primary_row_key; any fields passed to this arg will be created as entries
    :param col_fields: DEPRECATED - please use primary_col_key; any fields passed to this arg will be created as entries
    :param entry_fields: List of dictionary objects defining the entry fields for the Hail table. ***NOTE***:
                            use the field_params dictionary for an individual field to define a value distribution.
                            Options are (maxValue and minValue are required for all):
                                - Normal (reqs are mean and stdev; also known as Gaussian distribution)
                                - Beta (reqs are alpha and beta)
                                - Gamma (reqs are shape and scale)
                                - Exponential (req is rate)
    :return: Returns a Hail matrix
    """
    if spark_session is None:
        try:
            hl.init()
        except Exception as err:
            print('Hail session already initialized')
        spark_session = SparkSession.builder.getOrCreate()
#    if len(primary_row_key) > 1:
#        raise Exception('INPUT ERROR: only one primary row key')
    if 'field_name' not in primary_row_key:
        primary_row_key['field_name'] = 'row_id'
    if 'data_type' not in primary_row_key:
        primary_row_key['data_type'] = StringType()
    if 'value_list' not in primary_row_key:
        primary_row_key['value_list'] = ['a','b','c','d']
#    if len(primary_col_key) > 1:
#        raise Exception('INPUT ERROR: only one primary col key')
    if 'field_name' not in primary_col_key:
        primary_col_key['field_name'] = 'col_id'
    if 'data_type' not in primary_col_key:
        primary_col_key['data_type'] = StringType()
    if 'value_list' not in primary_col_key:
        primary_col_key['value_list'] = ['z','y','x','w']
#    no_of_rows = int(len(primary_row_key) * len(primary_col_key))
    df_spec = dg.DataGenerator(sparkSession=spark_session,
                               name=f'test_{table_name}')
    df_spec.withColumn(colName=primary_row_key['field_name'],
                       colType=primary_row_key['data_type'],
                       values=primary_row_key['value_list'],
                       random=True)
    df_spec.withColumn(colName=primary_col_key['field_name'],
                       colType=primary_col_key['data_type'],
                       values=primary_col_key['value_list'],
                       random=True)
    row_field_list = []
    col_field_list = []
    for field in row_fields:
        add_column_to_dg(df_spec=df_spec,field=field)
    for field in col_fields:
        add_column_to_dg(df_spec=df_spec,field=field)
    for field in entry_fields:
        add_column_to_dg(df_spec=df_spec,field=field)
    if no_of_rows > 0:
        df = df_spec.build().limit(int(no_of_rows))
    else:
        df = df_spec.build()
    df2 = hl.Table.from_spark(df).to_matrix_table(row_key=[primary_row_key['field_name']],
                                                  col_key=[primary_col_key['field_name']],
                                                  row_fields=row_field_list,
                                                  col_fields=col_field_list)
#    df = hl.Table.from_spark(df_spec.build()).to_matrix_table(row_key=[primary_row_key['field_name']],
#                                                              col_key=[primary_col_key['field_name']],
#                                                              row_fields=row_field_list,
#                                                              col_fields=col_field_list)
    return df2
