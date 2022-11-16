import inspect
import numpy as np
import pandas as pd
from pyspark.sql.types import IntegerType, FloatType, StringType, BooleanType, DoubleType
from pyspark.sql import dataframe
import hail as hl


def implied_schema(implicit_dataframe):
    """
    :param implicit_dataframe: Pandas or Pyspark Dataframe that contains an example of the req'd output
    :return: Returns schema for required test data
    """
    out = []
    if type(implicit_dataframe) == pd.DataFrame:
        aa, bb = implicit_dataframe.shape
        if aa < 1:
            raise Exception('INPUT EXCEPTION: Supplied dataframe has no data')
        if bb < 1:
            raise Exception('INPUT EXCEPTION: Supplied dataframe has no columns')
        for col in implicit_dataframe:
            temp = {'field_name':col}
            if type(implicit_dataframe[col][0]) == np.bool_:
                temp['data_type'] = BooleanType()
            elif type(implicit_dataframe[col][0]) == np.float64:
                temp['data_type'] = FloatType()
            elif type(implicit_dataframe[col][0]) == np.int64:
                temp['data_type'] = IntegerType()
            elif type(implicit_dataframe[col][0]) == str:
                temp['data_type'] = StringType()
            elif type(implicit_dataframe[col][0]) == int:
                temp['data_type'] = IntegerType()
            else:
                raise Exception(f'PARSE ERROR: unable to impute data type for column {col}')
            out.append(temp)
    elif type(implicit_dataframe) == dataframe.DataFrame:
        aa = implicit_dataframe.count()
        bb = len(implicit_dataframe.columns)
        if aa < 1:
            raise Exception('INPUT EXCEPTION: Supplied dataframe has no data')
        if bb < 1:
            raise Exception('INPUT EXCEPTION: Supplied dataframe has no columns')
        for col, dt in implicit_dataframe.dtypes:
            temp = {'field_name':col}
            if dt == 'string':
                temp['data_type'] = StringType()
            elif dt == 'bigint':
                temp['data_type'] = DoubleType()
            elif dt == 'boolean':
                temp['data_type'] = BooleanType()
            elif dt == 'double':
                temp['data_type'] = DoubleType()  # TODO Should this be FloatType or DoubleType?
            elif dt == 'float':
                temp['data_type'] = FloatType()
            elif dt == 'int':
                temp['data_type'] = IntegerType()
            else:
                raise Exception(f'PARSE ERROR: unable to impute data type for column {col}')
            out.append(temp)
    elif isinstance(type(implicit_dataframe),hl.matrixtable.MatrixTable):
        entries = dict(implicit_dataframe.entry)
        col_key = dict(implicit_dataframe.col)
        row_key = dict(implicit_dataframe.row)
        for entry in entries:
            temp = {'field_name':entry}
            vtype = type(entries[entry])
            if isinstance(vtype,hl.expr.expressions.typed_expressions.Float32Expression):
                temp['data_type'] = FloatType()
            elif isinstance(vtype,hl.expr.expressions.typed_expressions.Int32Expression):
                temp['data_type'] = IntegerType()
            elif isinstance(vtype,hl.expr.expressions.typed_expressions.StringExpression):
                temp['data_type'] = StringType()
            elif isinstance(vtype,hl.expr.expressions.typed_expressions.BooleanExpression):
                temp['data_type'] = BooleanType()
            out.append(temp)
        for entry in col_key:
            temp = {'field_name':entry}
            vtype = type(entries[entry])
            if isinstance(vtype,hl.expr.expressions.typed_expressions.Float32Expression):
                temp['data_type'] = FloatType()
            elif isinstance(vtype,hl.expr.expressions.typed_expressions.Int32Expression):
                temp['data_type'] = IntegerType()
            elif isinstance(vtype,hl.expr.expressions.typed_expressions.StringExpression):
                temp['data_type'] = StringType()
            elif isinstance(vtype,hl.expr.expressions.typed_expressions.BooleanExpression):
                temp['data_type'] = BooleanType()
            out.append(temp)
        for entry in row_key:
            temp = {'field_name':entry}
            vtype = type(entries[entry])
            if isinstance(vtype,hl.expr.expressions.typed_expressions.Float32Expression):
                temp['data_type'] = FloatType()
            elif isinstance(vtype,hl.expr.expressions.typed_expressions.Int32Expression):
                temp['data_type'] = IntegerType()
            elif isinstance(vtype,hl.expr.expressions.typed_expressions.StringExpression):
                temp['data_type'] = StringType()
            elif isinstance(vtype,hl.expr.expressions.typed_expressions.BooleanExpression):
                temp['data_type'] = BooleanType()
            out.append(temp)
    else:
        raise Exception('INPUT EXCEPTION: Supplied dataframe of incorrect type')
    return out


def implied_class(implicit_class):
    """
    :param implicit_class: Python or imported Scala class that contains sample data (NOT TYPES)
    :return: Returns schema for required test data
    """
    out = []
    type_list = vars(implicit_class)
    for attr in type_list:
        temp = {'field_name':attr}
        if type(type_list[attr]) == int:
            temp['data_type'] = IntegerType()
        elif type(type_list[attr]) == str:
            temp['data_type'] = StringType()
        elif type(type_list[attr]) == float:
            temp['data_type'] = FloatType()
        elif type(type_list[attr]) == bool:
            temp['data_type'] = BooleanType()
        else:
            raise Exception(f'PARSE ERROR: unable to impute data type for column {attr}')
        out.append(temp)
    return out


def supplied_class(explicit_class):
    """
    NOTE: this particular function currently isn't supported due to issues calling a Scala class from Python
    There are plans to support this in the future
    :param explicit_class: Python or imported Scala class that defines the schema
    :return: Returns schema for required test data
    """
    temp = vars(explicit_class)
    out = [{'field_name':attr,'data_type':temp[attr]} for attr in temp]
    return out


def return_schema(schema_object, explicit=False):
    """
    :param schema_object: Class or dataframe that somehow defines the required test data
    :param explicit: Flag to differentiate between explicit or implicit schema definition
    :return: Returns dictionary of the schema columns and datatypes
    """
    if explicit is True:
        temp = supplied_class(schema_object)
    elif explicit is False:
        if (type(schema_object) == pd.DataFrame) or (type(schema_object) == dataframe.DataFrame):
            temp = implied_schema(schema_object)
        elif isinstance(type(schema_object),hl.matrixtable.MatrixTable):
            temp = implied_schema(schema_object)
        elif inspect.isclass(type(schema_object)):
            temp = implied_class(schema_object)
        else:  ## TODO switch to elif to check for dataframe object instead of assuming
            raise Exception('INPUT ERROR: Incorrect format for schema_object')
    return temp
