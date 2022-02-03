import pandas as pd
from pandas.core.frame import DataFrame
from typing import Any, Dict, List,Optional
from sqlalchemy import inspect,dialects
from fuzzywuzzy import process
import re
from loguru import logger
from ccfatigue.services.database import Base, database, engine
from ccfatigue.models.database import *

logger.add("loading.log")

def get_table_metadata(table_name : str) -> list :
    inspector = inspect(engine)
    return inspector.get_columns(table_name)



def check_column_names(df : DataFrame, table_name : str, replace : bool, threshold : int) -> DataFrame :
    """
    Check if the dataframe columns names match the model and modify them by fuzzy proximity
    """

    model_attribute_names = [i.get('name') for i in get_table_metadata(table_name)]
    data_column_names = list(df.columns)
    data_column_names_not_found = list(set(data_column_names).difference(model_attribute_names))

    if replace :
        for col in data_column_names_not_found :
            highest = process.extractOne(col,model_attribute_names)
            if highest[1] > threshold :
                df = df.rename(columns={col:highest[0]})
            else :
                df = df.truncate()
                logger.warning(f"Column {col} has not been found in the model and can not be deduced by fuzzy proximity.")
                #raise ValueError(f"Column {col} has not been found in the model and can not be deduced by fuzzy proximity.")

    return df

def check_mandatory_column(df : DataFrame, table_name : str) -> bool :
    """
    Check if all the element in mandatory columns are not empty
    :param df: dataframe to check
    :param table_name: name of the table
    :return: true or false
    """
    list_of_complsory_attributes = [i.get('name') for i in get_table_metadata(table_name) if not i.get('nullable') and not i.get('autoincrement')]

    for column in list_of_complsory_attributes :
        list_index_with_null_value = df[df[column].isnull()].index.tolist()
        if list_index_with_null_value :
            logger.warning(f"Column {column} has null value(s) at index {list_index_with_null_value}")
            #raise ValueError(f"Column {column} has null value(s) at index {list_index_with_null_value}")
            return False
        else :
            return True



def check_column_data_type(df : DataFrame, table_name : str) -> bool :
    """
    Check the column type according to the model
    :param df: dataframe
    :param table_name: name of the table
    :return: True or False
    """

    dict_type_matching = dict()
    # PostgreSLQ data type VS pandas data type. Clearly not exhaustive !
    dict_type_matching['INTEGER'] = ['int64','int32']
    dict_type_matching['BIGINT'] = ['int64']
    dict_type_matching['SMALLINT'] = ['int64']
    dict_type_matching['DATE'] = ['datetime64','int64']
    dict_type_matching['DOUBLE_PRECISION'] = ['float64','int64']
    dict_type_matching['FLOAT'] = ['float64','int64']
    dict_type_matching['NUMERIC'] = ['float64','int64']
    dict_type_matching['BOOLEAN'] = ['bool']
    dict_type_matching['DATE'] = ['datetime64']
    dict_type_matching['TEXT'] = ['int64','datetime64','object','float64','bool']
    dict_type_matching['VARCHAR'] = ['int64','datetime64','object','float64','bool']


    df_temp = df.dtypes.to_dict()
    for column in df:
        df_column_type_as_string =  str(df.dtypes.to_dict().get(column)).replace('[ns]','')
        model_column_type_as_string = str([i.get('type') for i in get_table_metadata(table_name) if i.get('name') == column][0])
        allowed_model_types = dict_type_matching.get(model_column_type_as_string)

        if df_column_type_as_string not in allowed_model_types :
            logger.warning(f"Column {column} {df_column_type_as_string} type does not macht with the model type {model_column_type_as_string} ")
            #raise ValueError(f"Column {column} {df_column_type_as_string} type does not macht with the model type {model_column_type_as_string} ")
            return False
        return True




def check_string_formating(string_to_check : str, regex_pattern : str) -> bool :
    """
    Check if a string match a regex pattern
    :param string_to_check: string to check
    :param regex_pattern: regex
    :return: true or false
    """
    pattern = re.compile(regex_pattern)
    if pattern.match(string_to_check) :
        return True
    else :
        return False


def check_column_unit(df_in : DataFrame,column_name : str,min_value : Optional[float] =None, max_value : Optional[float] = None) -> bool :
    """
    Check if all element of a column is between a range
    :param df_in: input dataframe
    :param column_name: name of the column
    :param min_value: optional main value
    :param max_value: optionla max value
    :return:
    """
    response = True

    column = df_in[column_name]
    column = column.dropna()
    if not column.empty :
        if min_value :
            column_min = column.min()
            if min_value > column_min :
                logger.warning(f"Min value for column {column} is not respected")
                response = False

        if max_value :
            column_max = column.max()
            if max_value < column_max :
                logger.warning(f"Max value for column {column} is not respected")
                response = False

    return response








