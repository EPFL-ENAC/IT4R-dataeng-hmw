import os
import sys
import pandas as pd
sys.path.append(os.path.join(os.path.dirname(__file__),os.pardir))
from ccfatigue.data_checker import *


def test_check_column_names():
    d = {'machine_n_cycles': [1, 1], 'machine_loade': [3, 4], 'machine_displacement': [3, 4]}
    df_in = pd.DataFrame(data=d)
    df_out_expected = {'machine_n_cycles': {0: 1, 1: 1}, 'machine_load': {0: 3, 1: 4}, 'machine_displacement': {0: 3, 1: 4}}
    df_out = check_column_names(df_in,'results',True,95).to_dict()
    assert df_out == df_out_expected

def test_check_string_formating_true():
    string = '2021-12'
    result = check_string_formating(string,'\d{4}-\d{2}')
    assert result

def test_check_string_formating_false():
    string = '202112'
    result = check_string_formating(string,'\d{4}-\d{2}')
    assert not result


def test_check_column_unit_min():
    d = {'machine_n_cycles': [1, 10], 'machine_loade': [3, 4], 'machine_displacement': [3, 4]}
    df_in = pd.DataFrame(data=d)
    result = check_column_unit(df_in=df_in,column_name='machine_n_cycles',min_value=0,max_value=5)
    assert not result

    d = {'machine_n_cycles': [1, 4], 'machine_loade': [3, 4], 'machine_displacement': [3, 4]}
    df_in = pd.DataFrame(data=d)
    result = check_column_unit(df_in=df_in,column_name='machine_n_cycles',min_value=0,max_value=5)
    assert result

    d = {'machine_n_cycles': [1, 9999], 'machine_loade': [3, 4], 'machine_displacement': [3, 4]}
    df_in = pd.DataFrame(data=d)
    result = check_column_unit(df_in=df_in,column_name='machine_n_cycles',min_value=0)
    assert result

    d = {'machine_n_cycles': [1, 9999], 'machine_loade': [3, 4], 'machine_displacement': [3, 4]}
    df_in = pd.DataFrame(data=d)
    result = check_column_unit(df_in=df_in, column_name='machine_n_cycles', max_value=99)
    assert not result

    d = {'machine_n_cycles': [None, 9999], 'machine_loade': [3, 4], 'machine_displacement': [3, 4]}
    df_in = pd.DataFrame(data=d)
    result = check_column_unit(df_in=df_in, column_name='machine_n_cycles', min_value=99)
    assert result

    d = {'machine_n_cycles': [9, None], 'machine_loade': [3, 4], 'machine_displacement': [3, 4]}
    df_in = pd.DataFrame(data=d)
    result = check_column_unit(df_in=df_in, column_name='machine_n_cycles', min_value=99)
    assert not result

    d = {'machine_n_cycles': [None, None], 'machine_loade': [3, 4], 'machine_displacement': [3, 4]}
    df_in = pd.DataFrame(data=d)
    result = check_column_unit(df_in=df_in, column_name='machine_n_cycles', min_value=99)
    assert result






# Other tests should be implemented here....