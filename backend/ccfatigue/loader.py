import json
import datetime
import genericpath
from loguru import logger
import pandas as pd
from pandas.core.frame import DataFrame
import os

from ccfatigue.services.database import Base, database, engine
from sqlalchemy.orm import sessionmaker
from ccfatigue.models.database import *
from ccfatigue.data_checker import *


Base.metadata.create_all(engine)
Session = sessionmaker(engine)
session = Session()
logger.add("loading.log") # to be improved

def create_metadada_dict(in_xls_path: str,sheet_name : str,) -> dict:
    """
    Transform Excel metadat file into json.
    :param in_xls_path: path of the excel file
    :param sheet_name: sheet name
    :return: dict of the experiment metadata
    """
    if os.path.exists(in_xls_path) :
        input_file = pd.read_excel(in_xls_path, sheet_name = sheet_name, header=[0, 1])
        input_file = input_file.fillna('')
        temp_dict = dict()
        for key, value in input_file.to_dict('list').items():
            key_1 = key[0]
            key_2 = key[1]
            value_1 = value[0]
            if value_1 == '' :
                value_1 = None
            if key_1 not in temp_dict:
                temp_dict[key_1] = dict()
            temp_dict[key_1][key_2] = value_1
    else :
        raise ValueError(f'file {in_xls_path} not found')
    return temp_dict


def df_columns_name_formating(in_df: DataFrame) -> DataFrame:
    """
    Generic dataframe column fromating
    """
    in_df.columns = in_df.columns.str.lower()
    in_df.columns = in_df.columns.str.strip()
    in_df.columns = in_df.columns.str.replace(' ', '_')
    in_df.columns = in_df.columns.str.replace('-', '_')
    return in_df


def create_db_tables() :
    """
    Create the postgreSQL table according to the models
    Drop all table beforehand for development purpose --> should be improved
    """
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(engine)


def load_experiments_results(path_root_folder : str) :
    """
    Load data and metadata experiment results
    :param path_root_folder: folder containing the result
    :return: none
    """

    root_data_folder = path_root_folder

    # Load the types of test into the type table
    test_type_rows = [TestType(id=1,name="FA"),TestType(id=2,name="QS")]
    session.bulk_save_objects(test_type_rows)
    session.commit()

    # Tterate through the folder
    list_of_folder = [d for d in os.listdir(root_data_folder) if os.path.isdir(os.path.join(root_data_folder, d))]
    for folder_name in list_of_folder :

        # List of file in the current folder
        list_of_files = os.listdir(os.path.join(root_data_folder, folder_name))

        # Retrieve info from folder name
        researcher_lastname, experiment_date, test_type = folder_name.split('_')[1:]
        assert test_type in ['FA','QS']

        # Get test pk for folder name
        test_type_pk = [i.id for i in test_type_rows if i.name == test_type][0]

        # Transform metadata xls file into json file
        metadata_file_name = [i for i in list_of_files if i.endswith('.xls')][0] # we assume we have only one metadata xlsx file
        metadata_experiment_dict = create_metadada_dict(os.path.join(root_data_folder,folder_name,metadata_file_name),'Experiment')
        with open(os.path.join(root_data_folder,folder_name,f'{os.path.splitext(os.path.basename(metadata_file_name))[0]}.json'), 'w') as file:
            json.dump(metadata_experiment_dict, file,indent=4, sort_keys=True)

        # Populate PostgreSQL experiment table
        data_experiment = dict()
        data_experiment['laboratory'] = None
        data_experiment['researcher'] = researcher_lastname
        if not check_string_formating(experiment_date,'\d{4}-\d{2}') :
            logger.warning(f'Date wrongly formatted for folder {folder_name}')
        else :
            datetime_object = datetime.datetime.strptime(experiment_date, '%Y-%m')
            data_experiment['date'] = datetime_object
            data_experiment['test_type_id'] = test_type_pk
            experiment_row = Experiment(**data_experiment)
            result = session.add(experiment_row)

            # Get exeperiment pk
            session.flush()
            experiment_pk = experiment_row.id
            session.commit()

            # Populate the test table
            df_metadata_test = pd.read_excel(os.path.join(root_data_folder,folder_name,metadata_file_name), sheet_name = 'Tests', header=[0])
            df_metadata_test = df_columns_name_formating(df_metadata_test)
            df_metadata_test = df_metadata_test.replace('No',False)
            df_metadata_test = df_metadata_test.replace('Yes',True)
            df_metadata_test['experiment_id'] = experiment_pk
            df_metadata_test_json = df_metadata_test.to_dict(orient="records")
            for test_record in df_metadata_test_json :
                test_row = Test(**test_record)
                result = session.add(test_row)

                # Get exeperiment pk
                session.flush()
                test_pk = test_row.id
                specimen_number = test_row.specimen_number
                session.commit()


                # Populate the result table
                data_file_name = [i for i in list_of_files if i.endswith('.csv')]
                csv_file_name = [i for i in data_file_name if i.endswith(f"{specimen_number}.csv")][0] # to manage slightly wrongly file names
                csv_file_path = os.path.join(root_data_folder, folder_name, csv_file_name)
                if not os.path.exists(csv_file_path) :
                    logger.warning(f'CSV file not found {csv_file_path}')
                else :
                    df_result = pd.read_csv(csv_file_path,skip_blank_lines=True)

                    # Data cleansing and checking
                    df_result.dropna(how="all", inplace=True) # remove empty rows
                    df_result = df_columns_name_formating(df_result) # generic column name formatting
                    df_result["test_id"] = test_pk
                    df_result = check_column_names(df_result,'results',True,95)
                    check_mandatory_column_is_ok = check_mandatory_column(df_result,'results')
                    df_result = df_result.astype({'machine_n_cycles': 'int32'}) # change data type (float to integer)
                    check_column_data_type_is_ok = check_column_data_type(df_result,'results')
                    check_column_unit_is_ok = check_column_unit(df_result,'th_chamber',-273.15) # check temperature min absolute zeor


                    if check_column_data_type_is_ok and check_mandatory_column_is_ok and check_column_unit_is_ok :
                        try:
                            #result_schema.validate(df_result,lazy=True)
                            df_result.to_sql('results', engine, if_exists='append', index=False)  # bulk insert
                            logger.info(f"{csv_file_name} has been inserted")
                        except pa.errors.SchemaErrors as err:
                            logger.warning(f"Error while loading {csv_file_name}")
                            logger.warning(err.failure_cases)
                    else :
                        logger.info(f"{csv_file_name} has not been inserted")


            session.commit()





























