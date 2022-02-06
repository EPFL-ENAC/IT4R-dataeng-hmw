import pandas as pd
import os
import datetime
import re
import json

import sqlalchemy
from sqlalchemy import String, Integer, Boolean, Date
from sqlalchemy.dialects.postgresql import REAL, DOUBLE_PRECISION
from Levenshtein import distance as distance

from backend.ccfatigue.services.database import Base, engine

OUTPUTDIR = "../ProcessedData/"
if not os.path.exists(OUTPUTDIR):
    os.makedirs(OUTPUTDIR)


def BestWordMatch(word: str, alternatives: list, maxdiff=5) -> str:
    """
    Compares a word to be matched against a list of alternatives, and return the most likely.
    Word is matched as lowercase comparison
    :param word: string to be matched
    :param alternatives: list of possible matches
    :param maxdiff: maximum value of the Levenshtein edit distance
    :return: best matched word in list, None if it does not fit the criteria
    """
    if not alternatives:
        return None
    dists = [(distance(word.lower(), x.lower()), x) for x in alternatives]
    d, best_match = sorted(dists)[0]
    if d < maxdiff:
        return best_match
    return None


def save_nested_dict(top: list, bottom: list, data: list) -> dict:
    """
    creates a nested dictionary from input, not nans in keys
    :param top: top descriptor (repeated)
    :param bottom: individual key, second level
    :param data: individual value, second level
    :return: dictionary with {top : {bottom:data}, ...}
    """
    out = {}
    for i, j, k in zip(top, bottom, data):
        if pd.notna(i) and pd.notna(j):
            if i not in out.keys():
                out[i] = {j: k}
            else:
                out[i][j] = k
    return out


def NullablesInTable(toinsert: dict, table: sqlalchemy.Table) -> bool:
    """
    Checks required records in the table
    :param toinsert: key and value to insert
    :param table: tablebase
    """
    notnullable = [x.name for x in table.columns if not x.nullable]
    for col in notnullable:
        if col not in toinsert.keys():
            print(f"Column {col} not present and required")
            return True
    return False


def InsertToTable(data, engine: sqlalchemy.engine, outtable: sqlalchemy.Table):
    """
    subroutine to commit data to the DB
    :param data: dictionary or json to insert
    """
    try:
        ins = outtable.insert(data)
        engine.execute(ins)
    except Exception as e:
        print("XXXXXXX", e)


def path_from_data(
        researcher: str,
        date: datetime.datetime,
        test_type: str,
        test_num: int) -> tuple:
    """
    Construct the most probable path according to the schema
    dirpath/FakeResearcher_2019-09_QS/TST_2019-09_QS_metadata.xls
    dirpath/FakeResearcher_2019-09_QS/TST_2019-09_QS_04.csv
    :param researcher: researcher name
    :param date: date of the experiment
    :param test_type: type of test (QS or CA)
    :param test_num: test number in one experiment
    """
    year = '{:04d}'.format(date.month)
    month ='{:02d}'.format(date.month)
    ym = f"{year}-{month}"
    res = researcher.capitalize()
    if test_num < 100:
        t = '{:02d}'.format(test_num) # DANGER IF test_num > 100
    else:
        t = str(test_num) # no padding
        print("test_num might have an inaccurate format")
    mydir = f"{res}_{ym}_{test_type}"
    csv = f"TST_{ym}_{test_type}_{t}.csv"
    meta = f"TST_{ym}_{test_type}_metadata.xls"

    return mydir, csv, meta


def data_from_path(pathfile):
    """
    Receive the name of the excel file, expected to be in the form:
    dirpath/FakeResearcher_2019-09_QS/TST_2019-09_QS_metadata.xls
    Returns the with researcher name, date, type, test_number
    """
    fulldir, fname = os.path.split(pathfile)
    _, mydir = os.path.split(fulldir)
    # finds, optionally, researcher name, year, month, test_type
    re_dir = r"([\w]+)?_+(\d{4})?-(\d+)?_(FA|QS)?"
    #finds, optionally, year, month, test_type, test number
    re_f = r"(\d{4})?-(\d+)?_+(FA|QS)?_+(\d+)?"
    s_dir = re.compile(re_dir)
    s_f = re.compile(re_f)
    match_d = s_dir.search(mydir)
    match_f = s_f.search(fname)
    dir_results = [x for x in match_d.groups()] if match_d else [None,None,None,None]
    file_results = [x for x in match_f.groups()] if match_f else [None,None,None,None]
    dir_results.extend(file_results)
    return dir_results


class PdData:
    """
    Process a dataframe, to be mapped to a sql table, to extract information
    """

    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.columns = None
        self.units = None
        self.types = None

    def maptable(self, table: sqlalchemy.Table):
        """
        Associate the dataframe to column for later processing
        """
        allcolumns = [column for column in table.columns]
        self.tablecolnames = [col.name for col in allcolumns]
        self.tablecoltypes = {col.name: col.type for col in allcolumns}

    def clean_nans(self):
        """
        Removing all the empty lines and columns from the dataframe
        """
        self.df.dropna(axis=1, how='all', inplace=True)
        self.df.dropna(axis=0, how='all', inplace=True)

    def change_columns_names(self):
        """
        Renaming the dataframe columns according to the table column names
        """
        for oldcol in self.df.columns:
            newcol = BestWordMatch(oldcol, self.tablecolnames)
            if newcol is None:
                print(f"No match for column {oldcol}")
                continue
            self.df.rename({oldcol: newcol}, axis=1, inplace=True)

    def change_columns_types(self):
        """
        Renaming the dataframe columns according to the table column names
        """
        # mapper from sql type to python dataframe, to expand as needed
        SQL2PD = {
            String: "string",
            Integer: "Int64",  # Int64 allows NaN, int64 does not
            DOUBLE_PRECISION: "float64",
            REAL: "float32",
            Boolean: "bool",
            Date: "datetime64[D]"}
        SQL2FNC = {
            "string": str,
            "Int64": int,
            "float64": float,
            "float32": float,
            "bool": bool,
            "datetime64[D]": pd.to_datetime}
        for col in self.df.columns:
            tablecol = BestWordMatch(col, self.tablecolnames)
            if tablecol is None:
                print(f"Error: no match for {col}")
                continue
            coltype = self.tablecoltypes[tablecol]
            havetype = [SQL2PD[x] for x in SQL2PD.keys() if isinstance(coltype, x)]
            if havetype:
                typ = havetype[0]
                func = SQL2FNC[typ]
            else:
                print(f"column type {coltype} not supported")
                continue
            # before casting the column, some checks on data
            if isinstance(coltype, Boolean):
                # possible values that should be casted as no
                nos = ["no", "n", "false", "f"]
                self.df[col] = self.df[col].apply(
                    lambda x: False if str(x).lower() in nos else True)
            self.df[col] = self.df[col].apply(func)
            self.df[col] = self.df[col].astype(typ)


class PdExperimentTemplate(PdData):
    """
    Class that deals with reading the excel metadata template (experiment sheet)
    and saves relevant info to tables
    """

    def read_schema(self):
        # Infers the information from the file
        # I strongly assume a structure here
        self.df.set_index(self.df.columns[0], drop=True, inplace=True)
        # Expand top descriptor to adjacent cells
        self.database = self.df.iloc[0].fillna(method='ffill')
        self.columns = self.df.iloc[1]
        self.units = self.df.loc['Unit']
        self.types = self.df.loc["Type"]
        # Either of them, not the most robust method
        # self.mandatory = self.df.['Mandatory'].apply(lambda x: x == "*")
        self.mandatory = self.df['Mandatory'].apply(lambda x: not pd.isna(x))

    def export_json(self, fname):
        """
        Saves data to a series of json
        """
        x = [self.units, self.types, self.mandatory]
        fileextension = ["_names", "_units", "_mandatory"]
        for values, name in zip(x, fileextension):
            data = save_nested_dict(self.database, self.columns, values)
            json.dump(data, fname + name + ".json")


class PdTestsTemplate(PdData):
    """
    Class that deals with reading the excel metadata template (tests)
    and saves relevant info to tables
    """

    def read_schema(self):
        # Infers the information from the file
        # I strongly assume a structure here
        self.df.set_index(self.df.columns[0], drop=True, inplace=True)
        # Expand top descriptor to adjacent cells
        self.columns = self.df.iloc[0]
        self.units = self.df.loc['Unit']
        self.type = self.df.loc["Type"]
        # Either of them, not the most robust method
        # self.mandatory = self.df.['Mandatory'].apply(lambda x: x == "*")
        self.mandatory = self.df['Mandatory'].apply(lambda x: not pd.isna(x))

    def export_json(self, fname):
        """
        Saves data to a series of json
        Each record has a list of properties
        This
        """
        record = zip(self.columns, self.units, self.type, self.mandatory)
        data = {col: {
            'unit': unit,
            'type': typecol,
            'mandatory': mandatory}
            for col, unit, typecol, mandatory in record}
        json.dump(data, fname + ".json")


class PdExperimentData(PdData):
    """
    Class that deals with reading the excel metadata template (experiment sheet)
    and saves relevant info to tables
    """

    def read_schema(self):
        # Infers the information from the file; I strongly assume a schema here
        # Expand top descriptor to adjacent cells
        self.database = self.df.iloc[0].fillna(method='ffill')
        self.columns = self.df.iloc[1]
        self.data = self.df.iloc[2]

    def export_json(self, fname):
        """
        Saves data to a json
        will add extension
        """
        data = save_nested_dict(self.database, self.columns, self.data)
        outputname = fname + ".json"
        # also, save locally
        self.json = data
        with open(outputname, 'w') as outputfile:
            json.dump(data, outputfile)


class PdExperimentTest(PdData):
    """
    Class that deals with reading the excel metadata template (test sheet)
    and saves relevant info to tables
    """

    def export_csv(self, fname):
        """
        Saves data to a series of csv
        will add extension
        This file should only have data and the header
        """
        outputname = fname + ".csv"
        with open(outputname, 'w') as outputfile:
            self.df.to_csv(outputfile, index=False)


def ProcessPipeline(folder):
    """
    Process the metadata inside the folder to identify the
    metadata ivi contained and the test files
    This is an experiment folder! Not one with the metadata only
    input: folder that contains metadata and input data
    """
    for filename in os.listdir(folder):
        # prepare output files for processed data
        basefile = os.path.basename(filename)
        basefile = os.path.splitext(basefile)[0]
        outputname = os.path.join(OUTPUTDIR, basefile)
        fullpath = os.path.join(folder, filename)
        if filename.endswith("metadata.xls"):
            # process the excel file and saves the json
            sheet = pd.read_excel(
                fullpath,
                sheet_name="Experiment",
                header=None
            )
            experiment = PdExperimentData(sheet)
            experiment.clean_nans()
            experiment.read_schema()
            experiment.export_json(outputname)
            meta = experiment.json
            # bit of extra work, as I've chosen separated tables as schema
            alltabs = Base.metadata.tables.keys()
            for key, subdict in meta.items():
                t = BestWordMatch(key, alltabs)
                if t is None:
                    # there is no match for the table in the database; skipping
                    continue
                mytable = Base.metadata.tables[t]
                tablecols = mytable.columns.keys()
                # to reimport it as a sub-dataframe
                s = {x: [y] for x, y in subdict.items()}
                df = pd.DataFrame.from_dict(s)
                toclean = PdData(df)
                toclean.clean_nans()
                toclean.maptable(mytable)
                toclean.change_columns_names()
                toclean.change_columns_types()
                cleaned = toclean.df.to_dict(orient='records')
                if cleaned == []:
                    # no data to insert into the database
                    continue
                record = cleaned[0]
                if "Folder" in tablecols:
                    # folder is the ID key for some tables
                    record["Folder"] = folder

                # If the required values are there:
                if not NullablesInTable(record, mytable):
                    # Finally insert the metadata into the database
                    InsertToTable(record, engine, mytable)

            # Now, processing the "tests" sheet in the excel file and saves csv
            sheet = pd.read_excel(
                fullpath,
                sheet_name="Tests",
            )
            tests = PdExperimentTest(sheet)
            mytable = Base.metadata.tables["tests"]

            tests.clean_nans()
            tests.maptable(mytable)
            tests.change_columns_names()
            tests.change_columns_types()
            tests.export_csv(outputname)

            processedDF = tests.df
            processedDF["Folder"] = folder
            # Finally insert the metadata into the database
            # dataframe is properly transformed
            InsertToTable(
                processedDF.to_dict(orient="records"),
                engine,
                mytable
            )
        # processing the test data
        if filename.endswith(".csv"):
            # finds a file with the correct tail, and selects the test number

            ttype = data_from_path(fullpath)[-1]
            if ttype:
                test_number = int(ttype)
            else:
                # that's needed to insert into the database
                print(f"No recognisable test number in file {filename}")
                continue
            df = pd.read_csv(fullpath, index_col=None)

            # cleaning methods
            test = PdExperimentData(df)
            fatigue_table = Base.metadata.tables["fatigue_data"]
            test.clean_nans()
            test.maptable(fatigue_table)
            test.change_columns_names()
            test.change_columns_types()
            processedDF = test.df
            # add the test number column
            processedDF["TestMetadata_id"] = int(test_number)
            processedDF["Folder"] = folder
            InsertToTable(processedDF.to_dict(orient="records"), engine, fatigue_table)