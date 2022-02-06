import pandas as pd
import os
import datetime
import re
import json

import sqlalchemy
from sqlalchemy import String, Integer, Boolean, Date
from sqlalchemy.dialects.postgresql import REAL, DOUBLE_PRECISION
from Levenshtein import distance as distance

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


def ProcessPipeline(folder):
    """
    Process the metadata inside the folder to identify the
    metadata ivi contained and the test files
    This is an experiment folder! Not one with the metadata only
    input: folder that contains metadata and input data
    """
    for filename in os.listdir(folder):
        fullpath = os.path.join(folder, filename)
        print(filename, fullpath)
        if filename.endswith("metadata.xls"):
            # process the excel file and saves the json
            sheet = pd.read_excel(
                fullpath,
                sheet_name="Experiment",
                header=None
            )

            basefile = os.path.basename(filename)
            basefile = os.path.splitext(basefile)[0]
            outputname = os.path.join(OUTPUTDIR, basefile)

            experiment = PdExperimentData(sheet)
            experiment.clean_nans()
            experiment.export_json(outputname)
            meta = experiment.json
            # bit of extra work, as I've chosen separated table
            alltabs = Base.metadata.tables.keys()
            for key, subdict in meta.items():
                t = BestWordMatch(key, alltabs)
                if t is None:
                    # there is no match for the table in the database; skipping
                    continue
                mytable = Base.metadata.tables[t]
                tablecols = mytable.columns.keys()
                cleanedjson = CleanData(subdict, mytable)
                if cleanedjson == {}:
                    # no data to insert into the database
                    continue
                if "Folder" in tablecols:
                    # folder is the ID key for some tables
                    cleanedjson["Folder"] = folder

                # If the required values are there:
                if not NullablesInTable(cleanedjson, mytable):
                    # Finally insert the metadata into the database
                    InsertToTable(cleanedjson, engine, mytable)

            # Now, processing the "tests" sheet in the excel file and saves csv
            sheet = pd.read_excel(
                fullpath,
                sheet_name="Tests",
            )
            tests = PdExperimentTest(sheet)
            mytable = Base.metadata.tables["tests"]

            tests.clean_nans()
            tests.change_columns_names(mytable)
            tests.change_columns_types(mytable)
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
            match = re.search(r'(FA|QS)[_]+(\d+)', filename)
            if match is None:
                # File does not match the requirements; I do not need the date, as it is in metadata.
                # But the test number has to be there! It could be a random csv in folder
                continue
            test_type = match.group(1)
            test_number = match.group(2)

            df = pd.read_csv(fullpath, index_col=None)

            # cleaning methods
            test = PdExperimentData(df)
            fatigue_table = Base.metadata.tables["fatigue_data"]
            test.clean_nans()
            test.change_columns_names(fatigue_table)
            test.change_columns_types(fatigue_table)
            processedDF = test.df
            # add the test number column
            processedDF["TestMetadata_id"] = int(test_number)
            processedDF["Folder"] = folder
            InsertToTable(processedDF.to_dict(orient="records"), engine, mytable)


def ProcessCSVData(filename):
    """
    Receive the name of the csv file, expected to be in the form:
       dirpath/TST_FakeResearcher_2019-09_QS/TST_2019-09_QS_01.csv
    Returns dictionary with researcher name, date, type and test number
    """
    pass


def ProcessXLSMetadata(filename):
    """
    Receive the name of the excel file, expected to be in the form:
       dirpath/TST_FakeResearcher_2019-09_QS/TST_2019-09_QS_metadata.xls
    Returns the with researcher name, date, type
    """
    pass


class PdExcelData:
    """
    Process a dataframe, to be mapped to a sql table, to extract information
    """
    def __init__(self, df: pd.DataFrame)):
        self.df = df


    def maptable(self, , table:sqlalchemy:Table):
        """
        Associate the dataframe to column for later processing
        """
        allcolumns = [column for column in table.columns]
        self.tablecolnames = [col.name for col in allcolumns}
        self.tablecoltypes = {col.name: col.type for col in allcolumns}

    def clean_nans(self):
        """
        Removing all the unused lines and columns from the dataframe
        :param df: input dataframe
        """
        self.df.dropna(axis=1, how='all', inplace=True)
        self.df.dropna(axis=0, how='all', inplace=True)

    def change_columns_names(self, table: sqlalchemy.Table):
        """
        Renaming the dataframe columns according to the table column names
        """
        allcolumns = [column for column in table.columns]
        colnames = {col.name: col.type for col in allcolumns}
        coltypes = {col.name: col.type for col in allcolumns}
        for oldcol in self.df.columns:
            newcol = BestWordMatch(oldcol, self.tablecolnames)
            if newcol is None:
                print(f"No match for column {col}")
                continue
            self.df.rename({oldcol: newcol}, axis=1, inplace=True)

    def change_columns_types(self, table: sqlalchemy.Table):
        """
        Renaming the dataframe columns according to the table column names
        """
        # mapper from sql type to python dataframe, to expand as needed
        SQL2PD = {
            String: "string",
            Integer: "Int64",
            DOUBLE_PRECISION: "float64",
            REAL: "float32",
            Boolean: "bool",
            Date: "Date"
        }
        SQL2FNC = {
            String: str,
            Integer: int,
            DOUBLE_PRECISION: float,
            REAL: float,
            Boolean: bool,
            Date: pd.to_datetime
        }
        for oldcol in self.df.columns:
            newcol = BestWordMatch(oldcol, self.tablecolnames)
            if newcol is None:
                print(f"Error: no match for {col}")
                continue
            print(self.tablecoltypes[newcol], self.SQL2PD.keys())
            if tablecoltypes[newcol] in self.SQL2PD.keys():

                func = self.SQL2FUNC[coltypes(newcol)]
                typ = self.SQL2PD[coltypes(newcol)]
            else:
                print(f"column type {tablecoltypes[newcol]} not supported")
            # before casting the column, some checks on data
            if isinstance(tablecoltypes[newcol], Boolean):
                # possible values that should be casted as no
                nos = ["no", "n", "false", "f"]
                self.df.apply(lambda x: False if str(x).lower() in nos else True)
            self.df.apply(func)
            self.dt[col].astype(typ)


class PdExperimentTemplate(PdExcelData):
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
        self.type = self.df.loc["Type"]
        self.type = []
        # Either of them, not the most robust method
        # self.mandatory = self.df.['Mandatory'].apply(lambda x: x == "*")
        self.mandatory = self.df['Mandatory'].apply(lambda x: not pd.isna(x))

    def export_json(fname):
        """
        Saves data to a series of json
        """
        x = [self.units, self.types, self.mandatory]
        fileextension = ["_names", "_units", "_mandatory"]
        for values, name in zip(x, fileextension):
            data = save_nested_dict(self.database, self.columns, values)
            json.dump(data, fname + name + ".json")


class PdTestsTemplate(PdExcelData):
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

    def export_json(fname):
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
        json.dump(data, fname + name + ".json")


class PdExperimentData(PdExcelData):
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

    def export_json(fname):
        """
        Saves data to a json
        will add extension
        """
        self.data
        data = save_nested_dict(self.database, self.columns, self.data)
        outputname = fname + ".json"
        # also, save locally
        self.json = json.dumps(data)
        with open(outputname, 'w') as outputfile:
            json.dump(toexport, outputfile)


class PdExperimentTest(PdExcelData):
    """
    Class that deals with reading the excel metadata template (test sheet)
    and saves relevant info to tables
    """

    def export_csv(fname):
        """
        Saves data to a series of csv
        will add extension
        This file should only have data and the header
        """
        outputname = fname + ".csv"
        with open(outputname, 'w') as outputfile:
            pd.to_csv(toexport, outputfile, ind)