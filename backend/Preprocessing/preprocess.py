import pandas as pd
import os
import datetime
import re
import json

import sqlalchemy
from sqlalchemy import String, Integer, Boolean, Date
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION
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


def save_nested_dict(top: list, bottom: list, data:list) -> dict:
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


def CleanData(rawdata, OutTable):
    """
    Cleaning subroutine
    This subroutine receives a json or a dictionary ready to be inserted
    Data has to be validated against the existing metadata
    to convert data into the right format
    Small inconsistencies between column names, table names are ignored
    The cleaned dictionary is returned for db insertion
    """
    SQL2FUNC = {
        String: str,
        Integer: int,
        DOUBLE_PRECISION: float,
        Boolean: bool,
        Date: lambda x: datetime.datetime.strptime(x, '%Y-%m')
    }
    tablecols = OutTable.columns.keys()
    outdict = {}
    for col, val in rawdata.items():
        if val is None:
            # No need to insert null values
            continue
        mappedcol = BestWordMatch(col, tablecols)
        if mappedcol is None:
            # column name is too different to be guessed
            # won't commit to database
            return
        mappedtype = OutTable.columns[mappedcol].type
        # apply the given transformation
        func = SQL2FUNC[mappedtype]
        if func == bool:
            if str(val).lower() == "no":
                # Should put here all the boolean values that can
                # be considered False. Most strings or int are True by default
                val = False
        if isinstance(mappedtype, Date):
            # TODO insert more data possibilities for conversions
            func = lambda x: datetime.datetime.strptime(x, '%Y-%m')

        outdict[mappedcol] = func(val)
    return outdict


def ProcessDataFrame(df, OutTable):
    """
    Variation of the cleandata subroutine,
    but to bulk insert a modified pandas dataframe
    generated from the tests metadata or tests CSV
    """
        if typ == "bool":
            df[x] = df[x].apply(lambda x: False if str(x).lower() == "no" else x)
        # cast column for database insertion
        df[x] = df[x].astype(typ)
        df.rename({x: rightcol}, axis=1, inplace=True)
    return df


def InsertToTable(data, engine, outtable):
    """
    subroutine to commit the data to the DB
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
            jsonfile = ExtractFromExperimentExcel(fullpath, folder, schema=False)
            # re-import the json data
            with open(jsonfile, 'r') as fname:
                meta = json.load(fname)
            # this is a composite json that will inject to multiple tables
            # check against all the database table we created
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
                # Finally insert the metadata into the database
                InsertToTable(cleanedjson, engine, mytable)

            # Now, processing the "tests" sheet in the excel file and saves csv
            csvfile = ExtractFromTestsExcel(fullpath, folder, schema=False)
            df = pd.read_csv(csvfile, index_col=None)
            mytable = Base.metadata.tables["tests"]
            processedDF = ProcessDataFrame(df, mytable)
            # add folder column for this
            processedDF["Folder"] = folder
            # Finally insert the metadata into the database
            # dataframe is properly transformed
            InsertToTable(processedDF.to_dict(orient="records"), engine, mytable)
        # processing the test data
        if filename.endswith(".csv"):
            # feels hacky :(
            # finds a file with the correct tail, and selects the test number
            match = re.search(r'(FA|QS)[_]+(\d+)', filename)
            if match is None:
                # File does not match the requirements; I do not need the date, as it is in metadata.
                # But the test number has to be there! It could be a random csv in folder
                continue
            test_number = match.group(2)

            df = pd.read_csv(fullpath, index_col=None)

            # cleaning class
            test = PdExperimentData(df)
            fatigue_table = Base.metadata.tables["fatigue_data"]
            test.maptable(fatigue_table)
            test.clean_nans()

            processedDF = ProcessDataFrame(df, mytable)
            # add the test number column
            processedDF["TestMetadata_id"] = int(test_number)
            processedDF["Folder"] = folder
            InsertToTable(processedDF.to_dict(orient="records"), engine, mytable)


def ExtractFromExperimentExcel(filename, folder, schema=True):
    """
    Performs an initial feature extraction from the provided
    excel metadata file to extract information.
    Assumed structure:
    xls ------Sheet one named 'Experiment'
          |---Sheet two named 'Tests'

    This subroutine deals with the 'Experiment' data
    schema = "schema" is true for the toplevel xls, saves units
             = otherwise it looks for data" for the experiment folders
    """

    sheets = pd.read_excel(filename, sheet_name=None, header=None)
    # Would be safer to assume that there are two sheets and call
    # them by name by cycling over them
    # but I have to assume something somewhere!

    experiment = sheets["Experiment"]

    # clean empty fully empty rows and columns to identify those with data
    # Since there are required field, that should not remove all

    experiment = CleanNaNs(experiment)

    # Now, first line would be the top descriptor of the json I want
    top = experiment.iloc[0].fillna(method="ffill")
    bottom = experiment.iloc[1]

    outputfolder = OUTPUTDIR
    if not os.path.exists(outputfolder):
        os.makedirs(outputfolder)
    basefile = os.path.basename(filename)
    basefile = os.path.splitext(basefile)[0]
    outputname = os.path.join(
        outputfolder,
        basefile + "_experiment_metadata.json")

    if schema:
        # last, find the units
        # In this file at least, it seems to be a column name
        # It seems safer than assuming a column number...
        experiment = experiment.set_index(experiment.columns[0])
        data = experiment.iloc['Unit']

    else:
        # in the data I have it is the third line
        # again, this is not robust
        data = experiment.iloc[2]
    # Saving the schema to dictionary and json
    # Ignoring all NANs
    toexport = save_nested_dict(top, bottom, data)
    with open(outputname, 'w') as outputfile:
        json.dump(toexport, outputfile)

    return outputname


def ExtractFromTestsExcel(filename, folder, schema=True):
    """
    Performs an initial feature extraction from the provided
    excel metadata file to extract information.
    Assumed structure:
    xls ------Sheet one named 'Experiment'
          |---Sheet two named 'Tests'

    This subroutine deals with the "Tests" data
    schema = "schema" is true for the toplevel xls, saves units
             = otherwise it looks for data" for the experiment folders
    """

    sheets = pd.read_excel(filename, sheet_name=None, header=None)
    # Would be safer to assume that there are two sheets and call
    # them by name by cycling over them
    # but I have to assume something somewhere!

    tests = sheets["Tests"]

    # clean empty fully empty rows and columns to identify those with data
    # Since there are required field, that should not remove all

    tests = CleanNaNs(tests)

    # Now, first line would be the top descriptor of the json I want
    header = tests.iloc[0]

    basefile = os.path.basename(filename)
    basefile = os.path.splitext(basefile)[0]

    if schema:
        # assume there is an index
        tests = tests.set_index(tests.columns[0])
        # extract the unit data
        data = tests.loc['Unit']
        toexport = {}
        for i, j in zip(header, data):
            if pd.notna(i) and pd.notna(j):
                toexport[i] = j
        outputname = os.path.join(
            OUTPUTDIR,
            basefile + "_tests_metadata.json")

        with open(outputname, 'w') as outputfile:
            json.dump(toexport, outputfile)

    else:
        # return the csv of the tests to file
        # since I did not process it, header *should be the first line
        # data is in the subsequent lines
        outputname = os.path.join(
            OUTPUTDIR,
            basefile + "_tests_metadata.csv")

        tests.to_csv(outputname, index=False, header=False)

    return outputname


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


def CleanNaNs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removing all the unused lines and columns from the dataframe
    :param df: input dataframe
    """
    df.dropna(axis=1, how='all', inplace=True)
    df.dropna(axis=0, how='all', inplace=True)
    return df


def MapTableNames(df: pd.DataFrame, table: sqlalchemy.Table) -> pd.DataFrame:
    """
    Renames the colums according to the mapped table
    :return: output dataframe
    """
    allkeys = table.columns.keys()
    for col in df.columns:
        rightcol = BestWordMatch(col, allkeys)
        if rightcol is None:
            # no match for the column
            continue
        df.rename({col: rightcol}, axis=1, inplace=True)
    return df


def MapTableTypes(df: pd.DataFrame, table: sqlalchemy.Table) -> pd.DataFrame:
    """
    Renames the columns types to the mapped table
    :return: output dataframe
    """
    SQL2PYTHON = {
        String: "string",
        Integer: "Int64",
        DOUBLE_PRECISION: "float64",
        BOOLEAN: "boolc",
    }
    for col in df.columns:
        coltype = table.columns[col].type
        typ = SQL2PYTHON[coltype]
        df[col] = df[col].astype(typ)
    return df


class PdExcelData:
    """
    Process a dataframe, to be mapped to a sql table, to extract information
    """
    # mapper from sql type to python dataframe, to expand as needed
    SQL2PD = {
        String: "string",
        Integer: "Int64",
        DOUBLE_PRECISION: "float64",
        BOOLEAN: "bool",
    }

    def __init__(self, df: pd.DataFrame):
        self.df = df

    def maptable(self, table: sqlalchemy.Table):
        """
        Associate the df to a table
        :param table: output table
        """
        allcolumns = [column for column in table.columns]
        self.colnames = [col.name: col.type for col in allcolumns]
        self.coltypes = {col.name: col.type for col in allcolumns}
        self.colnullable = {col.name: col.nullable for col in allcolumns}

    def clean_nans(self):
        """
        Removing all the unused lines and columns from the dataframe
        :param df: input dataframe
        """
        self.df.dropna(axis=1, how='all', inplace=True)
        self.df.dropna(axis=0, how='all', inplace=True)

    def change_columns_names(self):
        """
        Renaming the dataframe columns according to the table column names
        """
        for col in self.df.columns:
            columns = self.coltypes.keys()
            rightcol = BestWordMatch(col, columns)
            if rightcol is None:
                # no match for the column
                print(f"No match for column {col}")
                continue
            self.df.rename({col: rightcol}, axis=1, inplace=True)


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
        data = {col:{
            'unit':unit,
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
        self.df.set_index(self.df.columns[0], drop=True, inplace=True)
        # Expand top descriptor to adjacent cells
        self.database = self.df.iloc[0].fillna(method='ffill')
        self.columns = self.df.iloc[1]
        self.data = self.df.iloc[2]

    def export_json(fname):
        """
        Saves data to a series of json
        will add extension
        """
        x = [self.units, self.types, self.mandatory]
        fileextension = ["_names", "_units", "_mandatory"]
        for values, name in zip(x, fileextension):
            data = save_nested_dict(self.database, self.columns, values)
            json.dump(data, fname + name + ".json")