import pandas as pd
import os
import datetime
import re
import json
from Levenshtein import distance as distance

OUTPUTDIR = "../ProcessedData/"
if not os.path.exists(OUTPUTDIR):
    os.makedirs(OUTPUTDIR)


def BestWordMatch(word, alternatives, maxdiff=5):
    """
    This subroutine compares a word with a list of alternatives,
    and return the most likely match according to a word distance.
    It returns the best word and its distance,
    everything compared with the same case,
    if the distance is below the maxdiff threshold,
    which is roughly the number of typos, or e.g. "_" vs "\s"
    """
    dists = [(distance(word.lower(), x.lower()), x) for x in alternatives]
    d, best_match = sorted(dists)[0]
    if d < maxdiff:
        return best_match
    return None


def CleanData(rawdata, OutTable):
    """
    Cleaning subroutine
    This subroutine receives a json or a dictionary ready to be inserted
    Data has to be validated against the existing metadata
    to convert data into the right format
    Small inconsistencies between column names, table names are ignored
    The cleaned dictionary is returned for db insertion
    """
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
        # apply the given transformat
        if isinstance(mappedtype, String):
            func = str
        if isinstance(mappedtype, Integer):
            func = int
        if isinstance(mappedtype, DOUBLE_PRECISION):
            func = float
        if isinstance(mappedtype, Boolean):
            func = bool
            if str(val).lower() == "no":
                # Should put here all the boolean values that can
                # be considered False. Most strings or int are True by default
                jsonval = False
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
    allkeys = OutTable.columns.keys()
    for x in df.columns:
        # find the best table column name matching
        rightcol = BestWordMatch(x, allkeys)
        if rightcol is None:
            # no match for the column
            continue
        coltype = OutTable.columns[rightcol].type
        if isinstance(coltype, String):
            typ = "string"
        if isinstance(coltype, Integer):
            typ = "Int64"
        if isinstance(coltype, DOUBLE_PRECISION):
            typ = "float64"
        if isinstance(coltype, Boolean):
            df[x] = df[x].apply(lambda x: False if str(x).lower() == "no" else x)
            typ = "bool"
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
        if filename.endswith(".xls"):
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
                # file does not match the requirements
                # I do not need the date, as it is in metadata,
                # but the test number has to be there
                # it could be a random csv in folder
                continue
            test_number = match.group(2)
            df = pd.read_csv(fullpath, index_col=None)
            df.dropna(axis=1, how='all', inplace=True)
            df.dropna(axis=0, how='all', inplace=True)
            mytable = Base.metadata.tables["fatigue_data"]
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

    experiment.dropna(axis=1, how='all', inplace=True)
    experiment.dropna(axis=0, how='all', inplace=True)

    # Now, first line would be the top descriptor of the json I want
    experiment.iloc[0].fillna(method='ffill')
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
    toexport = {}
    for i, j, k in zip(top, bottom, data):
        if pd.notna(i) and pd.notna(j) and pd.notna(k):
            if i not in toexport.keys():
                toexport[i] = {j: k}
            else:
                toexport[i][j] = k
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

    tests.dropna(axis=1, how='all', inplace=True)
    tests.dropna(axis=0, how='all', inplace=True)

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