'''
Define the model as it is saved in the DB
'''

from ccfatigue.services.database import Base
from sqlalchemy import Column
from sqlalchemy import Integer, String, Date, Boolean
from sqlalchemy.dialects.postgresql import REAL, DOUBLE_PRECISION, ENUM

# Generic definitions used in the models

S80 = String(80)
# Categorical types
Enum_ExpType = ENUM('FA', 'QS', name='ExpType')
Enum_FracMode = ENUM('Mode I', 'Mode II', 'Mode III', 'Combined', name='FracMode')
Enum_FatTest = ENUM('CA', 'VA', 'BL', 'Combined', name='FatTest')
Enum_ConMode = ENUM('Load Controlled', 'Displacement Controlled', name='ConMode')


# Here will come Model definitions


class Experiment(Base):
    """
    Data stored from each experiment folder
    Data from the base folder data
    Folder name should provide unicity and connection to the other tables
    """
    __tablename__ = 'experience'
    Folder = Column(S80, primary_key=True)
    Laboratory = Column(S80, nullable=False)
    Researcher = Column(S80, nullable=False)
    Date = Column(Date)
    Experiment_type = Column('Experiment Type', Enum_ExpType, nullable=False)
    Fracture = Column(Boolean, nullable=False)
    Fracture_mode = Column('Fracture Mode', Enum_FracMode)
    Initial_Crack_Length = Column("Initial Crack length", DOUBLE_PRECISION)
    Experiment_id = Column(S80)
    Experiment_number = Column(Integer)
    Researcher_id = Column(S80)
    Fatigue_Test_Type = Column('Fatigue Test Type', Enum_FatTest)
    Measuring_equipment = Column('Measuring Equipment', S80)
    Reliability_Level = Column('Reliability Level', DOUBLE_PRECISION)
    Control_mode = Column('Control mode', Enum_ConMode)


class Publication(Base):
    """
    Publication details from metadata
    """
    __tablename__ = 'publication'
    Folder = Column(S80, primary_key=True)
    Title = Column(S80)
    Author = Column(S80)
    Year = Column(String(4))
    DOI = Column(S80)
    Images_repository = Column('Images Repository', String(256))


class MaterialType(Base):
    """
    Material information of the experiment
    """
    __tablename__ = 'material_type'
    Folder = Column(S80, primary_key=True)
    Fiber_Material = Column('Fiber Material', S80)
    Fiber_Geometry = Column('Fiber Geometry', S80)
    Area_Density = Column('Area Density', DOUBLE_PRECISION)
    Resin = Column(S80)
    Hardener = Column(S80)
    Mixing_ratio = Column('Mixing ratio', S80)


class Geometry(Base):
    """
    Geometry information of the experiment
    """
    __tablename__ = 'geometry'
    Folder = Column(S80, primary_key=True)
    Length = Column(DOUBLE_PRECISION)
    Width = Column(DOUBLE_PRECISION)
    Thickness = Column(DOUBLE_PRECISION)


class LaminatesAndAssemblies(Base):
    """
    Data about Laminates and Assemblies
    """
    __tablename__ = 'laminates_and_assemblies'
    Folder = Column(S80, primary_key=True)
    Curing_Time = Column('Curing Time', DOUBLE_PRECISION)
    Curing_Temperature = Column('Curing Temperature', DOUBLE_PRECISION)
    Curing_Pressure = Column('Curing Pressure', DOUBLE_PRECISION)
    Fiber_Content = Column('Fiber Content', DOUBLE_PRECISION)
    Stacking_Sequence = Column('Stacking Sequence', S80)


class TestConditions(Base):
    """
    Data about test conditions
    """
    # I know it's a spelling name mistake, I've removed it
    # This is allowed if close enough to the column name
    __tablename__ = 'test_conditions'
    Folder = Column(S80, primary_key=True)
    Temperature = Column(DOUBLE_PRECISION)
    Humidity = Column(DOUBLE_PRECISION)


class DicAnalysis(Base):
    """
    Data about DIC Analysis
    """
    __tablename__ = 'dic_analysis'
    Folder = Column(S80, primary_key=True)
    Subset_Size = Column('Subset Size', Integer)
    Step_Size = Column('Step Size', Integer)


class TestMetadata(Base):
    """
    Metadata of each individual test within an experiment
    """
    __tablename__ = "tests"
    id = Column(Integer, primary_key=True)
    Folder = Column(S80)
    Specimen_number = Column('Specimen number', Integer)
    Stress_Ratio = Column('Stress Ratio', DOUBLE_PRECISION)
    Maximum_Stress = Column('Maximum Stress', DOUBLE_PRECISION)
    Loading_rate = Column('Loading rate', DOUBLE_PRECISION)
    Run_out = Column('Run-out', Boolean)


# noinspection SpellCheckingInspection
class FatigueData(Base):
    """
    Data stored from each fatigue test in each experiment
    """
    __tablename__ = 'fatigue_data'
    # required 
    id = Column(Integer, primary_key=True, autoincrement=True)
    Machine_N_cycles = Column('Machine N_cycles', Integer, nullable=False)
    Machine_Load = Column(DOUBLE_PRECISION, nullable=False)
    Machine_Displacement = Column(DOUBLE_PRECISION, nullable=False)
    index = Column(Integer)
    Camera_N_cycles = Column('Camera N_cycles', Integer)
    exx = Column(DOUBLE_PRECISION)
    eyy = Column(DOUBLE_PRECISION)
    exy = Column(DOUBLE_PRECISION)
    crack_length = Column('crack_length', DOUBLE_PRECISION)
    Th_time = Column(Integer)
    Th_N_cycles = Column('Th N_cycles', Integer)
    Th_N_specimen_max = Column('Th N_specimen_max', REAL)
    Th_specimen_mean = Column('Th specimen_mean', REAL)
    Th_chamber = Column(REAL)
    Th_uppergrips = Column(REAL)
    Th_lowergrips = Column(REAL)
    # added for clarity
    TestMetadata_id = Column(Integer)  # to directly join with TestMetadata
    Folder = Column(S80)  # this to query between tables


class ExperimentUnits(Base):
    """
    Standard units for the experiments that have units
    This is saved here as Tests have only one metadata file
    """
    __tablename__ = 'experiment_units'
    Folder = Column(S80, primary_key=True)
    Initial_Crack_Length = Column("Initial Crack length", S80,
                                  nullable=False)
    Area_Density = Column('Area Density', S80)
    Length = Column(S80)
    Width = Column(S80)
    Thickness = Column(S80)
    Curing_Time = Column('Curing Time', S80)
    Curing_Temperature = Column('Curing Temperature', S80)
    Curing_Pressure = Column('Curing Pressure', S80)
    Temperature = Column(S80)
    Humidity = Column(S80)


class TestUnits(Base):
    """
    Standard test units 
    I keep them separate, even if there is only one file, 
    as they are conceptually different from the Experiment units
    and could in principle vary between tests
    """
    __tablename__ = 'test_units'
    Folder = Column(S80, primary_key=True)
    Maximum_Stress = Column("Maximum Stress", S80)
    Loading_Rate = Column('Loading Rate', S80)
