'''
Define the model as it is saved in the DB
'''

from ccfatigue.services.database import Base
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import Column
from sqlalchemy import Integer, String, Date, Boolean, ForeignKey
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION, ENUM
from sqlalchemy.orm import relationship

# Generic definition in the models
S80 = String(80)
Enum_ExpType = ENUM('FA', 'QS', name='ExpType')
Enum_FracMode = ENUM('Mode I', 'Mode II', 'Mode III', 'Combined',name='FracMode')
Enum_FatTest = ENUM('FA', 'VA', 'BL', 'Combined', name='FatTest')
Enum_ConMode = ENUM('Load Controlled', 'Displacement Controlled', name = 'ConMode')

# Here will come Model definitions

# I've chosen to create a star-like database, with all the metadata
# connected to the main test one, rather than creating json data
# Folder string seems like a reasonable choice of primary key for the databases
# Alternatively, it can be saved as a json output,
# or a jsonb kind of column

class Experiment(Base):
    """
    Data stored from each experiment folder
    Data from the base folder data
    Folder name should provide unicity and connection to the other tables
    """
    __tablename__ = 'experiment_details'
    Folder = Column(S80,  primary_key=True)
    Laboratory = Column(S80, nullable=False)
    Researcher = Column(S80, nullable=False)
    Date = Column(Date)
    Experiment_type = Column('Experiment Type', Enum_ExpType, nullable=False)
    Fracture = Column(Boolean, nullable=False)
    Fracture_mode = Column('Fracture Mode', Enum_FracMode, nullable=False)
    Initial_Crack_Length = Column("Initial Crack length", DOUBLE_PRECISION, 
            nullable=False)
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
    Folder = Column(S80,  primary_key=True)
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
    Folder = Column(S80,  primary_key=True)
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
    Folder = Column(S80,  primary_key=True)
    Length = Column(DOUBLE_PRECISION)
    Width = Column(DOUBLE_PRECISION)
    Thickness = Column(DOUBLE_PRECISION)


class LaminatesAndAssemblies(Base):
    """
    Data about Laminates and Assemblies
    """
    __tablename__ = 'laminates_and_assemblies'
    Folder = Column(S80,  primary_key=True)
    Curing_Time = Column('Curing Time', DOUBLE_PRECISION)
    Curing_Temperature = Column('Curing Temperature', DOUBLE_PRECISION)
    Curing_Pressure = Column('Curing Pressure', DOUBLE_PRECISION)
    Fiber_Content = Column('Fiber Content', DOUBLE_PRECISION)
    Stacking_Sequence = Column('Stacking Sequence', S80)


class TestConditions(Base):
    """
    Data about test conditions
    """
    # I know it's a spelling name mistake, but I'll keep it for consistency
    __tablename__ = 'test_condtions' 
    Folder = Column(S80,  primary_key=True)
    Temperature = Column('Test condtions', DOUBLE_PRECISION)
    Humidity = Column('DIC Analysis', DOUBLE_PRECISION)
    

class DicAnalysis(Base):
    """
    Data about DIC Analysis
    """
    __tablename__ = 'DIC_analysis' 
    Folder = Column(S80,  primary_key=True)
    Subset_Size = Column('Subset Size', Integer)
    Step_Size = Column('Step Size', Integer)


class TestMetadata(Base):
    """
    Metadata of each individual test within an experiment
    """
    __tablename__ = "tests"
    id = Column(Integer, primary_key=True)
    Folder = Column(S80, ForeignKey('experiment_details.Folder'))
    Specimen_number = Column('Specimen number', Integer)
    Stress_Ratio = Column('Stress Ratio', DOUBLE_PRECISION)
    Maximum_Stress = Column('Maximum Stress', DOUBLE_PRECISION)
    Loading_rate = Column('Loading rate',DOUBLE_PRECISION)
    Run_out = Column('Run-out', Boolean)


class FatigueData(Base):
    """
    Data stored from each fatigue test in each experiment
    """
    __tablename__ = 'fatigue_data'
    # required 
    id = Column(Integer, primary_key=True)
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
    Th_N_specimen_max = Column('Th N_specimen_max', Integer)
    Th_specimen_mean = Column('Th specimen_mean', Integer)
    Th_chamber = Column(Integer)
    Th_uppergrips = Column(Integer)
    Th_lowergrips = Column(Integer)
    # added for clarity
    Folder = Column(S80, ForeignKey('experiment_details.Folder'))


