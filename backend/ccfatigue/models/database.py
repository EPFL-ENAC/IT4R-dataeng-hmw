'''
Define the model as it is saved in the DB
'''
from sqlalchemy import Column, Integer, String, ForeignKey, Table,Date,Float,Boolean
from ccfatigue.services.database import Base, database, engine
from sqlalchemy.orm import sessionmaker


class TestType(Base):
    __tablename__ = "type"
    id = Column(Integer, primary_key=True)
    name = Column(String)


class Experiment(Base):
    __tablename__ = "experiment"
    id =  Column(Integer, primary_key=True)
    laboratory = Column(String)
    researcher = Column(String)
    date = Column(Date)
    test_type_id = Column(Integer, ForeignKey('type.id'))

class Test(Base) :
    __tablename__ = "tests"
    id = Column(Integer, primary_key=True)
    experiment_id = Column(Integer, ForeignKey('experiment.id'))
    specimen_number = Column(Integer)
    stress_ratio = Column(Float)
    maximum_stress = Column(Float)
    loading_rate = Column(Float)
    run_out = Column(Boolean)


class Results(Base) :
    __tablename__ = "results"
    id = Column(Integer, primary_key=True)
    test_id = Column(Integer, ForeignKey('tests.id'))
    machine_n_cycles = Column(Integer,nullable=False,comment='Number of cycles counted by the machine')
    machine_load = Column(Float,nullable=False,comment='Load measured by the machine')
    machine_displacement = Column(Float,nullable=False,comment='Displacement measured by the machine')
    index = Column(Integer,nullable=True,comment='Image number')
    camera_n_cycles = Column(Integer,nullable=True,comment='Number of cycles counted by the camera')
    exx = Column(Float,nullable=True,comment='Strain measured along main axis')
    eyy = Column(Float,nullable=True,comment='Strain measured along secondary axis')
    exy = Column(Float,nullable=True,comment='Strain measured along a specified axis')
    crack_length = Column(Float,nullable=True,comment='Crack length measurement (for fracture testings)')
    th_time = Column(Integer,nullable=True,comment='Time as counted by temperature monitoring')
    th_n_cycles = Column(Integer,nullable=True,comment='Number of cycles counted by temperature monitoring')
    th_specimen_max = Column(Float,nullable=True,comment='Maximum temperature monitored')
    th_specimen_mean = Column(Float,nullable=True,comment='Mean temperature')
    th_chamber = Column(Float,nullable=True,comment='Temperature of the test environment')
    th_uppergrips = Column(Float,nullable=True,comment='Temperature of the upper grips')
    th_lowergrips = Column(Float,nullable=True,comment='Temperature of the lower grips')




