import pandas as pd
import os
from ccfatigue.services.database import Base, database, engine
from bokeh.plotting import figure, output_file, show
from bokeh.io import show, output_file
from bokeh.plotting import figure, output_file, show


import matplotlib
import matplotlib.pyplot as plt
import random
import itertools

# #dbConnection    = engine.connect()


from ccfatigue.models.database import *
print(Results.__table__.columns.keys())


def plot_stress_ratio_vs_max_load() :
    df_results = pd.read_sql("SELECT test_id, max(machine_load) FROM public.results Group by test_id", engine);
    df_tests = pd.read_sql("SELECT id, stress_ratio, experiment_id	FROM public.tests", engine);
    df_experiment = pd.read_sql("SELECT id as experiment_id, concat(laboratory,' ',researcher,' ', date,' ',(select name from type where id = test_type_id))as label 	FROM public.experiment;", engine);

    # df joining and processing
    df_results_tests = pd.concat([df_results.set_index('test_id'),df_tests.set_index('id')], axis=1, join='inner')
    df_results_test_max = df_results_tests.groupby(['stress_ratio','experiment_id'], sort=False)['max'].max()
    df_results_test_max = df_results_test_max.reset_index()
    df_result_test_max_experiment = df_results_test_max.merge(df_experiment,on='experiment_id',how='inner')

    # Create bokeh plot
    output_file('my_first_graph.html',title="Plot thickens")
    p = figure(x_axis_label='Stress ratio',y_axis_label='Maximum machine load')
    grouped = df_result_test_max_experiment.groupby(by=["label"])
    for i , (name, data) in enumerate(grouped) :
        r = random.randint(0,255)
        b = random.randint(0,255)
        g = random.randint(0,255)
        p.line(source=data[['stress_ratio','max']],x='stress_ratio', y='max', legend_label=name,color=(r, g, b))

    show(p)

def plot_stress_ratio_per_experiment():

    df_tests = pd.read_sql("SELECT id, stress_ratio, experiment_id	FROM public.tests", engine);
    df_experiment = pd.read_sql("SELECT id as experiment_id, concat(laboratory,' ',researcher,' ', date,' ',(select name from type where id = test_type_id))as label 	FROM public.experiment;", engine);
    df_test_experiment = df_tests.merge(df_experiment,on='experiment_id',how='inner')
    df_test_experiment = df_test_experiment[['label','stress_ratio']]
    df_test_experiment = df_test_experiment.groupby(['label','stress_ratio']).size().unstack(level=1)
    df_test_experiment.plot(kind='bar')
    plt.xticks(rotation=0)
    plt.ylabel('Number of stress ratio per experiment')
    plt.show()



def plot_maximum_machineload_per_experiment():

    # maximum machineload per experiment
    results = pd.read_sql("SELECT test_id, max(machine_load) FROM public.results Group by test_id", engine);
    tests = pd.read_sql("SELECT id as test_id, experiment_id FROM public.tests;", engine);
    experiment = pd.read_sql("SELECT id as experiment_id, concat(laboratory,' ',researcher,' ', date,' ',(select name from type where id = test_type_id))as label 	FROM public.experiment;", engine);



    tests_experiment = pd.merge(tests,experiment,how='left',on='experiment_id' )
    results_tests_experiment = pd.merge(results,tests_experiment,how='left',on='test_id' )
    results_tests_experiment = results_tests_experiment[["max","label"]]
    results_tests_experiment_max = results_tests_experiment.groupby(['label'], sort=False)['max'].max()
    print(results_tests_experiment_max)

    # results_tests = pd.concat([results.set_index('test_id'), tests.set_index('test_id')], axis=1, join='inner')
    #
    # results_tests_max = results_tests.groupby(['experiment_id'], sort=False)['max'].max()
    #
    # print(results_tests_max)
    #
    results_tests_experiment_max.plot.bar()
    plt.xticks(rotation=0)
    plt.ylabel('maximum machineload per experiment')
    plt.show()





