# Authors: Abhishek Bhatia, Tomas McIntee, John Powers
# © 2025, The University of North Carolina at Chapel Hill. Permission is granted to use in accordance with the MIT license.
# The code is licensed under the MIT license.


@transform_pandas(
    Output(rid="ri.foundry.main.dataset.6cf47362-6f83-4aec-96e9-b72095a38154"),
    set_window_dates=Input(rid="ri.foundry.main.dataset.2f6de052-a84e-491d-9329-6ac7d2dddd10")
)
def CFS_train(set_window_dates):
    cfs_cohort = set_window_dates.filter("cohort_label == 'CFS' or cohort_label == 'PASC_CFS'") \
        .withColumn("window_start",set_window_dates.window_start + (set_window_dates.cfs_index - set_window_dates.index_date)) \
        .withColumn("window_end",set_window_dates.window_end + (set_window_dates.cfs_index - set_window_dates.index_date)) \
        .withColumn("pre_covid",set_window_dates.pre_covid_cfs)
    cfs_cohort_train = cfs_cohort.sample(False,0.8, seed = 37)
    return cfs_cohort_train

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.1a5c5bce-6d55-41e6-b1f8-ddeabace3bf6"),
    set_window_dates=Input(rid="ri.foundry.main.dataset.2f6de052-a84e-491d-9329-6ac7d2dddd10")
)
def CTRL_train( set_window_dates):
    ctrl_cohort = set_window_dates.filter("cohort_label == 'CTRL'")
    sick_cohort = set_window_dates.filter("cohort_label != 'CTRL'")
    ctrl_cohort_train = ctrl_cohort.sample(False,2*sick_cohort.count()/ctrl_cohort.count(), seed = 37)
    return ctrl_cohort_train

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.69d89ac0-f1d2-464d-a87c-988f1e0f81ed"),
    set_window_dates=Input(rid="ri.foundry.main.dataset.2f6de052-a84e-491d-9329-6ac7d2dddd10")
)
def PASC_train(set_window_dates):
    pasc_cohort = set_window_dates.filter("cohort_label == 'PASC' or cohort_label == 'PASC_CFS'") \
        .withColumn("window_start",set_window_dates.window_start + (set_window_dates.pasc_index - set_window_dates.index_date)) \
        .withColumn("window_end",set_window_dates.window_end + (set_window_dates.pasc_index - set_window_dates.index_date)) \
        .withColumn("pre_covid",set_window_dates.pre_covid_pasc)
    pasc_cohort_train = pasc_cohort.sample(False,0.8, seed = 37)
    return pasc_cohort_train

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.3ef4e2e9-6d40-4e6a-804f-2d31faf7aafd"),
    Full_cohort=Input(rid="ri.foundry.main.dataset.5aa7b269-ea4c-4978-9cf5-5dbf92373cca"),
    blackout_dates=Input(rid="ri.foundry.main.dataset.678bd91e-e79d-49a2-8701-4cb6b552d167"),
    cohort_and_idx=Input(rid="ri.foundry.main.dataset.f195a364-30d1-490f-b70a-f64da43479a1"),
    recover_release_condition_occurrence=Input(rid="ri.foundry.main.dataset.ef3dcd85-f845-4710-923f-ceaeb71155e4")
)
# Removes blackout conditions

def conditions_blacked_out( blackout_dates, recover_release_condition_occurrence, Full_cohort, cohort_and_idx):
    # join the cohort with the blackout dates
    df = Full_cohort.join(cohort_and_idx, on = 'person_id')
    df = df.groupBy('person_id').count()
    bd = blackout_dates
    df = df.join(bd, on='person_id',how='left')
    df = df[['person_id','blackout_begin','blackout_end']]
    
    # trim the conditions to the bare minimum required for output to save memory where possible
    cond = recover_release_condition_occurrence
    cond = cond[['person_id','condition_occurrence_id','condition_start_date','condition_end_date','condition_concept_id','condition_concept_name']]
    
    # join blackout dates to find all the blacked out condition_occurrence_id's
    df = df.join(cond, on='person_id')
    df = df.filter(df['condition_start_date'] >= df['blackout_begin'])
    df = df.filter(df['condition_start_date'] <= df['blackout_end'])
    df = df[['condition_occurrence_id']]

    # anti join to rid ourselves of the blacked out visits
    out = cond.join(df, on='condition_occurrence_id', how='leftanti')
    return out

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.6bd8d2a5-17cc-47e2-b1fe-2f8e5ef07d9e"),
    training_set_cfs=Input(rid="ri.foundry.main.dataset.89f8dbfc-6f37-42ce-bece-5d2fd6c9455b")
)
from xgboost.sklearn import XGBClassifier
from foundry_ml import Model, Stage
from pandas import DataFrame

def feature_importance_cfs(training_set_cfs):
    df = training_set_cfs.toPandas()
    y = (df["sick"].to_numpy())
    X = df.drop(columns = ["sick", "person_id"])
    X = X.to_numpy()
    model =  XGBClassifier(colsample_bytree=0.1, gamma=0.4, learning_rate=0.09, max_depth=12, min_child_weight=0, n_estimators=400, subsample=0.9, random_state=42)
    model.fit(X, y)
    return(DataFrame(model.feature_importances_))

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b96eb1ef-ab6d-423f-a3f9-b45fe5f0d71d"),
    training_set_pasc=Input(rid="ri.foundry.main.dataset.4827c81f-824f-4dd2-9ae9-2bee586545f5")
)
from xgboost.sklearn import XGBClassifier
from foundry_ml import Model, Stage
from pandas import DataFrame

def feature_importance_pasc(training_set_pasc):
    df = training_set_pasc.toPandas()
    y = (df["sick"].to_numpy())
    X = df.drop(columns = ["sick", "person_id"])
    X = X.to_numpy()
    model =  XGBClassifier(colsample_bytree=0.1, gamma=0.4, learning_rate=0.09, max_depth=12, min_child_weight=0, n_estimators=400, subsample=0.9, random_state=42)
    model.fit(X, y)
    return(DataFrame(model.feature_importances_))

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.7efa8345-979d-4175-9b72-058ab250ad34"),
    training_set_pvc=Input(rid="ri.foundry.main.dataset.9faa1560-514c-484b-b31b-e87c9a946c2d")
)
from xgboost.sklearn import XGBClassifier
from foundry_ml import Model, Stage
from pandas import DataFrame

def feature_importance_pvc(training_set_pvc):
    df = training_set_pvc.toPandas()
    y = (df["sick"].to_numpy())
    X = df.drop(columns = ["sick", "person_id"])
    X = X.to_numpy()
    model =  XGBClassifier(colsample_bytree=0.1, gamma=0.4, learning_rate=0.09, max_depth=12, min_child_weight=0, n_estimators=400, subsample=0.9, random_state=42)
    model.fit(X, y)
    return(DataFrame(model.feature_importances_))

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.cd8dcbf3-815a-4255-b70c-3203627b0659"),
    training_set_cfs=Input(rid="ri.foundry.main.dataset.89f8dbfc-6f37-42ce-bece-5d2fd6c9455b")
)
from xgboost.sklearn import XGBClassifier
from foundry_ml import Model, Stage

def model_CFS(training_set_cfs):
    df = training_set_cfs.toPandas()
    y = (df["sick"].to_numpy())
    X = df.drop(columns = ["sick", "person_id"])
    X = X.to_numpy()
    model =  XGBClassifier(colsample_bytree=0.1, gamma=0.4, learning_rate=0.09, max_depth=12, min_child_weight=0, n_estimators=400, subsample=0.9, random_state=42)
    model.fit(X, y)
    return Model(Stage(model))

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.65ade11e-d9f0-4853-9b74-0e06b33e5ca4"),
    training_set_pasc=Input(rid="ri.foundry.main.dataset.4827c81f-824f-4dd2-9ae9-2bee586545f5")
)
from xgboost.sklearn import XGBClassifier
from foundry_ml import Model, Stage

def model_PASC(training_set_pasc):
    df = training_set_pasc.toPandas()
    y = (df["sick"].to_numpy())
    X = df.drop(columns = ["sick", "person_id"])
    X = X.to_numpy()
    model =  XGBClassifier(colsample_bytree=0.1, gamma=0.4, learning_rate=0.09, max_depth=12, min_child_weight=0, n_estimators=400, subsample=0.9, random_state=42)
    model.fit(X, y)
    return Model(Stage(model))

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.068cf9b5-ee4b-41ac-901a-1cc630df6bb6"),
    training_set_pvc=Input(rid="ri.foundry.main.dataset.9faa1560-514c-484b-b31b-e87c9a946c2d")
)
from xgboost.sklearn import XGBClassifier
from foundry_ml import Model, Stage

def model_PVC(training_set_pvc):
    df = training_set_pvc.toPandas()
    y = (df["sick"].to_numpy())
    X = df.drop(columns = ["sick", "person_id"])
    X = X.to_numpy()
    model =  XGBClassifier(colsample_bytree=0.1, gamma=0.4, learning_rate=0.09, max_depth=12, min_child_weight=0, n_estimators=400, subsample=0.9, random_state=42)
    model.fit(X, y)
    return Model(Stage(model))

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.7d0f2f21-8355-402a-a88d-afe3b3d77a7c"),
    modified_rollup_lookup=Input(rid="ri.foundry.main.dataset.80a77caf-1c2f-4713-832c-2b75f3ebb97f"),
    training_conditions_cfs=Input(rid="ri.foundry.main.dataset.c0393753-2713-4099-ad73-79c283964b7d")
)
def rolled_conditions_cfs( training_conditions_cfs, modified_rollup_lookup):
    training_conditions = training_conditions_cfs
    rolled_up = training_conditions \
        .join(modified_rollup_lookup, training_conditions['condition_concept_id'] == modified_rollup_lookup['descendant_concept_id'])
    rolled_up = rolled_up[['person_id','ancestor_concept_id','anc_concept_name','sick','cohort_label','pre_covid','condition_start_date','condition_end_date']] \
        .withColumnRenamed('ancestor_concept_id','condition_concept_id') \
        .withColumnRenamed('anc_concept_name','condition_concept_name') \
        .distinct()
    return rolled_up
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.330d9332-009f-4ae4-ac33-d0d6757488f9"),
    modified_rollup_lookup=Input(rid="ri.foundry.main.dataset.80a77caf-1c2f-4713-832c-2b75f3ebb97f"),
    training_conditions_pasc=Input(rid="ri.foundry.main.dataset.d24eba68-b481-464a-a2f9-54469f8405e2")
)
def rolled_conditions_pasc( training_conditions_pasc, modified_rollup_lookup):
    training_conditions = training_conditions_pasc
    rolled_up = training_conditions \
        .join(modified_rollup_lookup, training_conditions['condition_concept_id'] == modified_rollup_lookup['descendant_concept_id'])
    rolled_up = rolled_up[['person_id','ancestor_concept_id','anc_concept_name','sick','cohort_label','pre_covid','condition_start_date','condition_end_date']] \
        .withColumnRenamed('ancestor_concept_id','condition_concept_id') \
        .withColumnRenamed('anc_concept_name','condition_concept_name') \
        .distinct()
    return rolled_up
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f4a70e78-cc79-457b-98df-7256d60b09bc"),
    modified_rollup_lookup=Input(rid="ri.foundry.main.dataset.80a77caf-1c2f-4713-832c-2b75f3ebb97f"),
    training_conditions_pvc=Input(rid="ri.foundry.main.dataset.2831cf63-aa53-4acf-b639-b27cfeb7ae65")
)
def rolled_conditions_pvc( training_conditions_pvc, modified_rollup_lookup):
    training_conditions = training_conditions_pvc
    rolled_up = training_conditions \
        .join(modified_rollup_lookup, training_conditions['condition_concept_id'] == modified_rollup_lookup['descendant_concept_id'])
    rolled_up = rolled_up[['person_id','ancestor_concept_id','anc_concept_name','sick','cohort_label','pre_covid','condition_start_date','condition_end_date']] \
        .withColumnRenamed('ancestor_concept_id','condition_concept_id') \
        .withColumnRenamed('anc_concept_name','condition_concept_name') \
        .distinct()
    return rolled_up
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.76c1f9c9-60a4-487c-9ee4-5c10262800b3"),
    rolled_conditions_cfs=Input(rid="ri.foundry.main.dataset.7d0f2f21-8355-402a-a88d-afe3b3d77a7c"),
    rolled_conditions_pasc=Input(rid="ri.foundry.main.dataset.330d9332-009f-4ae4-ac33-d0d6757488f9")
)
from pyspark.sql import functions
from pyspark.sql.functions import when

import pandas
import numpy

def selected_features(rolled_conditions_cfs, rolled_conditions_pasc):
    #Parameters:
    select_features = 200 #Number to use with each
    #Drop "naughty" conditions
    drop_conditions = [
        # Roll-up misc bin:
        '4041283',  # General finding of observation of patient
        # COVID conditions:        
        '37311061', # COVID-19
        '4100065',  # Disease due to Coronaviridae
        '3661408',  # Pneumonia due to SARS-COV-2
        '4195694',  # Acute respiratory distress syndrome (ARDS).
        # Explicit sequelae diagnosis list follows:
        '36714927', # Sequelae of infectious disease
        '432738',   # Chronic fatigue syndrome
        '44793521', # Also CFS
        '44793522', # Also CFS
        '44793523', # Also CFS
        '40405599', # Fibromyalgia
        '444205',   # Disorder following viral disease
        '4202045',  # Postviral fatigue syndrome (G9.93)
        '705076',   # PASC (U09.9)
        '4159659'   # Postural orthostatic tachycardia syndrome (POTS, G90.A)
        ] 
    #Drop extra columns, make distinct().
    rolled_conditions_cfs = rolled_conditions_cfs[['person_id','sick','condition_concept_id','condition_concept_name']].distinct()
    rolled_conditions_pasc = rolled_conditions_pasc[['person_id','sick','condition_concept_id','condition_concept_name']].distinct()
    #Calculations:
    obs_cfs = rolled_conditions_cfs[['person_id']].distinct().count()
    obs_pasc = rolled_conditions_pasc[['person_id']].distinct().count()
    pos_cfs = rolled_conditions_cfs.filter(rolled_conditions_cfs.sick)[['person_id']].distinct().count()
    pos_pasc = rolled_conditions_pasc.filter(rolled_conditions_pasc.sick)[['person_id']].distinct().count()
    ev_cfs = pos_cfs/obs_cfs
    ev_pasc = pos_pasc/obs_pasc
    without_naughty_cfs = rolled_conditions_cfs.filter(rolled_conditions_cfs['condition_concept_id'].isin(drop_conditions) == False)
    without_naughty_pasc = rolled_conditions_pasc.filter(rolled_conditions_pasc['condition_concept_id'].isin(drop_conditions) == False)
    #Add means:
    with_means_cfs = without_naughty_cfs.groupBy('condition_concept_id').count()\
        .withColumn('mean',functions.col('count') / obs_cfs)\
        .drop('count')\
        .join(without_naughty_cfs, on='condition_concept_id')
    with_means_pasc = without_naughty_pasc.groupBy('condition_concept_id').count()\
        .withColumn('mean',functions.col('count') / obs_pasc)\
        .drop('count')\
        .join(without_naughty_pasc, on='condition_concept_id')
    #Calculate covariance
    cov_calced_cfs = with_means_cfs.withColumn('label', with_means_cfs.sick.cast("float"))\
        .withColumn('value', (1 - functions.col('mean')) * (functions.col('label') - ev_cfs) / obs_cfs)\
        .groupBy('condition_concept_name','condition_concept_id')\
        .agg(functions.abs(functions.sum('value')).alias('cfs_abs_covariance'),functions.sum('value').alias('cfs_covariance'))\
        .toPandas()\
        .sort_values(by = 'cfs_abs_covariance', ascending = False)
    cov_calced_pasc = with_means_pasc.withColumn('label', with_means_pasc.sick.cast("float"))\
        .withColumn('value', (1 - functions.col('mean')) * (functions.col('label') - ev_pasc) / obs_pasc)\
        .groupBy('condition_concept_name','condition_concept_id')\
        .agg(functions.abs(functions.sum('value')).alias('pasc_abs_covariance'),functions.sum('value').alias('pasc_covariance'))\
        .toPandas()\
        .sort_values(by = 'pasc_abs_covariance', ascending = False)
    
    cov_calced_all = cov_calced_cfs.merge(cov_calced_pasc, on = ['condition_concept_name','condition_concept_id'],how ='outer')
    cov_calced_pasc_short = cov_calced_all\
        .sort_values(by = 'pasc_abs_covariance', ascending = False)\
        .head(select_features)
    cov_calced_cfs_short = cov_calced_all\
        .sort_values(by = 'cfs_abs_covariance', ascending = False)\
        .head(select_features)
    cov_calced_all_short = cov_calced_cfs_short.merge(cov_calced_pasc_short, on = ['condition_concept_name','condition_concept_id','pasc_abs_covariance','cfs_abs_covariance','pasc_covariance','cfs_covariance'], how = 'outer')
    cov_calced_all_short['covariance_indicator'] = numpy.where((cov_calced_all_short.pasc_covariance < 0) & (cov_calced_all_short.cfs_covariance < 0), 'negative', 
                       numpy.where((cov_calced_all_short.pasc_covariance > 0) & (cov_calced_all_short.cfs_covariance > 0), 'positive', 'mixed'))
    return(cov_calced_all_short)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f40c4669-9259-4f83-bf23-731023691925"),
    rolled_conditions_pvc=Input(rid="ri.foundry.main.dataset.f4a70e78-cc79-457b-98df-7256d60b09bc")
)
from pyspark.sql import functions
from pyspark.sql.functions import when

import pandas
import numpy

def selected_features_pvc(rolled_conditions_pvc):
    #Parameters:
    select_features = 200 #Number to use with each
    #Drop "naughty" conditions
    drop_conditions = [
        # Roll-up misc bin:
        '4041283',  # General finding of observation of patient
        # COVID conditions:        
        '37311061', # COVID-19
        '4100065',  # Disease due to Coronaviridae
        '3661408',  # Pneumonia due to SARS-COV-2
        '4195694',  # Acute respiratory distress syndrome (ARDS).
        # Explicit sequelae diagnosis list follows:
        '36714927', # Sequelae of infectious disease
        '432738',   # Chronic fatigue syndrome
        '44793521', # Also CFS
        '44793522', # Also CFS
        '44793523', # Also CFS
        '40405599', # Fibromyalgia
        '444205',   # Disorder following viral disease
        '4202045',  # Postviral fatigue syndrome (G9.93)
        '705076',   # PASC (U09.9)
        '4159659'   # Postural orthostatic tachycardia syndrome (POTS, G90.A)
        ] 
    #Drop extra columns, make distinct().
    rolled_conditions_pvc = rolled_conditions_pvc[['person_id','sick','condition_concept_id','condition_concept_name']].distinct()
    #Calculations:
    obs_cfs = rolled_conditions_pvc[['person_id']].distinct().count()
    pos_cfs = rolled_conditions_pvc.filter(rolled_conditions_pvc.sick)[['person_id']].distinct().count()
    ev_cfs = pos_cfs/obs_cfs
    without_naughty_cfs = rolled_conditions_pvc.filter(rolled_conditions_pvc['condition_concept_id'].isin(drop_conditions) == False)
    #Add means:
    with_means_cfs = without_naughty_cfs.groupBy('condition_concept_id').count()\
        .withColumn('mean',functions.col('count') / obs_cfs)\
        .drop('count')\
        .join(without_naughty_cfs, on='condition_concept_id')
    #Calculate covariance
    cov_calced_cfs = with_means_cfs.withColumn('label', with_means_cfs.sick.cast("float"))\
        .withColumn('value', (1 - functions.col('mean')) * (functions.col('label') - ev_cfs) / obs_cfs)\
        .groupBy('condition_concept_name','condition_concept_id')\
        .agg(functions.abs(functions.sum('value')).alias('cfs_abs_covariance'),functions.sum('value').alias('cfs_covariance'))\
        .toPandas()\
        .sort_values(by = 'cfs_abs_covariance', ascending = False)
    
    cov_calced_cfs_short = cov_calced_cfs\
        .sort_values(by = 'cfs_abs_covariance', ascending = False)\
        .head(select_features)
    cov_calced_cfs_short['covariance_indicator'] = numpy.where((cov_calced_cfs_short.cfs_covariance < 0), 'negative', 'positive')
    return(cov_calced_cfs_short)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.92fac7ea-c839-4761-8d70-6db2d57006d8"),
    model_PASC=Input(rid="ri.foundry.main.dataset.65ade11e-d9f0-4853-9b74-0e06b33e5ca4"),
    selected_features=Input(rid="ri.foundry.main.dataset.76c1f9c9-60a4-487c-9ee4-5c10262800b3"),
    training_set_pasc=Input(rid="ri.foundry.main.dataset.4827c81f-824f-4dd2-9ae9-2bee586545f5")
)
from foundry_ml import Model, Stage
from pandas import DataFrame
import shap
import matplotlib
from matplotlib import pyplot

def shap_summary_PASC( training_set_pasc, selected_features, model_PASC):
    #Transformations and plot prep
    matplotlib.rc('font', size=10)
    fd = selected_features.toPandas()
    mapper = {}
    max_name_len = 50
    for i_row in range(fd.shape[0]):
        concept_id = str(fd['condition_concept_id'][i_row])
        concept_name = fd['condition_concept_name'][i_row]
        concept_name = concept_name.replace(' ','_').replace('(','_').replace(')','_')[:max_name_len]
        mapper[concept_id] = concept_name

    added_features = ['is_female', 'age_bin']
    for feat_name in added_features:
        mapper[feat_name] = feat_name
    
    keys = list(mapper.keys())
    values = [mapper[key] for key in keys]
    for i_val, value in enumerate(values):
        if value in values[:i_val]:
            value = '_' + value
            mapper[keys[i_val]] = value
    
    df = training_set_pasc.toPandas()
    X_train = df.drop(columns = ["person_id","sick"])
    X_train = X_train.rename(columns=mapper)
    print(X_train.columns)
    if len(set(X_train.columns)) < len(X_train.columns):
        raise Exception
    #Shapley values
    model = model_PASC.stages[0].model
    shap_values = shap.TreeExplainer(model).shap_values(X_train, approximate=False, check_additivity=False)
    shap.summary_plot(shap_values, X_train, plot_size=(38,15), max_display=50) 
    pyplot.show()
    #Adjust for sign-correlated shapleys
    df1 = DataFrame(shap_values) #Causes 0 values to be negative.
    df2 = X_train.sub(0.5)
    df1.columns = df2.columns.str.replace(',', '').str.replace(')','').str.replace('(','').str.replace(',','')
    df2.columns = df2.columns.str.replace(',', '').str.replace(')','').str.replace('(','').str.replace(',','')
    df3 = 0.5 * df1 / df2
    df3.columns = df3.columns
    return(df3)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.3696bc1f-d12b-43b2-8867-ce0df945c0cb"),
    model_PVC=Input(rid="ri.foundry.main.dataset.068cf9b5-ee4b-41ac-901a-1cc630df6bb6"),
    selected_features=Input(rid="ri.foundry.main.dataset.76c1f9c9-60a4-487c-9ee4-5c10262800b3"),
    training_set_pvc=Input(rid="ri.foundry.main.dataset.9faa1560-514c-484b-b31b-e87c9a946c2d")
)
from foundry_ml import Model, Stage
from pandas import DataFrame
import shap
import matplotlib
from matplotlib import pyplot

def shap_summary_PVC(model_PVC, training_set_pvc, selected_features):
    #Transformations and plot prep
    matplotlib.rc('font', size=10)
    fd = selected_features.toPandas()
    mapper = {}
    max_name_len = 50
    for i_row in range(fd.shape[0]):
        concept_id = str(fd['condition_concept_id'][i_row])
        concept_name = fd['condition_concept_name'][i_row]
        concept_name = concept_name.replace(' ','_').replace('(','_').replace(')','_')[:max_name_len]
        mapper[concept_id] = concept_name

    added_features = ['is_female', 'age_bin']
    for feat_name in added_features:
        mapper[feat_name] = feat_name
    
    keys = list(mapper.keys())
    values = [mapper[key] for key in keys]
    for i_val, value in enumerate(values):
        if value in values[:i_val]:
            value = '_' + value
            mapper[keys[i_val]] = value
    
    df = training_set_pvc.toPandas()
    X_train = df.drop(columns = ["person_id","sick"])
    X_train = X_train.rename(columns=mapper)
    print(X_train.columns)
    if len(set(X_train.columns)) < len(X_train.columns):
        raise Exception
    #Shapley values
    model = model_PVC.stages[0].model
    shap_values = shap.TreeExplainer(model).shap_values(X_train, approximate=False, check_additivity=False)
    shap.summary_plot(shap_values, X_train, plot_size=(38,15), max_display=50) 
    pyplot.show()
    #Adjust for sign-correlated shapleys
    df1 = DataFrame(shap_values) #Causes 0 values to be negative.
    df2 = X_train.sub(0.5)
    df1.columns = df2.columns.str.replace(',', '').str.replace(')','').str.replace('(','')
    df3 = 2 * df1 / df2
    df3.columns = df3.columns
    return(df1)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.806018de-b2fd-4aaa-81d8-23306f8e4f8e"),
    model_CFS=Input(rid="ri.foundry.main.dataset.cd8dcbf3-815a-4255-b70c-3203627b0659"),
    selected_features=Input(rid="ri.foundry.main.dataset.76c1f9c9-60a4-487c-9ee4-5c10262800b3"),
    training_set_cfs=Input(rid="ri.foundry.main.dataset.89f8dbfc-6f37-42ce-bece-5d2fd6c9455b")
)
from foundry_ml import Model, Stage
from pandas import DataFrame
import shap
import matplotlib
from matplotlib import pyplot

def shap_summary_cfs(model_CFS, training_set_cfs, selected_features):
    #Transformations and plot prep
    matplotlib.rc('font', size=10)
    fd = selected_features.toPandas()
    mapper = {}
    max_name_len = 50
    for i_row in range(fd.shape[0]):
        concept_id = str(fd['condition_concept_id'][i_row])
        concept_name = fd['condition_concept_name'][i_row]
        concept_name = concept_name.replace(' ','_').replace('(','_').replace(')','_')[:max_name_len]
        mapper[concept_id] = concept_name

    added_features = ['is_female', 'age_bin']
    for feat_name in added_features:
        mapper[feat_name] = feat_name
    
    keys = list(mapper.keys())
    values = [mapper[key] for key in keys]
    for i_val, value in enumerate(values):
        if value in values[:i_val]:
            value = '_' + value
            mapper[keys[i_val]] = value
    
    df = training_set_cfs.toPandas()
    X_train = df.drop(columns = ["person_id","sick"])
    X_train = X_train.rename(columns=mapper)
    print(X_train.columns)
    if len(set(X_train.columns)) < len(X_train.columns):
        raise Exception
    #Shapley values
    model = model_CFS.stages[0].model
    shap_values = shap.TreeExplainer(model).shap_values(X_train, approximate=False, check_additivity=False)
    shap.summary_plot(shap_values, X_train, plot_size=(38,15), max_display=50) 
    pyplot.show()
    #Adjust for sign-correlated shapleys
    df1 = DataFrame(shap_values) #Causes 0 values to be negative.
    df2 = X_train.sub(0.5)
    df1.columns = df2.columns.str.replace(',', '').str.replace(')','').str.replace('(','').str.replace(',','')
    df2.columns = df2.columns.str.replace(',', '').str.replace(')','').str.replace('(','').str.replace(',','')
    df3 = 0.5 * df1 / df2
    df3.columns = df3.columns
    return(df3)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.89f8dbfc-6f37-42ce-bece-5d2fd6c9455b"),
    cond_counts_cfs=Input(rid="ri.foundry.main.dataset.8f8e454b-4f30-4b8a-9dc1-25f43c67891e"),
    matched_CFS=Input(rid="ri.foundry.main.dataset.8e524689-b720-4f54-a13b-c0ab7275e0ad"),
    selected_features=Input(rid="ri.foundry.main.dataset.76c1f9c9-60a4-487c-9ee4-5c10262800b3")
)
def training_set_cfs(selected_features, matched_CFS, cond_counts_cfs):
    conds_counts = cond_counts_cfs
    selected_conditions = selected_features.join(conds_counts, on = 'condition_concept_id')\
        .groupBy('person_id')\
        .pivot('condition_concept_id')\
        .sum('one_or_many_conds')\
        .join(matched_CFS['person_id','age_bin','is_female','sick'], on = 'person_id', how = 'outer')\
        .na.fill(value = 0)
    return(selected_conditions)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.4827c81f-824f-4dd2-9ae9-2bee586545f5"),
    conds_counts_pasc=Input(rid="ri.foundry.main.dataset.51de9c2d-e32d-4f83-81df-cd415988987d"),
    matched_PASC=Input(rid="ri.foundry.main.dataset.9f95b1fc-c284-4492-8e78-0c08e17b1b73"),
    selected_features=Input(rid="ri.foundry.main.dataset.76c1f9c9-60a4-487c-9ee4-5c10262800b3")
)
def training_set_pasc( matched_PASC, selected_features, conds_counts_pasc):
    conds_counts = conds_counts_pasc
    selected_conditions = selected_features.join(conds_counts, on = 'condition_concept_id')\
        .groupBy('person_id')\
        .pivot('condition_concept_id')\
        .sum('one_or_many_conds')\
        .join(matched_PASC['person_id','age_bin','is_female','sick'], on = 'person_id', how = 'outer')\
        .na.fill(value = 0)
    return(selected_conditions)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.9faa1560-514c-484b-b31b-e87c9a946c2d"),
    cond_counts_pvc=Input(rid="ri.foundry.main.dataset.88ee2cef-070a-4c23-8a9b-e44f0b9af91d"),
    matched_pvc=Input(rid="ri.foundry.main.dataset.7cb1ce32-b984-407b-9064-0ac46535d157"),
    selected_features=Input(rid="ri.foundry.main.dataset.76c1f9c9-60a4-487c-9ee4-5c10262800b3")
)
def training_set_pvc( matched_pvc, cond_counts_pvc, selected_features):
    conds_counts = cond_counts_pvc
    selected_conditions = selected_features.join(conds_counts, on = 'condition_concept_id')\
        .groupBy('person_id')\
        .pivot('condition_concept_id')\
        .sum('one_or_many_conds')\
        .join(matched_pvc['person_id','age_bin','is_female','sick'], on = 'person_id', how = 'outer')\
        .na.fill(value = 0)
    return(selected_conditions)

