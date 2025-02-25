# Authors: Abhishek Bhatia, Tomas McIntee, John Powers
# © 2025, The University of North Carolina at Chapel Hill. Permission is granted to use in accordance with the MIT license.
# The code is licensed under the MIT license.


@transform_pandas(
    Output(rid="ri.foundry.main.dataset.048712c7-d1a1-4992-957b-9e98c4647703"),
    All_test=Input(rid="ri.foundry.main.dataset.72d0c73f-b1f5-4625-92cf-86b9497e4faf"),
    go_button=Input(rid="ri.foundry.main.dataset.d1a7fac6-74e1-4a94-b8f5-160cd6e38fba")
)
from pyspark.sql import functions

def base_subset(All_test, go_button):
    All_test = All_test.\
        orderBy("cohort_label").\
        orderBy("person_id")
    cfs_frame = All_test.\
        filter(All_test.cohort_label == "CFS").\
        orderBy(functions.rand(seed = 42)).\
        limit(4500)
    pasc_frame = All_test.\
        filter(All_test.cohort_label == "PASC").\
        orderBy(functions.rand(seed = 42)).\
        limit(4500)
    pasc_cfs_frame = All_test.\
        filter(All_test.cohort_label == "PASC_CFS").\
        orderBy(functions.rand(seed = 42)).\
        limit(500)
    cfs2_frame = All_test.\
        filter(All_test.cohort_label == "R5382").\
        orderBy(functions.rand(seed = 42)).\
        limit(4500)
    ctrl_frame = All_test.\
        filter(All_test.cohort_label == "CTRL").\
        orderBy(functions.rand(seed = 42)).\
        limit(95000)
    full_frame = ctrl_frame.\
        union(pasc_cfs_frame).\
        union(cfs2_frame).\
        union(pasc_frame).\
        union(cfs_frame)
    return(full_frame)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.8ee62ba2-93df-4c6d-8ca4-24ced51beebe"),
    Model_cfs=Input(rid="ri.foundry.main.dataset.cd8dcbf3-815a-4255-b70c-3203627b0659"),
    processed_for_models=Input(rid="ri.foundry.main.dataset.02fa90ae-9d83-488b-8391-964b1d89322b")
)
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType
import numpy as np

def model_scores_cfs(processed_for_models, Model_cfs):
    #Standard model setup:
    model = Model_cfs.stages[0].model
    #count_features = F.udf(lambda row: len([x for x in row if x == 1]), IntegerType())
    #Internal fn:
    def run_model(row):
        arr = np.array([x for x in row])[None,:]
        y_pred =  model.predict_proba(arr)
        value = float(y_pred.squeeze()[1]) # prob of class=1
        return value
    #Define 
    model_udf = F.udf(run_model, FloatType())
    #Now apply 
    predictions = processed_for_models.withColumn("model_score", model_udf(F.struct([processed_for_models[x] for x in processed_for_models.columns if x not in ['person_id']])))
    #Output narrow model score:
    predictions = predictions[['person_id','model_score']]
    return predictions

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.172dd54c-ecee-4a6f-b138-89ea228b2b53"),
    Model_cfs=Input(rid="ri.foundry.main.dataset.cd8dcbf3-815a-4255-b70c-3203627b0659"),
    processed_small=Input(rid="ri.foundry.main.dataset.76b937dc-1ed8-4bc2-9c24-62eba04323e5")
)
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType
import numpy as np

def model_scores_cfs_tiny(Model_cfs, processed_small):
    #Standard model setup:
    model = Model_cfs.stages[0].model
    #count_features = F.udf(lambda row: len([x for x in row if x == 1]), IntegerType())
    #Internal fn:
    def run_model(row):
        arr = np.array([x for x in row])[None,:]
        y_pred =  model.predict_proba(arr)
        value = float(y_pred.squeeze()[1]) # prob of class=1
        return value
    #Define 
    model_udf = F.udf(run_model, FloatType())
    #Now apply 
    predictions = processed_small.withColumn("model_score", model_udf(F.struct([processed_small[x] for x in processed_small.columns if x not in ['person_id']])))
    #Output narrow model score:
    predictions = predictions[['person_id','model_score']]
    return predictions

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.77352559-2c91-4621-812f-95e04e964ce0"),
    Model_pasc=Input(rid="ri.foundry.main.dataset.65ade11e-d9f0-4853-9b74-0e06b33e5ca4"),
    processed_for_models=Input(rid="ri.foundry.main.dataset.02fa90ae-9d83-488b-8391-964b1d89322b")
)
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType
import numpy as np

def model_scores_pasc(processed_for_models, Model_pasc):
    #Standard model setup:
    model = Model_pasc.stages[0].model
    #count_features = F.udf(lambda row: len([x for x in row if x == 1]), IntegerType())
    #Internal fn:
    def run_model(row):
        arr = np.array([x for x in row])[None,:]
        y_pred =  model.predict_proba(arr)
        value = float(y_pred.squeeze()[1]) # prob of class=1
        return value
    #Define 
    model_udf = F.udf(run_model, FloatType())
    #Now apply 
    predictions = processed_for_models.withColumn("model_score", model_udf(F.struct([processed_for_models[x] for x in processed_for_models.columns if x not in ['person_id']])))
    #Output narrow model score:
    predictions = predictions[['person_id','model_score']]
    return predictions

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.22d02628-8b0a-4f69-9fc0-ca99a790fca6"),
    Model_pasc=Input(rid="ri.foundry.main.dataset.65ade11e-d9f0-4853-9b74-0e06b33e5ca4"),
    processed_small=Input(rid="ri.foundry.main.dataset.76b937dc-1ed8-4bc2-9c24-62eba04323e5")
)
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType
import numpy as np

def model_scores_pasc_tiny(processed_small, Model_pasc):
    #Standard model setup:
    model = Model_pasc.stages[0].model
    #count_features = F.udf(lambda row: len([x for x in row if x == 1]), IntegerType())
    #Internal fn:
    def run_model(row):
        arr = np.array([x for x in row])[None,:]
        y_pred =  model.predict_proba(arr)
        value = float(y_pred.squeeze()[1]) # prob of class=1
        return value
    #Define 
    model_udf = F.udf(run_model, FloatType())
    #Now apply 
    predictions = processed_small.withColumn("model_score", model_udf(F.struct([processed_small[x] for x in processed_small.columns if x not in ['person_id']])))
    #Output narrow model score:
    predictions = predictions[['person_id','model_score']]
    return predictions

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.04b70fff-2f74-4b66-9b9f-250125bade16"),
    Model_pvc=Input(rid="ri.foundry.main.dataset.068cf9b5-ee4b-41ac-901a-1cc630df6bb6"),
    processed_pvc=Input(rid="ri.foundry.main.dataset.424a338b-0144-41d6-a80b-1bdb7b7818e5")
)
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType
import numpy as np

def model_scores_pvc(processed_pvc, Model_pvc):
    #Standard model setup:
    model = Model_pvc.stages[0].model
    #count_features = F.udf(lambda row: len([x for x in row if x == 1]), IntegerType())
    #Internal fn:
    def run_model(row):
        arr = np.array([x for x in row])[None,:]
        y_pred =  model.predict_proba(arr)
        value = float(y_pred.squeeze()[1]) # prob of class=1
        return value
    #Define 
    model_udf = F.udf(run_model, FloatType())
    #Now apply 
    predictions = processed_pvc.withColumn("model_score", model_udf(F.struct([processed_pvc[x] for x in processed_pvc.columns if x not in ['person_id']])))
    #Output narrow model score:
    predictions = predictions[['person_id','model_score']]
    return predictions

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.11c90969-5002-4908-ba7d-264d5da8d08f"),
    Model_cfs=Input(rid="ri.foundry.main.dataset.cd8dcbf3-815a-4255-b70c-3203627b0659"),
    small_pvc=Input(rid="ri.foundry.main.dataset.971f17b1-c01d-4912-8e5e-c4e431b5d385")
)
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType
import numpy as np

def model_scores_pvc_small(small_pvc, Model_cfs):
    #Standard model setup:
    model = Model_cfs.stages[0].model
    #count_features = F.udf(lambda row: len([x for x in row if x == 1]), IntegerType())
    #Internal fn:
    def run_model(row):
        arr = np.array([x for x in row])[None,:]
        y_pred =  model.predict_proba(arr)
        value = float(y_pred.squeeze()[1]) # prob of class=1
        return value
    #Define 
    model_udf = F.udf(run_model, FloatType())
    #Now apply 
    predictions = small_pvc.withColumn("model_score", model_udf(F.struct([small_pvc[x] for x in small_pvc.columns if x not in ['person_id']])))
    #Output narrow model score:
    predictions = predictions[['person_id','model_score']]
    return predictions

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.02fa90ae-9d83-488b-8391-964b1d89322b"),
    All_test=Input(rid="ri.foundry.main.dataset.72d0c73f-b1f5-4625-92cf-86b9497e4faf"),
    date_filtered_first_last=Input(rid="ri.foundry.main.dataset.c3ffb7bf-1b02-45cb-afe9-414c6d361637")
)
def processed_for_models( All_test, date_filtered_first_last):
    selected_conditions = date_filtered_first_last.groupBy('person_id')\
        .pivot('condition_concept_id')\
        .sum('one_or_many_conds')\
        .join(All_test['person_id','age_bin','is_female'], on = 'person_id', how = 'outer')\
        .na.fill(value = 0)
    return(selected_conditions)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.424a338b-0144-41d6-a80b-1bdb7b7818e5"),
    All_test=Input(rid="ri.foundry.main.dataset.72d0c73f-b1f5-4625-92cf-86b9497e4faf"),
    firstlast_pvc=Input(rid="ri.foundry.main.dataset.816b3fc0-57ac-4f11-b3af-8b0e1264fd7d")
)
def processed_pvc( All_test, firstlast_pvc):
    selected_conditions = firstlast_pvc.groupBy('person_id')\
        .pivot('condition_concept_id')\
        .sum('one_or_many_conds')\
        .join(All_test['person_id','age_bin','is_female'], on = 'person_id', how = 'outer')\
        .na.fill(value = 0)
    return(selected_conditions)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.50a25c96-de02-434a-946a-688c88ba5eda"),
    conditions_cohort=Input(rid="ri.foundry.main.dataset.6008ea8f-40c7-47a8-ab22-4cd0e518c22e"),
    modified_rollup_lookup=Input(rid="ri.foundry.main.dataset.80a77caf-1c2f-4713-832c-2b75f3ebb97f")
)
def rolled_conditions(modified_rollup_lookup, conditions_cohort):
    rolled_up = conditions_cohort \
        .join(modified_rollup_lookup, conditions_cohort['condition_concept_id'] == modified_rollup_lookup['descendant_concept_id'])
    rolled_up = rolled_up[['person_id','ancestor_concept_id','anc_concept_name','condition_start_date','condition_end_date','window_start','window_end']] \
        .withColumnRenamed('ancestor_concept_id','condition_concept_id') \
        .withColumnRenamed('anc_concept_name','condition_concept_name') \
        .distinct()
    return rolled_up
    

