# Authors: Abhishek Bhatia, Tomas McIntee, John Powers
# © 2025, The University of North Carolina at Chapel Hill. Permission is granted to use in accordance with the MIT license.
# The code is licensed under the MIT license.

import pyspark.sql.functions as F

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.1b98aa72-961f-48ca-a8eb-9945e1635961"),
    cohort_conditions=Input(rid="ri.foundry.main.dataset.7f63a854-3aa7-4b37-89cb-d86a8002c4e7"),
    modified_rollup_lookup=Input(rid="ri.foundry.main.dataset.80a77caf-1c2f-4713-832c-2b75f3ebb97f")
)
def apply_rollup(modified_rollup_lookup, cohort_conditions):
    Modified_rollup_lookup = modified_rollup_lookup
    rolled_up = cohort_conditions \
        .join(modified_rollup_lookup, cohort_conditions['condition_concept_id'] == modified_rollup_lookup['descendant_concept_id'])
    rolled_up = rolled_up[['person_id','ancestor_concept_id','anc_concept_name','condition_start_date','condition_end_date','window_start','window_end']] \
        .withColumnRenamed('ancestor_concept_id','condition_concept_id') \
        .withColumnRenamed('anc_concept_name','condition_concept_name') \
        .distinct()
    return rolled_up

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.01b39d45-0f48-4358-9cab-a4e22cf26c1e"),
    filtered_conditions_cfs=Input(rid="ri.foundry.main.dataset.32909008-285f-488a-8b48-5872ce2d7884"),
    get_weights_per_concept_cfs=Input(rid="ri.foundry.main.dataset.277e92e1-215e-477b-bc12-a3ff3fd3a131"),
    topdiagnoses_cfs=Input(rid="ri.foundry.main.dataset.81cca64e-8fac-43ca-8d97-ea66e3bf1466")
)
import pyspark.sql.functions as F
from pyspark.sql.types import FloatType, StringType, IntegerType, StructType, StructField, ArrayType, MapType
import numpy as np
import pandas as pd

def assign_cluster_scores_cfs(topdiagnoses_cfs, get_weights_per_concept_cfs, filtered_conditions_cfs):
    
    df = filtered_conditions_cfs
    print(f'Count before topdiagnosis filtering: {df.count()}')

    new_cond_df = df.groupBy('person_id').agg(F.collect_list(F.col('condition_concept_name')).alias('new_conditions_post_dx')).toPandas()
    print(f'Patient Count after topdiagnosis filtering: {new_cond_df.shape[0]}')
    cond_dt_df = df.groupBy('person_id').agg(F.collect_set(F.col('condition_start_date')).alias('new_conditions_post_dx_dates')).toPandas()
    print(f'Patient Count after topdiagnosis filtering with date: {cond_dt_df.shape[0]}')

    topdiagnoses_list = list(topdiagnoses_cfs.condition_concept_name)

    df = df.groupBy('person_id').agg(F.collect_list(F.col('condition_concept_name')).alias('top_condition_names')).toPandas()
    df['top_condition_names'] = df['top_condition_names'].apply(lambda x: [i for i in x if i in topdiagnoses_list])

    df = df.merge(new_cond_df, on='person_id', how='inner')
    df = df.merge(cond_dt_df, on='person_id', how='inner')
    print(df['top_condition_names'])

    wdf = get_weights_per_concept_cfs
    wdf['wts_dict'] = [{k:v} for k,v in zip(get_weights_per_concept_cfs['name'], get_weights_per_concept_cfs['weights'])]

    wts = wdf.groupby('community')['wts_dict'].apply(list)
    wts = wts.apply(lambda L: {k: v for d in L for k, v in d.items()})
    print(wts)

    for clst in wts.index:
        wts_dict = wts.loc[clst]
        df[f'cluster_{clst}'] = df['top_condition_names'].apply(lambda x: float(np.sum([wts_dict[i] for i in x if i in wts_dict.keys()])))
        print(df)
        df[f'cluster_{clst}'] = df[f'cluster_{clst}'].apply(lambda x: np.nan if x==0.0 else x)
    

    return df

def get_cluster_assn_wts(condition_list, wts_dict):

    cluster_wts = [x*wts_dict[x] for x in condition_list]

    return cluster_wts

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.0e9aa996-d9ec-4167-bfb2-ec115377489c"),
    filtered_conditions_cp_pasc_or_cfs=Input(rid="ri.foundry.main.dataset.d21aa532-5b69-4965-9b38-858e6445e862"),
    get_weights_per_concept_pasc_or_cfs=Input(rid="ri.foundry.main.dataset.46dc2567-e604-44d6-8143-39be1c5461bd"),
    topdiagnoses_pasc_or_cfs=Input(rid="ri.foundry.main.dataset.2c11529a-b8d3-4339-828e-0213b2378f95")
)
import pyspark.sql.functions as F
from pyspark.sql.types import FloatType, StringType, IntegerType, StructType, StructField, ArrayType, MapType
import numpy as np
import pandas as pd

def assign_cluster_scores_cp_pasc_or_cfs_1(topdiagnoses_pasc_or_cfs, get_weights_per_concept_pasc_or_cfs, filtered_conditions_cp_pasc_or_cfs):
    
    df = filtered_conditions_cp_pasc_or_cfs
    
    topdiagnoses_list = list(topdiagnoses_pasc_or_cfs.condition_concept_name)
    df = df.groupBy('person_id').agg(F.collect_list(F.col('condition_concept_name')).alias('top_condition_names')).toPandas()
    df['top_condition_names'] = df['top_condition_names'].apply(lambda x: [i for i in x if i in topdiagnoses_list])

    wdf = get_weights_per_concept_pasc_or_cfs
    wdf['wts_dict'] = [{k:v} for k,v in zip(get_weights_per_concept_pasc_or_cfs['name'], get_weights_per_concept_pasc_or_cfs['weights'])]

    wts = wdf.groupby('community')['wts_dict'].apply(list)
    wts = wts.apply(lambda L: {k: v for d in L for k, v in d.items()})

    for clst in wts.index:
        wts_dict = wts.loc[clst]
        df[f'cluster_{clst}'] = df['top_condition_names'].apply(lambda x: float(np.sum([wts_dict[i] for i in x if i in wts_dict.keys()])))
    
    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.dc957b4b-6a1e-4b9b-bd06-4c7113bc7914"),
    Filtered_conditions_pasc_or_cfs=Input(rid="ri.foundry.main.dataset.fdf4daf6-6ae9-40b8-8e87-5e1c98d78796"),
    get_weights_per_concept_pasc_or_cfs=Input(rid="ri.foundry.main.dataset.46dc2567-e604-44d6-8143-39be1c5461bd"),
    topdiagnoses_pasc_or_cfs=Input(rid="ri.foundry.main.dataset.2c11529a-b8d3-4339-828e-0213b2378f95")
)
import pyspark.sql.functions as F
from pyspark.sql.types import FloatType, StringType, IntegerType, StructType, StructField, ArrayType, MapType
import numpy as np
import pandas as pd

def assign_cluster_scores_diagnostic_pasc_or_cfs(topdiagnoses_pasc_or_cfs, get_weights_per_concept_pasc_or_cfs, Filtered_conditions_pasc_or_cfs):
    
    df = Filtered_conditions_pasc_or_cfs
    
    topdiagnoses_list = list(topdiagnoses_pasc_or_cfs.condition_concept_name)
    df = df.groupBy('person_id').agg(F.collect_list(F.col('condition_concept_name')).alias('top_condition_names')).toPandas()
    df['top_condition_names'] = df['top_condition_names'].apply(lambda x: [i for i in x if i in topdiagnoses_list])

    wdf = get_weights_per_concept_pasc_or_cfs
    wdf['wts_dict'] = [{k:v} for k,v in zip(get_weights_per_concept_pasc_or_cfs['name'], get_weights_per_concept_pasc_or_cfs['weights'])]

    wts = wdf.groupby('community')['wts_dict'].apply(list)
    wts = wts.apply(lambda L: {k: v for d in L for k, v in d.items()})

    for clst in wts.index:
        wts_dict = wts.loc[clst]
        df[f'cluster_{clst}'] = df['top_condition_names'].apply(lambda x: float(np.sum([wts_dict[i] for i in x if i in wts_dict.keys()])))
    
    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.5198bca8-8c50-476f-838a-3632059d52e3"),
    filtered_conditions_pasc=Input(rid="ri.foundry.main.dataset.84caf9d0-befc-4629-b5a9-49743bf12f55"),
    get_weights_per_concept_pasc=Input(rid="ri.foundry.main.dataset.0d1b05f1-6ac6-4bed-8832-5b65c04b758f"),
    topdiagnoses_pasc=Input(rid="ri.foundry.main.dataset.21e21123-d244-4084-bff7-857701926e74")
)
import pyspark.sql.functions as F
from pyspark.sql.types import FloatType, StringType, IntegerType, StructType, StructField, ArrayType, MapType
import numpy as np
import pandas as pd

def assign_cluster_scores_pasc(topdiagnoses_pasc, get_weights_per_concept_pasc, filtered_conditions_pasc):
    
    df = filtered_conditions_pasc
    print(f'Count before topdiagnosis filtering: {df.count()}')

    new_cond_df = df.groupBy('person_id').agg(F.collect_list(F.col('condition_concept_name')).alias('new_conditions_post_dx')).toPandas()
    print(f'Patient Count after topdiagnosis filtering: {new_cond_df.shape[0]}')
    cond_dt_df = df.groupBy('person_id').agg(F.collect_set(F.col('condition_start_date')).alias('new_conditions_post_dx_dates')).toPandas()
    print(f'Patient Count after topdiagnosis filtering with date: {cond_dt_df.shape[0]}')

    topdiagnoses_list = list(topdiagnoses_pasc.condition_concept_name)

    df = df.groupBy('person_id').agg(F.collect_list(F.col('condition_concept_name')).alias('top_condition_names')).toPandas()
    df['top_condition_names'] = df['top_condition_names'].apply(lambda x: [i for i in x if i in topdiagnoses_list])

    df = df.merge(new_cond_df, on='person_id', how='inner')
    df = df.merge(cond_dt_df, on='person_id', how='inner')
    print(df['top_condition_names'])

    wdf = get_weights_per_concept_pasc
    wdf['wts_dict'] = [{k:v} for k,v in zip(get_weights_per_concept_pasc['name'], get_weights_per_concept_pasc['weights'])]

    wts = wdf.groupby('community')['wts_dict'].apply(list)
    wts = wts.apply(lambda L: {k: v for d in L for k, v in d.items()})
    print(wts)

    for clst in wts.index:
        wts_dict = wts.loc[clst]
        df[f'cluster_{clst}'] = df['top_condition_names'].apply(lambda x: float(np.sum([wts_dict[i] for i in x if i in wts_dict.keys()])))
        print(df)
        df[f'cluster_{clst}'] = df[f'cluster_{clst}'].apply(lambda x: np.nan if x==0.0 else x)
    

    return df

def get_cluster_assn_wts(condition_list, wts_dict):

    cluster_wts = [x*wts_dict[x] for x in condition_list]

    return cluster_wts

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.2df9c772-2a46-4619-8843-723f3b697f5b"),
    filtered_conditions_pasc_cfs=Input(rid="ri.foundry.main.dataset.b5c67ad3-778b-4499-81ad-9fc79ef92f93"),
    get_weights_per_concept_pasc_cfs=Input(rid="ri.foundry.main.dataset.24f80887-d290-4a9e-8fe8-93d5be22c678"),
    topdiagnoses_pasc_cfs=Input(rid="ri.foundry.main.dataset.1345f383-f6ae-4264-a1d7-cc56e5d2ab83")
)
import pyspark.sql.functions as F
from pyspark.sql.types import FloatType, StringType, IntegerType, StructType, StructField, ArrayType, MapType
import numpy as np
import pandas as pd

def assign_cluster_scores_pasc_cfs(topdiagnoses_pasc_cfs, get_weights_per_concept_pasc_cfs, filtered_conditions_pasc_cfs):
    
    df = filtered_conditions_pasc_cfs
    print(f'Count before topdiagnosis filtering: {df.count()}')

    new_cond_df = df.groupBy('person_id').agg(F.collect_list(F.col('condition_concept_name')).alias('new_conditions_post_dx')).toPandas()
    print(f'Patient Count after topdiagnosis filtering: {new_cond_df.shape[0]}')
    cond_dt_df = df.groupBy('person_id').agg(F.collect_set(F.col('condition_start_date')).alias('new_conditions_post_dx_dates')).toPandas()
    print(f'Patient Count after topdiagnosis filtering with date: {cond_dt_df.shape[0]}')

    topdiagnoses_list = list(topdiagnoses_pasc_cfs.condition_concept_name)

    df = df.groupBy('person_id').agg(F.collect_list(F.col('condition_concept_name')).alias('top_condition_names')).toPandas()
    df['top_condition_names'] = df['top_condition_names'].apply(lambda x: [i for i in x if i in topdiagnoses_list])

    df = df.merge(new_cond_df, on='person_id', how='inner')
    df = df.merge(cond_dt_df, on='person_id', how='inner')
    print(df['top_condition_names'])

    wdf = get_weights_per_concept_pasc_cfs
    wdf['wts_dict'] = [{k:v} for k,v in zip(get_weights_per_concept_pasc_cfs['name'], get_weights_per_concept_pasc_cfs['weights'])]

    wts = wdf.groupby('community')['wts_dict'].apply(list)
    wts = wts.apply(lambda L: {k: v for d in L for k, v in d.items()})
    print(wts)

    for clst in wts.index:
        wts_dict = wts.loc[clst]
        df[f'cluster_{clst}'] = df['top_condition_names'].apply(lambda x: float(np.sum([wts_dict[i] for i in x if i in wts_dict.keys()])))
        print(df)
        df[f'cluster_{clst}'] = df[f'cluster_{clst}'].apply(lambda x: np.nan if x==0.0 else x)
    

    return df

def get_cluster_assn_wts(condition_list, wts_dict):

    cluster_wts = [x*wts_dict[x] for x in condition_list]

    return cluster_wts

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.2ee23deb-c256-4b2e-815a-ce1c500b68b4"),
    assign_cluster_scores_cp_pasc_or_cfs_1=Input(rid="ri.foundry.main.dataset.0e9aa996-d9ec-4167-bfb2-ec115377489c"),
    filtered_conditions_cp_pasc_or_cfs=Input(rid="ri.foundry.main.dataset.d21aa532-5b69-4965-9b38-858e6445e862")
)
def clusters_and_labels_cp(assign_cluster_scores_cp_pasc_or_cfs_1, filtered_conditions_cp_pasc_or_cfs):
    
    cohort_labels = filtered_conditions_cp_pasc_or_cfs.select('person_id', 'cohort_label').distinct()

    df = assign_cluster_scores_cp_pasc_or_cfs_1.join(cohort_labels, 'person_id', how='left')
    df = df.select('person_id', 'cohort_label', 'cluster_1', 'cluster_2', 'cluster_3').withColumnRenamed('cohort_label', 'dx_label')

    thr = 0.2
    df = df.withColumn('sub_1', F.when(F.col('cluster_1') >= thr, 1).otherwise(0))
    df = df.withColumn('sub_2', F.when(F.col('cluster_2') >= thr, 1).otherwise(0))
    df = df.withColumn('sub_3', F.when(F.col('cluster_3') >= thr, 1).otherwise(0))
    df = df.withColumn('subphenotype', F.col('sub_1') + (2 * F.col('sub_2')) + (4 * F.col('sub_3')))
    df = df.withColumn('subphenotype', F.when(F.col('subphenotype') == 0, 'None')
                                        .when(F.col('subphenotype') == 1, '1')
                                        .when(F.col('subphenotype') == 2, '2')
                                        .when(F.col('subphenotype') == 3, '1 & 2')
                                        .when(F.col('subphenotype') == 4, '3')
                                        .when(F.col('subphenotype') == 5, '1 & 3')
                                        .when(F.col('subphenotype') == 6, '2 & 3')
                                        .when(F.col('subphenotype') == 7, 'All'))

    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.8209cadc-4f2f-4ada-bd32-4af8a2b9fb00"),
    Filtered_conditions_pasc_or_cfs=Input(rid="ri.foundry.main.dataset.fdf4daf6-6ae9-40b8-8e87-5e1c98d78796"),
    assign_cluster_scores_diagnostic_pasc_or_cfs=Input(rid="ri.foundry.main.dataset.dc957b4b-6a1e-4b9b-bd06-4c7113bc7914")
)
def clusters_and_labels_diagnostic(assign_cluster_scores_diagnostic_pasc_or_cfs, Filtered_conditions_pasc_or_cfs):
    
    cohort_labels = Filtered_conditions_pasc_or_cfs.select('person_id', 'cohort_label').distinct()

    df = assign_cluster_scores_diagnostic_pasc_or_cfs.join(cohort_labels, 'person_id', how='left')
    df = df.select('person_id', 'cohort_label', 'cluster_1', 'cluster_2', 'cluster_3').withColumnRenamed('cohort_label', 'dx_label')

    thr = 0.2
    df = df.withColumn('sub_1', F.when(F.col('cluster_1') >= thr, 1).otherwise(0))
    df = df.withColumn('sub_2', F.when(F.col('cluster_2') >= thr, 1).otherwise(0))
    df = df.withColumn('sub_3', F.when(F.col('cluster_3') >= thr, 1).otherwise(0))
    df = df.withColumn('subphenotype', F.col('sub_1') + (2 * F.col('sub_2')) + (4 * F.col('sub_3')))
    df = df.withColumn('subphenotype', F.when(F.col('subphenotype') == 0, 'None')
                                        .when(F.col('subphenotype') == 1, '1')
                                        .when(F.col('subphenotype') == 2, '2')
                                        .when(F.col('subphenotype') == 3, '1 & 2')
                                        .when(F.col('subphenotype') == 4, '3')
                                        .when(F.col('subphenotype') == 5, '1 & 3')
                                        .when(F.col('subphenotype') == 6, '2 & 3')
                                        .when(F.col('subphenotype') == 7, 'All'))
    df = df.withColumn('dx_label', F.when(F.col('dx_label') == 'CFS', 'ME/CFS')
                                        .when(F.col('dx_label') == 'PASC_CFS', 'PASC_ME/CFS')
                                        .otherwise(F.col('dx_label')))

    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.fdf5cbeb-0cdf-489e-897f-10d03f8c7ed2"),
    filtered_conditions_cfs=Input(rid="ri.foundry.main.dataset.32909008-285f-488a-8b48-5872ce2d7884"),
    get_weights_per_concept_cfs=Input(rid="ri.foundry.main.dataset.277e92e1-215e-477b-bc12-a3ff3fd3a131"),
    topdiagnoses_cfs=Input(rid="ri.foundry.main.dataset.81cca64e-8fac-43ca-8d97-ea66e3bf1466")
)
import pyspark.sql.functions as F
from pyspark.sql.types import FloatType, StringType, IntegerType, StructType, StructField, ArrayType, MapType
import numpy as np
import pandas as pd

def get_cluster_assignment_scores_cfs(topdiagnoses_cfs, get_weights_per_concept_cfs, filtered_conditions_cfs):
    df = filtered_conditions_cfs
    print(f'Count before topdiagnosis filtering: {df.count()}')

    new_cond_df = df.groupBy('person_id').agg(F.collect_list(F.col('condition_concept_name')).alias('all_condition_names')).toPandas()
    print(f'Patient Count after topdiagnosis filtering: {new_cond_df.shape[0]}')
    cond_dt_df = df.groupBy('person_id').agg(F.collect_set(F.col('condition_start_date')).alias('all_condition_dates')).toPandas()
    print(f'Patient Count after topdiagnosis filtering with date: {cond_dt_df.shape[0]}')

    df = df.join(topdiagnoses_cfs, 'condition_concept_name', 'inner')
    print(f'Count after topdiagnosis filtering: {df.count()}')

    df = df.groupBy('person_id').agg(F.collect_list(F.col('condition_concept_name')).alias('condition_names')).toPandas()

    df = df.merge(new_cond_df, on='person_id', how='inner')
    df = df.merge(cond_dt_df, on='person_id', how='inner')
    print(df['condition_names'])
    
    wdf = get_weights_per_concept_cfs
    wdf['wts_dict'] = [{k:v} for k,v in zip(get_weights_per_concept_cfs['name'], get_weights_per_concept_cfs['weights'])]

    wts = wdf.groupby('community')['wts_dict'].apply(list)
    wts = wts.apply(lambda L: {k: v for d in L for k, v in d.items()})
    print(wts)

    for clst in wts.index:
        wts_dict = wts.loc[clst]
        df[f'cluster_{clst}'] = df['condition_names'].apply(lambda x: float(np.sum([wts_dict[i] for i in x if i in wts_dict.keys()])))
        print(df)
        # df[f'cluster_{clst}'] = df[f'cluster_{clst}'].astype(float)
        df[f'cluster_{clst}'] = df[f'cluster_{clst}'].apply(lambda x: np.nan if x==0.0 else x)
    
    # df = df.replace(0.0,np.nan)
    # df = df.where((df==0).all(), np.nan)
    
    df['new_condition_count'] = df['all_condition_names'].apply(len)
    df['date_lens'] = df['all_condition_dates'].apply(len)

    return df

def get_cluster_assn_wts(condition_list, wts_dict):
    # wts_dict = wts_df.loc[cluster_label]

    cluster_wts = [x*wts_dict[x] for x in condition_list]

    return cluster_wts

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.aa46469f-e12a-4339-b199-1b025ee92abf"),
    filtered_conditions_pasc=Input(rid="ri.foundry.main.dataset.84caf9d0-befc-4629-b5a9-49743bf12f55"),
    get_weights_per_concept_pasc=Input(rid="ri.foundry.main.dataset.0d1b05f1-6ac6-4bed-8832-5b65c04b758f"),
    topdiagnoses_pasc=Input(rid="ri.foundry.main.dataset.21e21123-d244-4084-bff7-857701926e74")
)
import pyspark.sql.functions as F
from pyspark.sql.types import FloatType, StringType, IntegerType, StructType, StructField, ArrayType, MapType
import numpy as np
import pandas as pd

def get_cluster_assignment_scores_pasc(topdiagnoses_pasc, get_weights_per_concept_pasc, filtered_conditions_pasc):
    df = filtered_conditions_pasc
    print(f'Count before topdiagnosis filtering: {df.count()}')

    new_cond_df = df.groupBy('person_id').agg(F.collect_list(F.col('condition_concept_name')).alias('all_condition_names')).toPandas()
    print(f'Patient Count after topdiagnosis filtering: {new_cond_df.shape[0]}')
    cond_dt_df = df.groupBy('person_id').agg(F.collect_set(F.col('condition_start_date')).alias('all_condition_dates')).toPandas()
    print(f'Patient Count after topdiagnosis filtering with date: {cond_dt_df.shape[0]}')

    df = df.join(topdiagnoses_pasc, 'condition_concept_name', 'inner')
    print(f'Count after topdiagnosis filtering: {df.count()}')

    df = df.groupBy('person_id').agg(F.collect_list(F.col('condition_concept_name')).alias('condition_names')).toPandas()

    df = df.merge(new_cond_df, on='person_id', how='inner')
    df = df.merge(cond_dt_df, on='person_id', how='inner')
    print(df['condition_names'])
    
    wdf = get_weights_per_concept_pasc
    wdf['wts_dict'] = [{k:v} for k,v in zip(get_weights_per_concept_pasc['name'], get_weights_per_concept_pasc['weights'])]

    wts = wdf.groupby('community')['wts_dict'].apply(list)
    wts = wts.apply(lambda L: {k: v for d in L for k, v in d.items()})
    print(wts)

    for clst in wts.index:
        wts_dict = wts.loc[clst]
        df[f'cluster_{clst}'] = df['condition_names'].apply(lambda x: float(np.sum([wts_dict[i] for i in x if i in wts_dict.keys()])))
        print(df)
        # df[f'cluster_{clst}'] = df[f'cluster_{clst}'].astype(float)
        df[f'cluster_{clst}'] = df[f'cluster_{clst}'].apply(lambda x: np.nan if x==0.0 else x)
    
    # df = df.replace(0.0,np.nan)
    # df = df.where((df==0).all(), np.nan)
    
    df['new_condition_count'] = df['all_condition_names'].apply(len)
    df['date_lens'] = df['all_condition_dates'].apply(len)

    return df

def get_cluster_assn_wts(condition_list, wts_dict):
    # wts_dict = wts_df.loc[cluster_label]

    cluster_wts = [x*wts_dict[x] for x in condition_list]

    return cluster_wts

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.57a0fe41-9ed3-43d6-9abf-203d84dd2a20"),
    filtered_conditions_pasc_cfs=Input(rid="ri.foundry.main.dataset.b5c67ad3-778b-4499-81ad-9fc79ef92f93"),
    get_weights_per_concept_pasc_cfs=Input(rid="ri.foundry.main.dataset.24f80887-d290-4a9e-8fe8-93d5be22c678"),
    topdiagnoses_pasc_cfs=Input(rid="ri.foundry.main.dataset.1345f383-f6ae-4264-a1d7-cc56e5d2ab83")
)
import pyspark.sql.functions as F
from pyspark.sql.types import FloatType, StringType, IntegerType, StructType, StructField, ArrayType, MapType
import numpy as np
import pandas as pd

def get_cluster_assignment_scores_pasc_cfs(topdiagnoses_pasc_cfs, get_weights_per_concept_pasc_cfs, filtered_conditions_pasc_cfs):
    df = filtered_conditions_pasc_cfs
    print(f'Count before topdiagnosis filtering: {df.count()}')

    new_cond_df = df.groupBy('person_id').agg(F.collect_list(F.col('condition_concept_name')).alias('all_condition_names')).toPandas()
    print(f'Patient Count after topdiagnosis filtering: {new_cond_df.shape[0]}')
    cond_dt_df = df.groupBy('person_id').agg(F.collect_set(F.col('condition_start_date')).alias('all_condition_dates')).toPandas()
    print(f'Patient Count after topdiagnosis filtering with date: {cond_dt_df.shape[0]}')

    df = df.join(topdiagnoses_pasc_cfs, 'condition_concept_name', 'inner')
    print(f'Count after topdiagnosis filtering: {df.count()}')

    df = df.groupBy('person_id').agg(F.collect_list(F.col('condition_concept_name')).alias('condition_names')).toPandas()

    df = df.merge(new_cond_df, on='person_id', how='inner')
    df = df.merge(cond_dt_df, on='person_id', how='inner')
    print(df['condition_names'])
    
    wdf = get_weights_per_concept_pasc_cfs
    wdf['wts_dict'] = [{k:v} for k,v in zip(get_weights_per_concept_pasc_cfs['name'], get_weights_per_concept_pasc_cfs['weights'])]

    wts = wdf.groupby('community')['wts_dict'].apply(list)
    wts = wts.apply(lambda L: {k: v for d in L for k, v in d.items()})
    print(wts)

    for clst in wts.index:
        wts_dict = wts.loc[clst]
        df[f'cluster_{clst}'] = df['condition_names'].apply(lambda x: float(np.sum([wts_dict[i] for i in x if i in wts_dict.keys()])))
        print(df)
        # df[f'cluster_{clst}'] = df[f'cluster_{clst}'].astype(float)
        df[f'cluster_{clst}'] = df[f'cluster_{clst}'].apply(lambda x: np.nan if x==0.0 else x)
    
    # df = df.replace(0.0,np.nan)
    # df = df.where((df==0).all(), np.nan)
    
    df['new_condition_count'] = df['all_condition_names'].apply(len)
    df['date_lens'] = df['all_condition_dates'].apply(len)

    return df

def get_cluster_assn_wts(condition_list, wts_dict):
    # wts_dict = wts_df.loc[cluster_label]

    cluster_wts = [x*wts_dict[x] for x in condition_list]

    return cluster_wts

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.277e92e1-215e-477b-bc12-a3ff3fd3a131"),
    network_metrics_cfs=Input(rid="ri.foundry.main.dataset.2d0b5eba-92cf-4f7a-9912-619d0f17a666")
)
def get_weights_per_concept_cfs(network_metrics_cfs):
    df = network_metrics_cfs
    tot_pats = df.groupby('community')['pat_count'].sum()
    print(tot_pats)

    df['weights'] = df['pat_count']

    weight_list = []
    for index, row in df.iterrows():
        weight_list.append(row['pat_count']/tot_pats.loc[row['community']])
    
    df['weights'] = weight_list

    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.0d1b05f1-6ac6-4bed-8832-5b65c04b758f"),
    network_metrics_pasc=Input(rid="ri.foundry.main.dataset.05ae9739-033a-4af4-8c23-2b44265a3583")
)
def get_weights_per_concept_pasc(network_metrics_pasc):
    df = network_metrics_pasc
    tot_pats = df.groupby('community')['pat_count'].sum()
    print(tot_pats)

    df['weights'] = df['pat_count']

    weight_list = []
    for index, row in df.iterrows():
        weight_list.append(row['pat_count']/tot_pats.loc[row['community']])
    
    df['weights'] = weight_list

    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.24f80887-d290-4a9e-8fe8-93d5be22c678"),
    network_metrics_pasc_cfs=Input(rid="ri.foundry.main.dataset.228c12c3-2ece-4543-b2e9-6d9a5b66b225")
)
def get_weights_per_concept_pasc_cfs(network_metrics_pasc_cfs):
    df = network_metrics_pasc_cfs
    tot_pats = df.groupby('community')['pat_count'].sum()
    print(tot_pats)

    df['weights'] = df['pat_count']

    weight_list = []
    for index, row in df.iterrows():
        weight_list.append(row['pat_count']/tot_pats.loc[row['community']])
    
    df['weights'] = weight_list

    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.46dc2567-e604-44d6-8143-39be1c5461bd"),
    network_metrics_pasc_or_cfs=Input(rid="ri.foundry.main.dataset.28907b00-d6de-4174-b381-dd4e834508de")
)
def get_weights_per_concept_pasc_or_cfs(network_metrics_pasc_or_cfs):
    df = network_metrics_pasc_or_cfs
    tot_pats = df.groupby('community')['pat_count'].sum()
    print(tot_pats)

    df['weights'] = df['pat_count']

    weight_list = []
    for index, row in df.iterrows():
        weight_list.append(row['pat_count']/tot_pats.loc[row['community']])
    
    df['weights'] = weight_list

    return df

@transform_pandas(
    Output(rid="ri.vector.main.execute.d3c4b186-905f-4717-b994-fc6b9aa4d720"),
    Full_cohort_w_ml_vars=Input(rid="ri.foundry.main.dataset.5aa7b269-ea4c-4978-9cf5-5dbf92373cca"),
    mecfs_dyspnea=Input(rid="ri.foundry.main.dataset.5df1c9af-092f-4d85-9043-ff4ad1cc94b1")
)
def mecfs_dyspnea_rate(mecfs_dyspnea, Full_cohort_w_ml_vars):

    cohort = Full_cohort_w_ml_vars
    mecfs_total = cohort.where(F.col('cohort_label') == 'CFS').count()
    print('total ME/CFS:', mecfs_total)
    r5382_total = cohort.where(F.col('cohort_label') == 'R5382').count()
    print('total R53.82:', r5382_total)

    mecfs_pre_total = cohort.where((F.col('cohort_label') == 'CFS') & (F.date_sub(F.col('index_date'), 70) <= '2020-01-31')).count()
    mecfs_since_total = cohort.where((F.col('cohort_label') == 'CFS') & (F.date_add(F.col('index_date'), 30) > '2020-01-31')).count()
    r5382_pre_total = cohort.where((F.col('cohort_label') == 'R5382') & (F.date_sub(F.col('index_date'), 70) <= '2020-01-31')).count()
    r5382_since_total = cohort.where((F.col('cohort_label') == 'R5382') & (F.date_add(F.col('index_date'), 30) > '2020-01-31')).count()
    print('total ME/CFS pre:', mecfs_pre_total)
    print('total ME/CFS since:', mecfs_since_total)
    print('total R53.82 pre:', r5382_pre_total)
    print('total R53.82 since:', r5382_since_total)

    first_dyspnea = (
        mecfs_dyspnea
        .groupBy('person_id')
        .agg(F.min('condition_start_date').alias('condition_start_date'), 
            F.first('cohort_label').alias('cohort_label'))
    )
    
    mecfs = first_dyspnea.where(F.col('cohort_label') == 'CFS')
    r5382 = first_dyspnea.where(F.col('cohort_label') == 'R5382')

    print('ME/CFS dyspnea rate before pandemic:', mecfs.where(F.col('condition_start_date') <= '2020-01-31').count() / mecfs_pre_total * 100)
    print('ME/CFS dyspnea rate since pandemic:', mecfs.where(F.col('condition_start_date') > '2020-01-31').count() / mecfs_since_total * 100)
    print('R53.82 dyspnea rate before pandemic:', r5382.where(F.col('condition_start_date') <= '2020-01-31').count() / r5382_pre_total * 100)
    print('R53.82 dyspnea rate since pandemic:', r5382.where(F.col('condition_start_date') > '2020-01-31').count() / r5382_since_total * 100)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c01e74ca-a0d8-4d6e-83d6-3c2f7ed4cf97"),
    clusters_and_labels_diagnostic=Input(rid="ri.foundry.main.dataset.8209cadc-4f2f-4ada-bd32-4af8a2b9fb00")
)
def subphenotype_summary(clusters_and_labels_diagnostic):
    totals = clusters_and_labels_diagnostic.groupBy('dx_label').count().withColumnRenamed('count', 'total')
    df = clusters_and_labels_diagnostic.groupBy('dx_label', 'subphenotype').count()
    df = df.join(totals, 'dx_label', how='left')
    df = df.withColumn('percentage', (F.col('count') / F.col('total')) * 100)
    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.d1fd284f-250f-4165-95a6-3511c9012019"),
    clusters_and_labels_cp=Input(rid="ri.foundry.main.dataset.2ee23deb-c256-4b2e-815a-ce1c500b68b4")
)
def subphenotype_summary_cp(clusters_and_labels_cp):
    totals = clusters_and_labels_cp.groupBy('dx_label').count().withColumnRenamed('count', 'total')
    df = clusters_and_labels_cp.groupBy('dx_label', 'subphenotype').count()
    df = df.join(totals, 'dx_label', how='left')
    df = df.withColumn('percentage', (F.col('count') / F.col('total')) * 100)
    return df

