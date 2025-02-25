# Authors: Abhishek Bhatia, Tomas McIntee, John Powers
# © 2025, The University of North Carolina at Chapel Hill. Permission is granted to use in accordance with the MIT license.
# The code is licensed under the MIT license.


import pyspark.sql.functions as F

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.01dccdc0-0498-46ed-915c-e402d1cf2bd6"),
    mecfs_group_g9332_plus_other=Input(rid="ri.foundry.main.dataset.224b605e-250d-41e9-992e-f3df019fd992"),
    pasc_group=Input(rid="ri.foundry.main.dataset.18987526-547b-45a3-acf7-21aaf51e5991")
)
# outer join the ME/CFS and PASC groups
# and set cohort_label to indicate whether patient has 1 condition or both
# for patients with both, use earlier condition index as the overall index date

def combined_groups(pasc_group, mecfs_group_g9332_plus_other):
    df = mecfs_group_g9332_plus_other.join(pasc_group, ['person_id', 'sex', 'race', 'ethnicity', 'birth_date'], how='outer')
    df = df.withColumn('cfs', F.when(F.col('cfs').isNotNull(), F.col('cfs')).otherwise(0))
    df = df.withColumn('pasc', F.when(F.col('pasc').isNotNull(), F.col('pasc')).otherwise(0))
    df = (
        df
        .withColumn('cohort_label', 
            F.when((F.col('cfs') + F.col('pasc')) == 2, 'PASC_CFS')
            .when(F.col('cfs') == 1, 'CFS')
            .otherwise('PASC'))
    )
    df = df.withColumn('index_date', F.least(F.col('pasc_index'), F.col('cfs_index')))
    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f1ca1106-2d83-4c4e-bd66-264c4fdf7880"),
    combined_plus_ctrls=Input(rid="ri.foundry.main.dataset.4d13cdf1-0162-4b39-91a3-80048bce4bb1"),
    recover_release_condition_occurrence=Input(rid="ri.foundry.main.dataset.ef3dcd85-f845-4710-923f-ceaeb71155e4")
)
# filter condition_occurrences table for pre-index conditions for patients in our groups
def condition_occurrences(recover_release_condition_occurrence, combined_plus_ctrls):
    
    co = recover_release_condition_occurrence
    combined = combined_plus_ctrls.select('person_id', 'index_date')
    
    df = (
        co
        .join(combined, 'person_id', how='inner')
        .where(F.col('condition_start_date') <= F.col('index_date'))
    )

    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.0bf6e59f-ed2b-4887-b22f-48fa9ad32143"),
    conditions_replace_I10_prefix=Input(rid="ri.foundry.main.dataset.3f85a04e-5c07-49d2-8d5a-6f13786f96cc")
)
# Address entries with Epic-styled compounded ICD10 codes as source codes (e.g., "C50.912, Z17.0")

from pyspark.sql.functions import split, explode, trim, regexp_replace

def conditions_resolve_multi_icd(conditions_replace_I10_prefix):

    cols = ["condition_occurrence_id", "person_id", "data_partner_id", "condition_source_value"]
    df = conditions_replace_I10_prefix

    # Split condition value col by "," and create new rows for each split element
    df = df.select(*cols, 
                    explode(split(df.condition_source_value_cleaned,",")).alias("condition_source_value_cleaned")
    )

    # remove whitespace introduced during split
    df = df.withColumn("condition_source_value_cleaned",
                        trim(df.condition_source_value_cleaned)
    )
    
    return(df)

@transform_pandas(
    Output(rid="ri.vector.main.execute.2efa17b9-44b0-43c4-bbb5-857a8eb1148a"),
    combined_groups=Input(rid="ri.foundry.main.dataset.01dccdc0-0498-46ed-915c-e402d1cf2bd6")
)
def group_counts(combined_groups):
    df = combined_groups.groupBy('cohort_label').count()
    return df

@transform_pandas(
    Output(rid="ri.vector.main.execute.83aba97b-1643-41c4-802c-0f6df3b06a8f"),
    only_cci=Input(rid="ri.foundry.main.dataset.2f1b4413-2d6e-4272-bb33-940ac2305fc7")
)
def group_counts_2(only_cci):
    df = only_cci.groupBy('cohort_label').count()
    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.122f5ec8-023e-424b-b38e-0fe000c3f15d"),
    mecfs_conditions=Input(rid="ri.foundry.main.dataset.53196dd0-0f4a-4261-9d45-dbe85f559950"),
    recover_release_concept=Input(rid="ri.foundry.main.dataset.6fd878f0-895f-48fa-ad01-999883522a54")
)
# examine the main and source concepts being captured by the standard OMOP ME/CFS concepts in the data
# the results of this node were used to set appropriate filters for mecfs_conditions node

def mecfs_condition_breakdown(mecfs_conditions, recover_release_concept):
    df = (
        mecfs_conditions
        .groupBy('condition_concept_id', 'condition_concept_name', 'condition_source_concept_id', 'condition_source_concept_name')
        .count()    
    )
    df = (
        df
        .join(recover_release_concept, (df['condition_source_concept_id'] == recover_release_concept['concept_id']), how='left')
        .select('condition_concept_id', 'condition_concept_name', 'count', 'condition_source_concept_id', 'condition_source_concept_name', 'vocabulary_id', 'concept_code')
    )
    return df.sort('condition_concept_id', 'count', ascending=False)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f6a3a7a7-74b0-4a99-9f38-f380060654a0"),
    recover_release_condition_occurrence=Input(rid="ri.foundry.main.dataset.ef3dcd85-f845-4710-923f-ceaeb71155e4")
)
# get count of ME/CFS dxs per site, and rate of patients with ME/CFS dx per site

def mecfs_sites1(recover_release_condition_occurrence):
    partner_list = recover_release_condition_occurrence.select('data_partner_id', 'person_id').distinct().groupBy('data_partner_id').count().withColumnRenamed('count', 'site_total')
    df = recover_release_condition_occurrence.where((F.col('condition_concept_id') == 432738) & (F.col('condition_source_concept_id') == 37402487)).select('data_partner_id', 'person_id').distinct()
    df = df.groupBy('data_partner_id').count()
    df = partner_list.join(df, 'data_partner_id', how='left')
    df = df.withColumn('mecfs_count', F.when(F.col('count').isNull(), 0).otherwise(F.col('count'))).drop('count')
    df = df.withColumn('mecfs_per_thous', ((F.col('mecfs_count') / F.col('site_total')) * 1000))
    return df.sort('mecfs_per_thous')

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.2f1b4413-2d6e-4272-bb33-940ac2305fc7"),
    charlson_scores=Input(rid="ri.foundry.main.dataset.8e8089df-225e-48dd-b53f-5d2983e62b7c"),
    combined_plus_ctrls=Input(rid="ri.foundry.main.dataset.4d13cdf1-0162-4b39-91a3-80048bce4bb1")
)
# filter combined_plus_ctrls to patients with CCI scores from sites with high ICD coverage

def only_cci(charlson_scores, combined_plus_ctrls):
    charlson = charlson_scores.where(F.col('site_icd_gt_80pct') == 1).select('person_id', 'cci_score')
    df = combined_plus_ctrls.join(charlson, 'person_id', how='inner')
    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.9cd5e32a-1933-422c-8bd6-59808dc400df"),
    pasc_conditions=Input(rid="ri.foundry.main.dataset.ee300e81-4e7e-46ce-a18e-4dc5d0316e65"),
    recover_release_concept=Input(rid="ri.foundry.main.dataset.6fd878f0-895f-48fa-ad01-999883522a54")
)
# examine the main and source concepts being captured by the standard OMOP PASC concepts in the data
# the results of this node were used to set appropriate filters for pasc_conditions node

def pasc_condition_breakdown(pasc_conditions, recover_release_concept):
    df = (
        pasc_conditions
        .groupBy('condition_concept_id', 'condition_concept_name', 'condition_source_concept_id', 'condition_source_concept_name')
        .count()    
    )
    df = (
        df
        .join(recover_release_concept, (df['condition_source_concept_id'] == recover_release_concept['concept_id']), how='left')
        .select('condition_concept_id', 'condition_concept_name', 'count', 'condition_source_concept_id', 'condition_source_concept_name', 'vocabulary_id', 'concept_code')
    )
    return df.sort('condition_concept_id', 'count', ascending=False)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.82fbed64-e834-4e15-b3f3-1b5625f46b0f"),
    recover_release_condition_occurrence=Input(rid="ri.foundry.main.dataset.ef3dcd85-f845-4710-923f-ceaeb71155e4")
)
# get count of PASC dxs per site, and rate of patients with PASC dx per site

def pasc_sites1(recover_release_condition_occurrence):
    partner_list = recover_release_condition_occurrence.select('data_partner_id', 'person_id').distinct().groupBy('data_partner_id').count().withColumnRenamed('count', 'site_total')
    df = recover_release_condition_occurrence.where((F.col('condition_concept_id') == 705076) & (F.col('condition_source_concept_id').isin(766503, 710706))).select('data_partner_id', 'person_id').distinct()
    df = df.groupBy('data_partner_id').count()
    df = partner_list.join(df, 'data_partner_id', how='left')
    df = df.withColumn('pasc_count', F.when(F.col('count').isNull(), 0).otherwise(F.col('count'))).drop('count')
    df = df.withColumn('pasc_per_thous', ((F.col('pasc_count') / F.col('site_total')) * 1000))
    return df.sort('pasc_per_thous')

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.46d0583e-1390-4f2e-b09d-e6553ffc36aa"),
    recover_release_person=Input(rid="ri.foundry.main.dataset.41c06cfb-7df3-4400-95bf-809274b63318"),
    site_dx_use1=Input(rid="ri.foundry.main.dataset.456dbf94-f320-4a66-976a-7c8c58751a95")
)
# filter person table to patients from data partner sites with >0 occurrences of ME/CFS and PASC dxs

def person_reduced_sites(site_dx_use1, recover_release_person):
    sites = site_dx_use1.where((F.col('mecfs_count') > 0) & (F.col('pasc_count') > 0)).select('data_partner_id')
    print(sites.count())
    df = recover_release_person.join(sites, 'data_partner_id', how='inner')
    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.500c7920-49b4-4afb-af1e-002c44c88aa4"),
    combined_groups=Input(rid="ri.foundry.main.dataset.01dccdc0-0498-46ed-915c-e402d1cf2bd6"),
    r5382_pre_group=Input(rid="ri.foundry.main.dataset.53676f49-e34c-463f-9758-f78fc35ebdf1")
)
# exclude any patients that met criteria for a case group
# (the condition_concept_id used to identify these patients already precludes any overlap with controls)

def r5382_group(r5382_pre_group, combined_groups):
    df = r5382_pre_group.join(combined_groups, 'person_id', how='anti')
    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.456dbf94-f320-4a66-976a-7c8c58751a95"),
    mecfs_sites1=Input(rid="ri.foundry.main.dataset.f6a3a7a7-74b0-4a99-9f38-f380060654a0"),
    pasc_sites1=Input(rid="ri.foundry.main.dataset.82fbed64-e834-4e15-b3f3-1b5625f46b0f")
)
def site_dx_use1(mecfs_sites1, pasc_sites1):
    df = mecfs_sites1.join(pasc_sites1.drop('site_total'), 'data_partner_id', how='outer')
    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.8d152c24-76f3-43a8-a986-9174a5c3c82c"),
    person_reduced_sites=Input(rid="ri.foundry.main.dataset.46d0583e-1390-4f2e-b09d-e6553ffc36aa"),
    recover_release_manifest_1=Input(rid="ri.foundry.main.dataset.d0c9e0dc-2dac-4d3f-8391-51664bf249e3")
)
def sites_details(recover_release_manifest_1, person_reduced_sites):
    sites = person_reduced_sites.select('data_partner_id').distinct()
    df = recover_release_manifest_1.join(sites, 'data_partner_id', how='inner')
    return df

