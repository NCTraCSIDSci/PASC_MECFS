# Authors: Abhishek Bhatia, Tomas McIntee, John Powers
# © 2025, The University of North Carolina at Chapel Hill. Permission is granted to use in accordance with the MIT license.
# The code is licensed under the MIT license.

import pyspark.sql.functions as F

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.cb6831a5-504e-4bce-a271-b7d72937eb9f"),
    Full_cohort_w_ml_vars_1=Input(rid="ri.foundry.main.dataset.5aa7b269-ea4c-4978-9cf5-5dbf92373cca"),
    Full_test_cohort_scored=Input(rid="ri.foundry.main.dataset.5d36ccd6-4154-4e9f-83e8-e8d9e064704f"),
    sdoh=Input(rid="ri.foundry.main.dataset.d373e6c6-be32-4b43-9a98-6e6f3f0e675b")
)
def full_cohort(Full_cohort_w_ml_vars_1, Full_test_cohort_scored, sdoh):
    df = Full_test_cohort_scored.select('person_id', 'PASC_score', 'CFS_score')
    df2 = Full_cohort_w_ml_vars_1.join(df, 'person_id', how='left')
    df3 = df2.join(sdoh, 'person_id', how='left')
    return df3

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.d373e6c6-be32-4b43-9a98-6e6f3f0e675b"),
    Full_cohort_w_ml_vars_1=Input(rid="ri.foundry.main.dataset.5aa7b269-ea4c-4978-9cf5-5dbf92373cca"),
    ZCTA_by_SDoH_percentages=Input(rid="ri.foundry.main.dataset.f6ce6698-905f-445b-a7b6-4b3894d73d61"),
    ZiptoZcta_Crosswalk_2021_ziptozcta2020=Input(rid="ri.foundry.main.dataset.99aaa287-8c52-4809-b448-6e46999a6aa7"),
    recover_release_location=Input(rid="ri.foundry.main.dataset.cdcdc3c5-bc22-4c7c-a11e-423dcc817fcc"),
    recover_release_person=Input(rid="ri.foundry.main.dataset.41c06cfb-7df3-4400-95bf-809274b63318")
)
def sdoh(recover_release_person, recover_release_location, Full_cohort_w_ml_vars_1, ZiptoZcta_Crosswalk_2021_ziptozcta2020, ZCTA_by_SDoH_percentages):
    df = recover_release_person.join(Full_cohort_w_ml_vars_1.select('person_id'), 'person_id', how='inner').select('person_id', 'location_id')
    df = df.join(recover_release_location, 'location_id', how='inner').select('person_id', 'zip')
    df = df.join(ZiptoZcta_Crosswalk_2021_ziptozcta2020, [df['zip'] == ZiptoZcta_Crosswalk_2021_ziptozcta2020['ZIP_CODE']], how='inner').select('person_id', 'ZCTA')
    df = df.join(ZCTA_by_SDoH_percentages.select('ZCTA', 'poverty_status', 'health_insurance_19to64'), 'ZCTA', how='inner')
    df = df.withColumn('poverty_status', F.when(F.col('poverty_status') < 10, 'lt_10')
                                            .when(F.col('poverty_status') < 20, '10-20')
                                            .when(F.col('poverty_status') < 30, '20-30')
                                            .when(F.col('poverty_status') < 40, '30-40')
                                            .when(F.col('poverty_status') < 50, '40-50')
                                            .when(F.col('poverty_status') >= 50, 'at_least_50'))
    df = df.withColumn('health_insurance_19to64', F.when(F.col('health_insurance_19to64') > 90, 'gt_90')
                                            .when(F.col('health_insurance_19to64') > 80, '80-90')
                                            .when(F.col('health_insurance_19to64') > 70, '70-80')
                                            .when(F.col('health_insurance_19to64') > 60, '60-70')
                                            .when(F.col('health_insurance_19to64') <= 60, '60_or_less'))
    return df
    

@transform_pandas(
    Output(rid="ri.vector.main.execute.6a3baca3-4881-4d89-899e-003bf3ceae28"),
    data_switch_container=Input(rid="ri.foundry.main.dataset.ff35d9f3-f359-4aca-9d8a-3806c8a2675a")
)
def unnamed_1(data_switch_container):
    print(data_switch_container.where((F.col('CP_ME_CFS')) & (F.col('race') == 'White')).count())
    print(data_switch_container.where((F.col('CP_ME_CFS').isNotNull()) & (F.col('race') == 'White')).count())

