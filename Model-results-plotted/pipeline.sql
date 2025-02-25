-- Authors: Abhishek Bhatia, Tomas McIntee, John Powers
-- © 2025, The University of North Carolina at Chapel Hill. Permission is granted to use in accordance with the MIT license.
-- The code is licensed under the MIT license.


@transform_pandas(
    Output(rid="ri.foundry.main.dataset.e92fdec1-3078-45b0-afde-706b53cce589"),
    S2=Input(rid="ri.foundry.main.dataset.fc80da73-ad6c-40b8-9b22-80336b6f406c")
)
SELECT CP_PASC,
    CP_ME_CFS,
    sum(count_in_subgroup) AS count_in_group
FROM S2
GROUP BY CP_PASC, CP_ME_CFS

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.04c12a91-8138-4ef8-9262-bc02816d8a14"),
    S2=Input(rid="ri.foundry.main.dataset.fc80da73-ad6c-40b8-9b22-80336b6f406c")
)
SELECT cohort_label,
    sum(count_in_subgroup) AS count_in_group
FROM S2
GROUP BY cohort_label

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.fe059d7e-eeee-4ee4-8f59-75e9d86f4502"),
    data_switch_container=Input(rid="ri.foundry.main.dataset.ff35d9f3-f359-4aca-9d8a-3806c8a2675a")
)
SELECT cohort_label,
    CASE WHEN age_bin <= 2 THEN '18-29'
        WHEN age_bin = 3 THEN '30-39'
        WHEN age_bin = 4 THEN '40-49'
        WHEN age_bin = 5 THEN '50-59'
        WHEN age_bin = 6 THEN '60-69'
        WHEN age_bin >= 7 THEN '70+'
    END AS row_name,
    COUNT(*) AS count_in_bin,
    COUNT(CASE WHEN CP_PASC THEN 1 END) AS PASC_CP,
    COUNT(CP_PASC) AS PASC_CP_total,
    COUNT(CASE WHEN CP_ME_CFS THEN 1 END) AS MECFS_CP,
    COUNT(CP_ME_CFS) AS MECFS_CP_total,
    'age_bin' AS subtable
FROM data_switch_container 
GROUP BY cohort_label, age_bin

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.4c03c233-704e-4946-96fd-8a097ba196c1"),
    data_switch_container=Input(rid="ri.foundry.main.dataset.ff35d9f3-f359-4aca-9d8a-3806c8a2675a")
)
SELECT person_id,
    cohort_label,
    CP_PASC,
    CP_ME_CFS,
    CAST ((MONTH(index_date)+2)/3 AS INTEGER) AS quarter_index,
    YEAR(index_date) AS year_index

FROM data_switch_container

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.bd34215a-88e4-4872-a363-ff598dc921c5"),
    data_switch_container=Input(rid="ri.foundry.main.dataset.ff35d9f3-f359-4aca-9d8a-3806c8a2675a")
)
SELECT cohort_label,
    race AS row_name,
    COUNT(*) AS count_in_bin,
    COUNT(CASE WHEN CP_PASC THEN 1 END) AS PASC_CP,
    COUNT(CP_PASC) AS PASC_CP_total,
    COUNT(CASE WHEN CP_ME_CFS THEN 1 END) AS MECFS_CP,
    COUNT(CP_ME_CFS) AS MECFS_CP_total,
    'race' AS subtable
FROM data_switch_container
WHERE race != 'Middle Eastern or North African' 
GROUP BY cohort_label, race

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.e813678c-3284-44db-98d2-3ded54cf1754"),
    data_switch_container=Input(rid="ri.foundry.main.dataset.ff35d9f3-f359-4aca-9d8a-3806c8a2675a")
)
SELECT cohort_label,
    sex AS row_name,
    COUNT(*) AS count_in_bin,
    COUNT(CASE WHEN CP_PASC THEN 1 END) AS PASC_CP,
    COUNT(CP_PASC) AS PASC_CP_total,
    COUNT(CASE WHEN CP_ME_CFS THEN 1 END) AS MECFS_CP,
    COUNT(CP_ME_CFS) AS MECFS_CP_total,
    'sex' AS subtable
FROM data_switch_container 
GROUP BY cohort_label, sex

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.e032fb0e-0d41-46f7-ad93-684828cdff3c"),
    data_switch_container=Input(rid="ri.foundry.main.dataset.ff35d9f3-f359-4aca-9d8a-3806c8a2675a")
)
SELECT cohort_label,
    CASE WHEN cci_score >= 7 THEN '7+'
        WHEN cci_score = 0 THEN '0'
        WHEN cci_score BETWEEN 1 AND 2 THEN '1-2'
        WHEN cci_score BETWEEN 3 AND 4 THEN '3-4'
        WHEN cci_score BETWEEN 5 AND 6 THEN '5-6'
        ELSE CAST(cci_score AS string) 
    END AS row_name,
    COUNT(*) AS count_in_bin,
    COUNT(CASE WHEN CP_PASC THEN 1 END) AS PASC_CP,
    COUNT(CP_PASC) AS PASC_CP_total,
    COUNT(CASE WHEN CP_ME_CFS THEN 1 END) AS MECFS_CP,
    COUNT(CP_ME_CFS) AS MECFS_CP_total,
    'cci_score' AS subtable
FROM data_switch_container 
GROUP BY cohort_label, cci_score

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.83c66546-c74c-4cd0-b3c1-bd939da9c83c"),
    Matched_test_cfs_copied=Input(rid="ri.foundry.main.dataset.14e71fe9-d249-488e-b465-e1ff4e3f7c2f"),
    Matched_test_pasc_copied=Input(rid="ri.foundry.main.dataset.9a1004fe-f04b-4357-b204-3eba0c2ce47d")
)
SELECT *
FROM Matched_test_pasc_copied
UNION
SELECT *
FROM Matched_test_cfs_copied

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.ff35d9f3-f359-4aca-9d8a-3806c8a2675a"),
    Base_subset=Input(rid="ri.foundry.main.dataset.f050f145-1a51-4653-af41-3893a03216bd"),
    Full_test_cohort_scored=Input(rid="ri.foundry.main.dataset.5d36ccd6-4154-4e9f-83e8-e8d9e064704f"),
    Matched_test_cfs_copied=Input(rid="ri.foundry.main.dataset.14e71fe9-d249-488e-b465-e1ff4e3f7c2f"),
    Matched_test_pasc_copied=Input(rid="ri.foundry.main.dataset.9a1004fe-f04b-4357-b204-3eba0c2ce47d"),
    combined_matched=Input(rid="ri.foundry.main.dataset.83c66546-c74c-4cd0-b3c1-bd939da9c83c"),
    full_cohort=Input(rid="ri.foundry.main.dataset.cb6831a5-504e-4bce-a271-b7d72937eb9f"),
    full_cohort_test_and_train=Input(rid="ri.foundry.main.dataset.ab759106-68ab-48ba-bf5c-873c4752d8a1")
)
SELECT person_id,
    cohort_label,
    sex,
    race,
    ethnicity,
    age_bin,
    index_date,
    cci_score,
    PASC_score,
    CFS_score,
    PASC_score > 0.5 AS CP_PASC,
    CFS_score > 0.5 AS CP_ME_CFS,
    poverty_status,
    health_insurance_19to64 AS insurance
--FROM Base_subset -- Small subset with controlled cohort sizes, has 104,500 total (exactly 100,000 if either the PASC or CFS cohorts are eliminated)
--FROM Full_test_cohort_scored -- Full subset, takes a long time to run, disproportionately small number of PASC / CFS because the matched training set has been subtracted.
-- FROM full_cohort_test_and_train -- F
--FROM Matched_test_cfs_copied
--FROM Matched_test_pasc_copied
--FROM combined_matched
FROM full_cohort

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.9cb42b5e-ab85-40a3-b8f8-02db5ff3790c"),
    data_switch_container=Input(rid="ri.foundry.main.dataset.ff35d9f3-f359-4aca-9d8a-3806c8a2675a")
)
SELECT cohort_label,
    ethnicity AS row_name,
    COUNT(*) AS count_in_bin,
    COUNT(CASE WHEN CP_PASC THEN 1 END) AS PASC_CP,
    COUNT(CP_PASC) AS PASC_CP_total,
    COUNT(CASE WHEN CP_ME_CFS THEN 1 END) AS MECFS_CP,
    COUNT(CP_ME_CFS) AS MECFS_CP_total,
    'ethnicity' AS subtable
FROM data_switch_container 
GROUP BY cohort_label, ethnicity

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.a59c701f-f7f0-4978-b9ec-4ca4f047f82a"),
    features_05_threshold=Input(rid="ri.foundry.main.dataset.b6428f8c-c169-483b-8a0e-073bf8312df1")
)
SELECT condition_concept_name AS Condition,
    CASE WHEN cfs_mean_shapley > 0.05 AND pasc_mean_shapley > 0.05 THEN 'Indicates both strongly'
        WHEN pasc_mean_shapley > 0.05 AND cfs_mean_shapley <= 0.05 THEN 'Strong PASC indicator'
        WHEN cfs_mean_shapley > 0.05 AND pasc_mean_shapley <= 0.05 THEN 'Strong ME/CFS indicator'
        ELSE ''
    END AS Indicator_type,
    ROUND(cfs_mean_shapley,2) AS CP_ME_CFS_importance,
    ROUND(pasc_mean_shapley,2) AS CP_PASC_importance
FROM features_05_threshold
ORDER BY CP_PASC_importance DESC

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.0fce4305-556b-4407-bd0d-4a1b3bc97890"),
    Feature_importance=Input(rid="ri.foundry.main.dataset.acd2302f-e989-4d58-838e-dd0a9b27b528")
)
SELECT condition_concept_name,
    cfs_mean_shapley,
    pasc_mean_shapley,
    diff_mean_shapley
FROM Feature_importance
WHERE condition_concept_name != 'is_female'
    AND condition_concept_name != 'age_bin'
    AND (cfs_mean_shapley > 0.01
        --OR diff_mean_shapley > 0.05 --Nicotine dependence 
        --OR diff_mean_shapley < -0.05 --Cough; chronic cough is the stronger indicator anyway.
        OR pasc_mean_shapley > 0.01)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b6428f8c-c169-483b-8a0e-073bf8312df1"),
    Feature_importance=Input(rid="ri.foundry.main.dataset.acd2302f-e989-4d58-838e-dd0a9b27b528")
)
SELECT condition_concept_name,
    cfs_mean_shapley,
    pasc_mean_shapley,
    diff_mean_shapley
FROM Feature_importance
WHERE condition_concept_name != 'is_female'
    AND condition_concept_name != 'age_bin'
    AND (cfs_mean_shapley > 0.05
        --OR diff_mean_shapley > 0.05 --Nicotine dependence 
        --OR diff_mean_shapley < -0.05 --Cough; chronic cough is the stronger indicator anyway.
        OR pasc_mean_shapley > 0.05)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.ab759106-68ab-48ba-bf5c-873c4752d8a1"),
    Full_cohort_w_ml_vars=Input(rid="ri.foundry.main.dataset.5aa7b269-ea4c-4978-9cf5-5dbf92373cca"),
    Full_test_cohort_scored=Input(rid="ri.foundry.main.dataset.5d36ccd6-4154-4e9f-83e8-e8d9e064704f")
)
SELECT Full_cohort_w_ml_vars.*,
    Full_test_cohort_scored.CFS_score,
    Full_test_cohort_scored.PASC_score
FROM Full_cohort_w_ml_vars FULL OUTER JOIN Full_test_cohort_scored ON Full_cohort_w_ml_vars.person_id = Full_test_cohort_scored.person_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.7963cecf-19ad-4197-9105-ed97c90101c8"),
    data_switch_container=Input(rid="ri.foundry.main.dataset.ff35d9f3-f359-4aca-9d8a-3806c8a2675a")
)
SELECT cohort_label,
    insurance AS row_name,
    COUNT(*) AS count_in_bin,
    COUNT(CASE WHEN CP_PASC THEN 1 END) AS PASC_CP,
    COUNT(CP_PASC) AS PASC_CP_total,
    COUNT(CASE WHEN CP_ME_CFS THEN 1 END) AS MECFS_CP,
    COUNT(CP_ME_CFS) AS MECFS_CP_total,
    'insurance' AS subtable
FROM data_switch_container 
WHERE insurance IS NOT NULL
GROUP BY cohort_label, insurance

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.2aee3f87-d0db-4825-a4f7-bd31264f04a3"),
    by_quarter_dates=Input(rid="ri.foundry.main.dataset.4c03c233-704e-4946-96fd-8a097ba196c1")
)
SELECT cohort_label,
    year_index,
    quarter_index,
    COUNT(*) AS number
FROM by_quarter_dates
WHERE year_index != 2017
GROUP BY cohort_label, year_index, quarter_index 

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.97e710d7-ab82-40be-ab8b-1f6446451c7a"),
    by_quarter_dates=Input(rid="ri.foundry.main.dataset.4c03c233-704e-4946-96fd-8a097ba196c1")
)
SELECT CP_PASC,
    CP_ME_CFS,
    year_index,
    quarter_index,
    COUNT(*) AS number
FROM by_quarter_dates
WHERE year_index != 2017 --Group too small for prevalence figures
GROUP BY CP_PASC, CP_ME_CFS, year_index, quarter_index

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.fae9099d-0de9-42c9-bf30-b0436ec6017a"),
    data_switch_container=Input(rid="ri.foundry.main.dataset.ff35d9f3-f359-4aca-9d8a-3806c8a2675a")
)
SELECT cohort_label,
    poverty_status AS row_name,
    COUNT(*) AS count_in_bin,
    COUNT(CASE WHEN CP_PASC THEN 1 END) AS PASC_CP,
    COUNT(CP_PASC) AS PASC_CP_total,
    COUNT(CASE WHEN CP_ME_CFS THEN 1 END) AS MECFS_CP,
    COUNT(CP_ME_CFS) AS MECFS_CP_total,
    'poverty_status' AS subtable
FROM data_switch_container 
WHERE poverty_status IS NOT NULL
GROUP BY cohort_label, poverty_status

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.29d1cbfc-23b9-4150-8d27-37085cd5f857"),
    data_switch_container=Input(rid="ri.foundry.main.dataset.ff35d9f3-f359-4aca-9d8a-3806c8a2675a")
)
SELECT cohort_label,
    CP_PASC,
    CP_ME_CFS,
    COUNT(*) AS count_in_subgroup
FROM data_switch_container
GROUP BY cohort_label, CP_PASC, CP_ME_CFS

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.117f50da-b920-4302-99a7-d7d75f972782"),
    Feature_importance=Input(rid="ri.foundry.main.dataset.acd2302f-e989-4d58-838e-dd0a9b27b528")
)
SELECT DISTINCT * FROM
(SELECT condition_concept_name,
    cfs_mean_shapley,
    pasc_mean_shapley
FROM Feature_importance
ORDER BY cfs_mean_shapley DESC
LIMIT 30)
UNION 
(SELECT condition_concept_name,
    cfs_mean_shapley,
    pasc_mean_shapley
FROM Feature_importance
ORDER BY pasc_mean_shapley DESC
LIMIT 30)
ORDER BY cfs_mean_shapley DESC

