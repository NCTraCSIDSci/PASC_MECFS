-- Authors: Abhishek Bhatia, Tomas McIntee, John Powers
-- © 2025, The University of North Carolina at Chapel Hill. Permission is granted to use in accordance with the MIT license.
-- The code is licensed under the MIT license.


@transform_pandas(
    Output(rid="ri.foundry.main.dataset.21668fb9-8415-49ca-bb72-8f910117ca58"),
    conditions_resolve_icd9=Input(rid="ri.foundry.main.dataset.f0819050-0b33-4688-8128-7c6c2deb9e0e")
)
-- Get ICD10 code FROM conditions, assign to charlson groups, assign charlson weights to groups
-- Codes based on U Manitoba algorithm (originally, Quan et al. 2005)
-- Weights based on Quan et al. 2011

SELECT b.*,
-- CASE WHEN charlson_group between 1 and 10 THEN 1
--      WHEN charlson_group between 11 and 14 THEN 2
--      WHEN charlson_group==15 THEN 3
--      WHEN charlson_group in (16,17) THEN 6
--      ELSE 0 end as charlson_weight,
CASE WHEN charlson_group in (1,3,4,8,10) THEN 0
     WHEN charlson_group in (6,7,11,13) THEN 1
     WHEN charlson_group in (2,5,9,12,14) THEN 2
     WHEN charlson_group in (15,17) THEN 4
     WHEN charlson_group==16 THEN 6
     ELSE 0 end as quan_weight

FROM (
    SELECT a.*,
         --acute or historical mi
    CASE WHEN substring(icd,1,3) in ('I21','I22') THEN 1
         WHEN substring(icd,1,4) in ('I252') THEN 1
         --chf
         WHEN substring(icd,1,3) in ('I43','I50') THEN 2
         WHEN substring(icd,1,4) in ('I099','I110','I130','I132','I255','I420','I425','I426','I427','I428','I429','P290') THEN 2 
         --peripheral vascular disease
         WHEN substring(icd,1,3) in ('I70','I71') THEN 3 
         WHEN substring(icd,1,4) in ('I731','I738','I739','I771','I790','I792','K551','K558','K559','Z958','Z959') THEN 3
         --cerebrovascular disease
         WHEN substring(icd,1,3) in ('G45','G46','I60','I61','I62','I63','I64','I65','I66','I67','I68','I69') THEN 4
         WHEN substring(icd,1,4) in ('H340') THEN 4 
         --dementia
         WHEN substring(icd,1,3) in ('F00','F01','F02','F03','G30') THEN 5
         WHEN substring(icd,1,4) in ('F051','G311') THEN 5
         --copd
         WHEN substring(icd,1,3) in ('J40','J41','J42','J43','J44','J45','J46','J47','J60','J61','J62','J63','J64','J65','J66','J67') THEN 6
         WHEN substring(icd,1,4) in ('I278','I279','J684','J701','J703') THEN 6
         --rheumatic disease
         WHEN substring(icd,1,3) in ('M32','M33','M34','M06','M05') THEN 7
         WHEN substring(icd,1,4) in ('M315','M351','M353','M360') THEN 7
         --peptic ulcer
         WHEN substring(icd,1,3) in ('K25','K26','K27','K28') THEN 8
         --mild liver disease
         WHEN substring(icd,1,3) in ('B18','K73','K74') THEN 9
         WHEN substring(icd,1,4) in ('K700','K701','K702','K703','K709','K717','K713','K714','K715','K760','K762','K763','K764','K768','K769','Z944') THEN 9
         --diabetes
         WHEN substring(icd,1,4) in ('E100','E101','E106','E108','E109','E110','E111','E116','E118','E119','E120','E121','E126','E128','E129','E130','E131','E136','E138','E139','E140','E141','E146','E148','E149') THEN 10 
         --diabaetes w/chronic complications
         WHEN substring(icd,1,4) in ('E102','E103','E104','E105','E107','E112','E113','E114','E115','E117','E122','E123','E124','E125','E127','E132','E133','E134','E135','E137','E142','E143','E144','E145','E147') THEN 11
         --paralysis
         WHEN substring(icd,1,3) in ('G81','G82') THEN 12
         WHEN substring(icd,1,4) in ('G041','G114','G801','G802','G830','G831','G832','G833','G834','G839') THEN 12
         --renal disease
         WHEN substring(icd,1,3) in ('N18','N19') THEN 13
         WHEN substring(icd,1,4) in ('N052','N053','N054','N055','N056','N057','N250','I120','I131','N032','N033','N034','N035','N036','N037','Z490','Z491','Z492','Z940','Z992') THEN 13
         --localized cancer/leukemia/lymphoma
         WHEN substring(icd,1,3) in ('C00','C01','C02','C03','C04','C05','C06','C07','C08','C09','C10','C11',
                             'C12','C13','C14','C15','C16','C17','C18','C19','C20','C21','C22','C23',
                             'C24','C25','C26','C30','C31','C32','C33','C34','C37','C38','C39','C40',
                             'C41','C43','C45','C46','C47','C48','C49','C50','C51','C52','C53','C54',
                             'C55','C56','C57','C58','C60','C61','C62','C63','C64','C65','C66','C67',
                             'C68','C69','C70','C71','C72','C73','C74','C75','C76','C81','C82','C83',
                             'C84','C85','C88','C90','C91','C92','C93','C94','C95','C96','C97') THEN 14
         --moderative/severe liver disease
         WHEN substring(icd,1,4) in ('K704','K711','K721','K729','K765','K766','K767','I850','I859','I864','I982') THEN 15
         --metastatic cancer
         WHEN substring(icd,1,3) in ('C77','C78','C79','C80') THEN 16
         --hiv/aids
         WHEN substring(icd,1,3) in ('B20','B21','B22','B24') THEN 17 
    end as charlson_group

    FROM (
        SELECT DISTINCT 
            person_id,
            data_partner_id,
            condition_source_value,
            replace(regexp_extract(replace(condition_source_value_cleaned,'ICD10',''), '([A-Z][0-9][0-9A-Z][.]{0,1}[0-9A-Z]{0,4})'),'.','') as icd
        FROM conditions_resolve_icd9
    ) a

) b

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.8e8089df-225e-48dd-b53f-5d2983e62b7c"),
    charlson_groups_weights=Input(rid="ri.foundry.main.dataset.21668fb9-8415-49ca-bb72-8f910117ca58"),
    site_icd_info=Input(rid="ri.foundry.main.dataset.486432c3-2aad-4189-83d7-e686e9c5d67e")
)
-- CCI scores at the patient level
-- `cci_score`: CCI using ICD10 codeset (Quan et al. 2005) and updated weights (Quan et al. 2011)
-- `site_icd_pct_cvg`: % of condition occurrences that use ICD10 codes at the patient's data partner.
-- `site_icd_gt_80%`: If a patient's site has an ICD coverage of > 80%.
-- `OMOP_site`: If a patient's site is OMOP native. OMOP native sites may, but are not expected to, use ICD source values.

SELECT b.person_id, b.data_partner_id, b.cci_score, c.site_icd_pct_cvg, c.site_icd_gt_80pct, c.OMOP_site
FROM (
    SELECT a.person_id, a.data_partner_id, sum(a.quan_weight) as cci_score
    FROM (
        SELECT DISTINCT person_id, data_partner_id, charlson_group, quan_weight
        FROM charlson_groups_weights
    ) a
    GROUP BY a.person_id, a.data_partner_id
) b
LEFT JOIN site_icd_info as c
ON b.data_partner_id = c.data_partner_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.4d13cdf1-0162-4b39-91a3-80048bce4bb1"),
    combined_groups=Input(rid="ri.foundry.main.dataset.01dccdc0-0498-46ed-915c-e402d1cf2bd6"),
    ctrl_group_w_index=Input(rid="ri.foundry.main.dataset.cc0b2c49-42e2-42d3-8de4-42507681c132"),
    r5382_group=Input(rid="ri.foundry.main.dataset.500c7920-49b4-4afb-af1e-002c44c88aa4")
)
SELECT * FROM combined_groups
UNION
SELECT * FROM ctrl_group_w_index
UNION
SELECT * FROM r5382_group

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.3f85a04e-5c07-49d2-8d5a-6f13786f96cc"),
    conditions_replace_IMO_prefix=Input(rid="ri.foundry.main.dataset.c6671c91-bb52-4a4a-a5a7-f52a7c0716db")
)
-- Addresses data quality issues associated with abbreviating ICD10 as I10 (which could get assigned as hypertension)

SELECT condition_occurrence_id, 
       person_id, 
       data_partner_id, 
       condition_source_value,
       regexp_replace(condition_source_value_cleaned,'I10:','') as condition_source_value_cleaned
FROM conditions_replace_IMO_prefix

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c6671c91-bb52-4a4a-a5a7-f52a7c0716db"),
    condition_occurrences=Input(rid="ri.foundry.main.dataset.f1ca1106-2d83-4c4e-bd66-264c4fdf7880")
)
-- Addresses data quality issues associated with IMO0001 and IMO0002 codes
-- Apparently, this is an EHR code for some systems if there is a mapping issue

SELECT condition_occurrence_id, 
       person_id, 
       data_partner_id, 
       condition_source_value,
       regexp_replace(condition_source_value,'IMO000[1,2]','') as condition_source_value_cleaned
FROM condition_occurrences

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f0819050-0b33-4688-8128-7c6c2deb9e0e"),
    conditions_resolve_multi_icd=Input(rid="ri.foundry.main.dataset.0bf6e59f-ed2b-4887-b22f-48fa9ad32143")
)
SELECT condition_occurrence_id, 
       person_id, 
       data_partner_id, 
       condition_source_value,
       regexp_replace(condition_source_value_cleaned,'ICD9[\\S]*','') as condition_source_value_cleaned
FROM conditions_resolve_multi_icd

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.8eacbc96-9347-4c9a-bc01-7554679047dd"),
    person_reduced_sites=Input(rid="ri.foundry.main.dataset.46d0583e-1390-4f2e-b09d-e6553ffc36aa"),
    recover_release_condition_occurrence=Input(rid="ri.foundry.main.dataset.ef3dcd85-f845-4710-923f-ceaeb71155e4")
)
-- get patients from the person table that don't have any ME/CFS or PASC diagnoses
-- and use the same general filtering criteria used for those groups

WITH pos AS (
SELECT person_id
FROM recover_release_condition_occurrence
WHERE recover_release_condition_occurrence.condition_concept_id IN (432738, 44793522, 44793523, 44793521, 705076)
GROUP BY person_id
)

SELECT 
    person_reduced_sites.person_id,
    person_reduced_sites.gender_concept_name AS sex,
    CASE WHEN person_reduced_sites.race_concept_name IS NULL THEN 'Unknown'
        WHEN person_reduced_sites.race_concept_name IN ('No matching concept', 'No information', 'Unknown racial group', 'Refuse to answer') THEN 'Unknown'
        WHEN person_reduced_sites.race_concept_name = 'Other' THEN 'Other Race'
        WHEN person_reduced_sites.race_concept_name = 'Multiple race' THEN 'Multiple races'
        WHEN person_reduced_sites.race_concept_name IN ('Korean', 'Filipino', 'Chinese', 'Asian Indian', 'Japanese', 'Vietnamese') THEN 'Asian'
        WHEN person_reduced_sites.race_concept_name IN ('Asian or Pacific islander', 'Other Pacific Islander') THEN 'Native Hawaiian or Other Pacific Islander'
        ELSE person_reduced_sites.race_concept_name END AS race,
    CASE WHEN person_reduced_sites.ethnicity_concept_name IS NULL THEN 'Unknown'
        WHEN person_reduced_sites.ethnicity_concept_name IN ('No matching concept', 'Other', 'No information', 'Other/Unknown') THEN 'Unknown'
        ELSE person_reduced_sites.ethnicity_concept_name END AS ethnicity,
    person_reduced_sites.birth_date AS birth_date,
    0 AS cfs,
    cast(NULL AS date) AS cfs_index,
    cast(NULL AS boolean) AS pre_covid_cfs,
    0 AS pasc,
    cast(NULL AS date) AS pasc_index,
    cast(NULL AS boolean) AS pre_covid_pasc,
    'CTRL' AS cohort_label
FROM person_reduced_sites ANTI JOIN pos ON person_reduced_sites.person_id = pos.person_id
WHERE person_reduced_sites.birth_date IS NOT NULL
    AND (person_reduced_sites.gender_concept_name = 'FEMALE' OR person_reduced_sites.gender_concept_name = 'MALE')

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.cc0b2c49-42e2-42d3-8de4-42507681c132"),
    ctrl_group=Input(rid="ri.foundry.main.dataset.8eacbc96-9347-4c9a-bc01-7554679047dd"),
    random_condition_occurrence=Input(rid="ri.foundry.main.dataset.75ab2daa-3cbf-416f-a4c2-3578f14c14fd")
)
-- set index date as the condition_start_date from a random condition occurrence record for each patient in ctrl_group
-- and filter to patients age >= 18 at the index date

SELECT ctrl_group.*, random_condition_occurrence.condition_start_date AS index_date
FROM ctrl_group INNER JOIN random_condition_occurrence ON ctrl_group.person_id = random_condition_occurrence.person_id
WHERE FLOOR(DATEDIFF(random_condition_occurrence.condition_start_date, ctrl_group.birth_date) / 365.25) >= 18

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.5aa7b269-ea4c-4978-9cf5-5dbf92373cca"),
    only_cci=Input(rid="ri.foundry.main.dataset.2f1b4413-2d6e-4272-bb33-940ac2305fc7")
)
SELECT only_cci.*,
    only_cci.sex = 'FEMALE' AS is_female,
    FLOOR(DATEDIFF(only_cci.index_date, only_cci.birth_date)/3652.5) AS age_bin,
    YEAR(index_date) < 2020 AS pre_covid 
FROM only_cci

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.477a08c7-43be-4a98-bf41-cc1d7ee9ed70"),
    condition_occurrences=Input(rid="ri.foundry.main.dataset.f1ca1106-2d83-4c4e-bd66-264c4fdf7880"),
    conditions_resolve_multi_icd=Input(rid="ri.foundry.main.dataset.0bf6e59f-ed2b-4887-b22f-48fa9ad32143"),
    full_cohort_w_ml_vars=Input(rid="ri.foundry.main.dataset.5aa7b269-ea4c-4978-9cf5-5dbf92373cca")
)
SELECT
    fc.person_id,
    crm.condition_source_value_cleaned,
    CAST(co.condition_start_date AS DATE) AS condition_start_date
FROM
    full_cohort_w_ml_vars fc
LEFT JOIN conditions_resolve_multi_icd crm ON fc.person_id = crm.person_id
LEFT JOIN condition_occurrences co ON crm.condition_occurrence_id = co.condition_occurrence_id
WHERE
    crm.condition_source_value_cleaned IN ('G93.32', 'R53.82', 'U09.9')

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c1b3bbca-c279-45fe-836e-6efc25113e71"),
    full_cohort_w_ml_vars=Input(rid="ri.foundry.main.dataset.5aa7b269-ea4c-4978-9cf5-5dbf92373cca")
)
SELECT 
    index_date,
    cohort_label,
    COUNT(*) AS incidence_count
FROM 
    full_cohort_w_ml_vars
WHERE 
    cohort_label != 'CTRL'
GROUP BY 
    index_date, 
    cohort_label
ORDER BY 
    index_date ASC;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.53196dd0-0f4a-4261-9d45-dbe85f559950"),
    person_reduced_sites=Input(rid="ri.foundry.main.dataset.46d0583e-1390-4f2e-b09d-e6553ffc36aa"),
    recover_release_condition_occurrence=Input(rid="ri.foundry.main.dataset.ef3dcd85-f845-4710-923f-ceaeb71155e4")
)
-- getting all person-condition records matching ME/CFS diagnostic code
-- also filters for persons with non-null birth dates and female or male for sex
-- also filters for age >= 18 and filters out unwanted source concepts

SELECT 
    person_reduced_sites.person_id,
    person_reduced_sites.gender_concept_name AS sex,
    CASE WHEN person_reduced_sites.race_concept_name IS NULL THEN 'Unknown'
        WHEN person_reduced_sites.race_concept_name IN ('No matching concept', 'No information', 'Unknown racial group', 'Refuse to answer') THEN 'Unknown'
        WHEN person_reduced_sites.race_concept_name = 'Other' THEN 'Other Race'
        WHEN person_reduced_sites.race_concept_name = 'Multiple race' THEN 'Multiple races'
        WHEN person_reduced_sites.race_concept_name IN ('Korean', 'Filipino', 'Chinese', 'Asian Indian', 'Japanese', 'Vietnamese') THEN 'Asian'
        WHEN person_reduced_sites.race_concept_name IN ('Asian or Pacific islander', 'Other Pacific Islander') THEN 'Native Hawaiian or Other Pacific Islander'
        ELSE person_reduced_sites.race_concept_name END AS race,
    CASE WHEN person_reduced_sites.ethnicity_concept_name IS NULL THEN 'Unknown'
        WHEN person_reduced_sites.ethnicity_concept_name IN ('No matching concept', 'Other', 'No information', 'Other/Unknown') THEN 'Unknown'
        ELSE person_reduced_sites.ethnicity_concept_name END AS ethnicity,
    person_reduced_sites.birth_date,
    recover_release_condition_occurrence.condition_concept_id,
    recover_release_condition_occurrence.condition_concept_name,
    recover_release_condition_occurrence.condition_start_date AS cfs_date,
    recover_release_condition_occurrence.condition_source_concept_id,
    recover_release_condition_occurrence.condition_source_concept_name,
    recover_release_condition_occurrence.data_partner_id
FROM recover_release_condition_occurrence INNER JOIN person_reduced_sites ON recover_release_condition_occurrence.person_id = person_reduced_sites.person_id
WHERE recover_release_condition_occurrence.condition_concept_id IN (432738)
    AND person_reduced_sites.birth_date IS NOT NULL
    AND (person_reduced_sites.gender_concept_name = 'FEMALE' OR person_reduced_sites.gender_concept_name = 'MALE')
    AND FLOOR(DATEDIFF(recover_release_condition_occurrence.condition_start_date, person_reduced_sites.birth_date) / 365.25) >= 18
    AND recover_release_condition_occurrence.condition_source_concept_name IS NOT NULL
    AND recover_release_condition_occurrence.condition_source_concept_id = 37402487
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.224b605e-250d-41e9-992e-f3df019fd992"),
    mecfs_group_one_g9332=Input(rid="ri.foundry.main.dataset.705f7906-86e2-44e1-9200-69ca122ba7e0"),
    recover_release_condition_occurrence=Input(rid="ri.foundry.main.dataset.ef3dcd85-f845-4710-923f-ceaeb71155e4")
)
-- filter to patients with >1 ME/CFS dx_ct
-- we already filtered down to patients with at least 1 G93.32 code
-- here we allow additional ME/CFS codes to be G93.32 or R53.82

SELECT me.person_id,
    FIRST(me.sex) AS sex,
    FIRST(me.race) AS race,
    FIRST(me.ethnicity) AS ethnicity,
    FIRST(me.birth_date) AS birth_date,
    FIRST(me.cfs) AS cfs,
    FIRST(me.cfs_index) AS cfs_index,
    FIRST(me.pre_covid_cfs) AS pre_covid_cfs
FROM mecfs_group_one_g9332 me INNER JOIN recover_release_condition_occurrence co ON me.person_id = co.person_id
WHERE co.condition_source_concept_id IN (37402487, 45573032)
GROUP BY me.person_id
HAVING COUNT(DISTINCT co.condition_start_date) > 1

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.705f7906-86e2-44e1-9200-69ca122ba7e0"),
    mecfs_conditions=Input(rid="ri.foundry.main.dataset.53196dd0-0f4a-4261-9d45-dbe85f559950")
)
-- get first (index) G93.32 ME/CFS dx per patient

SELECT mecfs_conditions.person_id AS person_id,
    FIRST(mecfs_conditions.sex) AS sex,
    FIRST(mecfs_conditions.race) AS race,
    FIRST(mecfs_conditions.ethnicity) AS ethnicity,
    FIRST(mecfs_conditions.birth_date) AS birth_date,
    1 AS cfs,
    MIN(mecfs_conditions.cfs_date) AS cfs_index,
    YEAR(MIN(mecfs_conditions.cfs_date)) < 2020 AS pre_covid_cfs
FROM mecfs_conditions
GROUP BY mecfs_conditions.person_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.394cc4d8-581e-4e5e-89dd-1eb4f524e273"),
    recover_release_manifest=Input(rid="ri.foundry.main.dataset.d0c9e0dc-2dac-4d3f-8391-51664bf249e3")
)
SELECT data_partner_id,
       cdm_name,
       CASE WHEN lower(cdm_name) LIKE 'omop%' THEN 1 ELSE 0 END AS OMOP_site
FROM recover_release_manifest

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.ee300e81-4e7e-46ce-a18e-4dc5d0316e65"),
    person_reduced_sites=Input(rid="ri.foundry.main.dataset.46d0583e-1390-4f2e-b09d-e6553ffc36aa"),
    recover_release_condition_occurrence=Input(rid="ri.foundry.main.dataset.ef3dcd85-f845-4710-923f-ceaeb71155e4")
)
-- getting all person-condition records matching PASC diagnostic codes
-- also filters for persons with non-null birth dates and female or male for sex
-- also filters for age >= 18 and filters out unwanted source concepts

SELECT 
    person_reduced_sites.person_id,
    person_reduced_sites.gender_concept_name AS sex,
    CASE WHEN person_reduced_sites.race_concept_name IS NULL THEN 'Unknown'
        WHEN person_reduced_sites.race_concept_name IN ('No matching concept', 'No information', 'Unknown racial group', 'Refuse to answer') THEN 'Unknown'
        WHEN person_reduced_sites.race_concept_name = 'Other' THEN 'Other Race'
        WHEN person_reduced_sites.race_concept_name = 'Multiple race' THEN 'Multiple races'
        WHEN person_reduced_sites.race_concept_name IN ('Korean', 'Filipino', 'Chinese', 'Asian Indian', 'Japanese', 'Vietnamese') THEN 'Asian'
        WHEN person_reduced_sites.race_concept_name IN ('Asian or Pacific islander', 'Other Pacific Islander') THEN 'Native Hawaiian or Other Pacific Islander'
        ELSE person_reduced_sites.race_concept_name END AS race,
    CASE WHEN person_reduced_sites.ethnicity_concept_name IS NULL THEN 'Unknown'
        WHEN person_reduced_sites.ethnicity_concept_name IN ('No matching concept', 'Other', 'No information', 'Other/Unknown') THEN 'Unknown'
        ELSE person_reduced_sites.ethnicity_concept_name END AS ethnicity,
    person_reduced_sites.birth_date,
    recover_release_condition_occurrence.condition_concept_id,
    recover_release_condition_occurrence.condition_concept_name,
    recover_release_condition_occurrence.condition_start_date AS pasc_date,
    recover_release_condition_occurrence.condition_source_concept_id,
    recover_release_condition_occurrence.condition_source_concept_name,
    recover_release_condition_occurrence.data_partner_id
FROM recover_release_condition_occurrence INNER JOIN person_reduced_sites ON recover_release_condition_occurrence.person_id = person_reduced_sites.person_id
WHERE recover_release_condition_occurrence.condition_concept_id = 705076
    AND recover_release_condition_occurrence.condition_start_date >= '2020-02-01'
    AND person_reduced_sites.birth_date IS NOT NULL
    AND (person_reduced_sites.gender_concept_name = 'FEMALE' OR person_reduced_sites.gender_concept_name = 'MALE')
    AND FLOOR(DATEDIFF(recover_release_condition_occurrence.condition_start_date, person_reduced_sites.birth_date) / 365.25) >= 18
    AND recover_release_condition_occurrence.condition_source_concept_id IN (766503, 710706)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.18987526-547b-45a3-acf7-21aaf51e5991"),
    pasc_conditions=Input(rid="ri.foundry.main.dataset.ee300e81-4e7e-46ce-a18e-4dc5d0316e65")
)
-- filter to patients with >1 PASC dx
-- get first (index) PASC dx per patient

WITH freq AS (
SELECT person_id, COUNT(DISTINCT pasc_date) AS dx_ct
FROM pasc_conditions
GROUP BY person_id
)

SELECT pasc_conditions.person_id AS person_id,
    FIRST(pasc_conditions.sex) AS sex,
    FIRST(pasc_conditions.race) AS race,
    FIRST(pasc_conditions.ethnicity) AS ethnicity,
    FIRST(pasc_conditions.birth_date) AS birth_date,
    1 AS pasc,
    MIN(pasc_conditions.pasc_date) AS pasc_index,
    FALSE AS pre_covid_pasc
FROM pasc_conditions INNER JOIN freq ON pasc_conditions.person_id = freq.person_id
WHERE freq.dx_ct > 1
GROUP BY pasc_conditions.person_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.099b3a9d-a120-473a-a2c1-6a3b7ce14413"),
    full_cohort_w_ml_vars=Input(rid="ri.foundry.main.dataset.5aa7b269-ea4c-4978-9cf5-5dbf92373cca")
)
SELECT person_id, cohort_label, index_date
FROM full_cohort_w_ml_vars
WHERE cohort_label <> 'CTRL'

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.53676f49-e34c-463f-9758-f78fc35ebdf1"),
    person_reduced_sites=Input(rid="ri.foundry.main.dataset.46d0583e-1390-4f2e-b09d-e6553ffc36aa"),
    recover_release_condition_occurrence=Input(rid="ri.foundry.main.dataset.ef3dcd85-f845-4710-923f-ceaeb71155e4")
)
-- getting all patients R53.82
-- also filters for persons with non-null birth dates and female or male for sex
-- also filters for age >= 18

SELECT 
    person_reduced_sites.person_id,
    FIRST(person_reduced_sites.gender_concept_name) AS sex,
    CASE WHEN FIRST(person_reduced_sites.race_concept_name) IS NULL THEN 'Unknown'
        WHEN FIRST(person_reduced_sites.race_concept_name) IN ('No matching concept', 'No information', 'Unknown racial group', 'Refuse to answer') THEN 'Unknown'
        WHEN FIRST(person_reduced_sites.race_concept_name) = 'Other' THEN 'Other Race'
        WHEN FIRST(person_reduced_sites.race_concept_name) = 'Multiple race' THEN 'Multiple races'
        WHEN FIRST(person_reduced_sites.race_concept_name) IN ('Korean', 'Filipino', 'Chinese', 'Asian Indian', 'Japanese', 'Vietnamese') THEN 'Asian'
        WHEN FIRST(person_reduced_sites.race_concept_name) IN ('Asian or Pacific islander', 'Other Pacific Islander') THEN 'Native Hawaiian or Other Pacific Islander'
        ELSE FIRST(person_reduced_sites.race_concept_name) END AS race,
    CASE WHEN FIRST(person_reduced_sites.ethnicity_concept_name) IS NULL THEN 'Unknown'
        WHEN FIRST(person_reduced_sites.ethnicity_concept_name) IN ('No matching concept', 'Other', 'No information', 'Other/Unknown') THEN 'Unknown'
        ELSE FIRST(person_reduced_sites.ethnicity_concept_name) END AS ethnicity,
    FIRST(person_reduced_sites.birth_date) AS birth_date,
    0 AS cfs,
    cast(NULL AS date) AS cfs_index,
    cast(NULL AS boolean) AS pre_covid_cfs,
    0 AS pasc,
    cast(NULL AS date) AS pasc_index,
    cast(NULL AS boolean) AS pre_covid_pasc,
    'R5382' AS cohort_label,
    MIN(recover_release_condition_occurrence.condition_start_date) AS index_date
FROM recover_release_condition_occurrence INNER JOIN person_reduced_sites ON recover_release_condition_occurrence.person_id = person_reduced_sites.person_id
WHERE recover_release_condition_occurrence.condition_concept_id IN (432738)
    AND person_reduced_sites.birth_date IS NOT NULL
    AND (person_reduced_sites.gender_concept_name = 'FEMALE' OR person_reduced_sites.gender_concept_name = 'MALE')
    AND FLOOR(DATEDIFF(recover_release_condition_occurrence.condition_start_date, person_reduced_sites.birth_date) / 365.25) >= 18
    AND recover_release_condition_occurrence.condition_source_concept_name IS NOT NULL
    AND recover_release_condition_occurrence.condition_source_concept_id = 45573032
GROUP BY person_reduced_sites.person_id
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.75ab2daa-3cbf-416f-a4c2-3578f14c14fd"),
    recover_release_condition_occurrence=Input(rid="ri.foundry.main.dataset.ef3dcd85-f845-4710-923f-ceaeb71155e4")
)
-- get condition_occurrence table and add a random value to each row
WITH add_rand AS (
SELECT 
    person_id,
    condition_start_date,
    RAND(0) AS rand_val
FROM recover_release_condition_occurrence
)

-- select a random condition occurrence per person
SELECT a.person_id, a.condition_start_date
FROM add_rand a
INNER JOIN (
    SELECT person_id, MAX(rand_val) AS rand_val
    FROM add_rand
    GROUP BY person_id
) b ON a.person_id = b.person_id AND a.rand_val = b.rand_val

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.cee3a4d7-aca1-4f63-96ff-e02e48a0da6f"),
    conditions_replace_I10_prefix=Input(rid="ri.foundry.main.dataset.3f85a04e-5c07-49d2-8d5a-6f13786f96cc")
)
-- Summarize ICD10 coverage by site
-- Defined as (# ICD Condition Occurrences / # Condition Occurrences) per site

SELECT DISTINCT c.*
FROM (
    SELECT b.*, 
           round(b.icd10_cond_total_org/b.cond_total,3) as site_icd_pct_cvg_org, 
           round(b.icd10_cond_total/b.cond_total,3) as site_icd_pct_cvg
    FROM (
        SELECT a.data_partner_id, 
               sum(a.icd_flag_org) as icd10_cond_total_org, 
               sum(a.icd_flag) as icd10_cond_total, 
               count(a.person_id) as cond_total
        FROM (
            SELECT *,
                CASE WHEN length(replace(regexp_extract(replace(condition_source_value_cleaned,'ICD10',''), '([A-Z][0-9]+[.]*[0-9 ]+)'),'.',''))>0 
                     THEN 1 
                     ELSE 0 
                END as icd_flag_org,
                CASE WHEN length(replace(regexp_extract(replace(condition_source_value_cleaned,'ICD10',''), '([A-Z][0-9][0-9A-Z][.]{0,1}[0-9A-Z]{0,4})'),'.',''))>0 
                     THEN 1 
                     ELSE 0 
                END as icd_flag
            FROM conditions_replace_I10_prefix
        ) a
        GROUP BY data_partner_id
    ) b
) c
ORDER BY c.data_partner_id ASC

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.486432c3-2aad-4189-83d7-e686e9c5d67e"),
    omop_sites=Input(rid="ri.foundry.main.dataset.394cc4d8-581e-4e5e-89dd-1eb4f524e273"),
    site_icd_coverage=Input(rid="ri.foundry.main.dataset.cee3a4d7-aca1-4f63-96ff-e02e48a0da6f")
)
SELECT a.*, 
       b.OMOP_site, 
       CASE WHEN a.site_icd_pct_cvg_org > 0.8 THEN 1 ELSE 0 END as site_icd_gt_80pct_org,
       CASE WHEN a.site_icd_pct_cvg > 0.8 THEN 1 ELSE 0 END as site_icd_gt_80pct
FROM site_icd_coverage as a
LEFT JOIN omop_sites as b
ON a.data_partner_id = b.data_partner_id

