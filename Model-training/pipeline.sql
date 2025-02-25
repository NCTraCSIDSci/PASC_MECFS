-- Authors: Abhishek Bhatia, Tomas McIntee, John Powers
-- © 2025, The University of North Carolina at Chapel Hill. Permission is granted to use in accordance with the MIT license.
-- The code is licensed under the MIT license.


@transform_pandas(
    Output(rid="ri.foundry.main.dataset.72d0c73f-b1f5-4625-92cf-86b9497e4faf"),
    combined_training=Input(rid="ri.foundry.main.dataset.ddde1713-6672-4174-affd-b187a6e78861"),
    set_window_dates=Input(rid="ri.foundry.main.dataset.2f6de052-a84e-491d-9329-6ac7d2dddd10")
)
SELECT set_window_dates.*
FROM set_window_dates FULL OUTER JOIN combined_training ON set_window_dates.person_id = combined_training.person_id
WHERE combined_training.person_id IS NULL

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.678bd91e-e79d-49a2-8701-4cb6b552d167"),
    Reinfection_fact=Input(rid="ri.foundry.main.dataset.29858ba3-b095-4e59-a72b-c3d2a0de16ce"),
    cohort_and_idx=Input(rid="ri.foundry.main.dataset.f195a364-30d1-490f-b70a-f64da43479a1"),
    go_button=Input(rid="ri.foundry.main.dataset.6a52c628-8100-4406-bb3a-14c8a6cc2257")
)
--gather everyone's first infection and any qualifying reinfections and put them in a table to black out data from those windows. 
--infections after the first that do not qualify for inclusion in our official reinfection table do not count here either (e.g., random U07.1s)

SELECT c.person_id, r.reinfection_date AS infection_date, 
    r.reinfection_index AS infection_count, 
    date_add(r.reinfection_date,-7) AS blackout_begin, 
    date_add(r.reinfection_date,28) AS blackout_end
FROM cohort_and_idx c JOIN Reinfection_fact r ON c.person_id = r.person_id
UNION
SELECT c.person_id, array_min(array(c.pos_test_idx, c.u07_any_idx, c.pax_or_rem_idx)) AS infection_date,
    0 AS infection_count, 
    date_add(array_min(array(c.pos_test_idx, c.u07_any_idx, c.pax_or_rem_idx)),-7) AS blackout_begin, 
    date_add(array_min(array(c.pos_test_idx, c.u07_any_idx, c.pax_or_rem_idx)),28) AS blackout_end
from cohort_and_idx c
where c.pos_test_idx IS NOT NULL
    OR c.u07_any_idx IS NOT NULL 
    OR c.pax_or_rem_idx IS NOT NULL

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f80fa8d4-6417-4a2e-a78d-f7cf3b2fffcf"),
    cfs_shapleys=Input(rid="ri.foundry.main.dataset.d398f855-59f7-415c-b426-d55ca1affcd7"),
    pasc_shapleys=Input(rid="ri.foundry.main.dataset.14274e39-6303-4fe3-a40b-771436859932"),
    pvc_shapleys=Input(rid="ri.foundry.main.dataset.f86845e3-dc9f-480f-91fa-6861acbb2085"),
    stored_importance=Input(rid="ri.foundry.main.dataset.a1fa9b1d-9fca-4600-ae5d-afb71f75da03")
)
SELECT stored_importance.*,
    pasc_shapleys.pasc_mean_shapley,
    pasc_shapleys.pasc_median_shapley,
    pasc_shapleys.pasc_sd_shapley,
    cfs_shapleys.cfs_mean_shapley,
    cfs_shapleys.cfs_median_shapley,
    cfs_shapleys.cfs_sd_shapley,
    pvc_shapleys.pvc_mean_shapley,
    pvc_shapleys.pvc_median_shapley,
    pvc_shapleys.pvc_sd_shapley
FROM stored_importance INNER JOIN pasc_shapleys ON stored_importance.condition_concept_name = pasc_shapleys.condition_concept_name
    INNER JOIN cfs_shapleys ON stored_importance.condition_concept_name = cfs_shapleys.condition_concept_name
    INNER JOIN pvc_shapleys ON stored_importance.condition_concept_name = pvc_shapleys.condition_concept_name
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.ddde1713-6672-4174-affd-b187a6e78861"),
    matched_CFS=Input(rid="ri.foundry.main.dataset.8e524689-b720-4f54-a13b-c0ab7275e0ad"),
    matched_PASC=Input(rid="ri.foundry.main.dataset.9f95b1fc-c284-4492-8e78-0c08e17b1b73")
)
SELECT * FROM matched_CFS
UNION
SELECT * FROM matched_PASC

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.8f8e454b-4f30-4b8a-9dc1-25f43c67891e"),
    rolled_conditions_cfs=Input(rid="ri.foundry.main.dataset.7d0f2f21-8355-402a-a88d-afe3b3d77a7c")
)
SELECT DISTINCT rolled_conditions_cfs.person_id,
    rolled_conditions_cfs.condition_concept_id,
    CASE WHEN MAX(rolled_conditions_cfs.condition_start_date) = MIN(rolled_conditions_cfs.condition_start_date) AND
        MAX(rolled_conditions_cfs.condition_end_date) = MIN(rolled_conditions_cfs.condition_end_date) THEN 1
    ELSE 1 --Change to 2 for ternary conditions
    END AS one_or_many_conds
FROM rolled_conditions_cfs
GROUP BY rolled_conditions_cfs.person_id, rolled_conditions_cfs.condition_concept_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.88ee2cef-070a-4c23-8a9b-e44f0b9af91d"),
    rolled_conditions_pasc=Input(rid="ri.foundry.main.dataset.330d9332-009f-4ae4-ac33-d0d6757488f9"),
    rolled_conditions_pvc=Input(rid="ri.foundry.main.dataset.f4a70e78-cc79-457b-98df-7256d60b09bc")
)
SELECT DISTINCT rolled_conditions_pvc.person_id,
    rolled_conditions_pvc.condition_concept_id,
    CASE WHEN MAX(rolled_conditions_pvc.condition_start_date) = MIN(rolled_conditions_pvc.condition_start_date) AND
        MAX(rolled_conditions_pvc.condition_end_date) = MIN(rolled_conditions_pvc.condition_end_date) THEN 1
    ELSE 1 --Change to 2 for ternary conditions
    END AS one_or_many_conds
FROM rolled_conditions_pvc
GROUP BY rolled_conditions_pvc.person_id, rolled_conditions_pvc.condition_concept_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.51de9c2d-e32d-4f83-81df-cd415988987d"),
    rolled_conditions_pasc=Input(rid="ri.foundry.main.dataset.330d9332-009f-4ae4-ac33-d0d6757488f9")
)
SELECT DISTINCT rolled_conditions_pasc.person_id,
    rolled_conditions_pasc.condition_concept_id,
    CASE WHEN MAX(rolled_conditions_pasc.condition_start_date) = MIN(rolled_conditions_pasc.condition_start_date) AND
        MAX(rolled_conditions_pasc.condition_end_date) = MIN(rolled_conditions_pasc.condition_end_date) THEN 1
    ELSE 1 --Change to 2 for ternary conditions
    END AS one_or_many_conds
FROM rolled_conditions_pasc
GROUP BY rolled_conditions_pasc.person_id, rolled_conditions_pasc.condition_concept_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.6a52c628-8100-4406-bb3a-14c8a6cc2257"),
    Full_cohort=Input(rid="ri.foundry.main.dataset.5aa7b269-ea4c-4978-9cf5-5dbf92373cca")
)
SELECT *
FROM Full_cohort

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.80a77caf-1c2f-4713-832c-2b75f3ebb97f"),
    pregnancy_concepts=Input(rid="ri.foundry.main.dataset.702060b7-e583-4a5d-9c1d-2bdc49f8df06"),
    rolled_lookup=Input(rid="ri.foundry.main.dataset.3b9a15dc-1b06-4773-8a6b-d25ef2bf3b03")
)
SELECT DISTINCT nvl(pregnancy_concepts.descendant_concept_id,rolled_lookup.descendant_concept_id) AS descendant_concept_id,
    nvl(pregnancy_concepts.des_concept_name,rolled_lookup.des_concept_name) AS des_concept_name,
    nvl(pregnancy_concepts.ancestor_concept_id,rolled_lookup.ancestor_concept_id) AS ancestor_concept_id,
    nvl(pregnancy_concepts.anc_concept_name,rolled_lookup.anc_concept_name) AS anc_concept_name
FROM rolled_lookup FULL OUTER JOIN pregnancy_concepts ON rolled_lookup.descendant_concept_id = pregnancy_concepts.descendant_concept_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.702060b7-e583-4a5d-9c1d-2bdc49f8df06"),
    go_button=Input(rid="ri.foundry.main.dataset.6a52c628-8100-4406-bb3a-14c8a6cc2257"),
    recover_release_concept=Input(rid="ri.foundry.main.dataset.6fd878f0-895f-48fa-ad01-999883522a54"),
    recover_release_concept_ancestor=Input(rid="ri.foundry.main.dataset.8c6328cc-e9d2-4448-a66c-c0d782fbd87d")
)
SELECT ca.ancestor_concept_id, 
    rr1.concept_name as anc_concept_name, 
    ca.descendant_concept_id, 
    rr2.concept_name as des_concept_name, 
    ca.min_levels_of_separation, 
    ca.max_levels_of_separation
FROM recover_release_concept_ancestor ca JOIN recover_release_concept rr1 ON ca.ancestor_concept_id = rr1.concept_id
    JOIN recover_release_concept rr2 ON ca.descendant_concept_id = rr2.concept_id
WHERE rr1.vocabulary_id = 'SNOMED' 
    AND rr1.standard_concept = 'S' 
    AND rr1.domain_id = 'Condition'
    AND (rr1.concept_name = 'Finding related to pregnancy' OR rr1.concept_name = 'Delivery finding')

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.2f6de052-a84e-491d-9329-6ac7d2dddd10"),
    Full_cohort=Input(rid="ri.foundry.main.dataset.5aa7b269-ea4c-4978-9cf5-5dbf92373cca"),
    go_button=Input(rid="ri.foundry.main.dataset.6a52c628-8100-4406-bb3a-14c8a6cc2257")
)
SELECT Full_cohort.*,
    Full_cohort.index_date - 70 AS window_start,
    Full_cohort.index_date + 30 AS window_end
    --Window will be adjusted for PASC and CFS training set in order to accommodate the PASC_CFS dual cohort.
FROM Full_cohort

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c0393753-2713-4099-ad73-79c283964b7d"),
    conditions_blacked_out=Input(rid="ri.foundry.main.dataset.3ef4e2e9-6d40-4e6a-804f-2d31faf7aafd"),
    matched_CFS=Input(rid="ri.foundry.main.dataset.8e524689-b720-4f54-a13b-c0ab7275e0ad")
)
SELECT matched_CFS.*,
    conditions_blacked_out.condition_concept_id,
    conditions_blacked_out.condition_concept_name,
    nvl(conditions_blacked_out.condition_end_date, conditions_blacked_out.condition_start_date) as condition_end_date,
    conditions_blacked_out.condition_start_date
FROM matched_CFS INNER JOIN conditions_blacked_out ON matched_CFS.person_id = conditions_blacked_out.person_id
WHERE (conditions_blacked_out.condition_start_date BETWEEN matched_CFS.window_start AND matched_CFS.window_end)
    OR (conditions_blacked_out.condition_end_date BETWEEN matched_CFS.window_start AND matched_CFS.window_end)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.d24eba68-b481-464a-a2f9-54469f8405e2"),
    conditions_blacked_out=Input(rid="ri.foundry.main.dataset.3ef4e2e9-6d40-4e6a-804f-2d31faf7aafd"),
    matched_PASC=Input(rid="ri.foundry.main.dataset.9f95b1fc-c284-4492-8e78-0c08e17b1b73")
)
SELECT matched_PASC.*,
    conditions_blacked_out.condition_concept_id,
    conditions_blacked_out.condition_concept_name,
    nvl(conditions_blacked_out.condition_end_date,conditions_blacked_out.condition_start_date) AS condition_end_date,
    conditions_blacked_out.condition_start_date
FROM matched_PASC INNER JOIN conditions_blacked_out ON matched_PASC.person_id = conditions_blacked_out.person_id
WHERE (conditions_blacked_out.condition_start_date BETWEEN matched_PASC.window_start AND matched_PASC.window_end)
    OR (conditions_blacked_out.condition_end_date BETWEEN matched_PASC.window_start AND matched_PASC.window_end)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.2831cf63-aa53-4acf-b639-b27cfeb7ae65"),
    conditions_blacked_out=Input(rid="ri.foundry.main.dataset.3ef4e2e9-6d40-4e6a-804f-2d31faf7aafd"),
    matched_pvc=Input(rid="ri.foundry.main.dataset.7cb1ce32-b984-407b-9064-0ac46535d157")
)
SELECT matched_pvc.*,
    conditions_blacked_out.condition_concept_id,
    conditions_blacked_out.condition_concept_name,
    nvl(conditions_blacked_out.condition_end_date, conditions_blacked_out.condition_start_date) as condition_end_date,
    conditions_blacked_out.condition_start_date
FROM matched_pvc INNER JOIN conditions_blacked_out ON matched_pvc.person_id = conditions_blacked_out.person_id
WHERE (conditions_blacked_out.condition_start_date BETWEEN matched_pvc.window_start AND matched_pvc.window_end)
    OR (conditions_blacked_out.condition_end_date BETWEEN matched_pvc.window_start AND matched_pvc.window_end)

