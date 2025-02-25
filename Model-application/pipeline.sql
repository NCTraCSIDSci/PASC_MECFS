-- Authors: Abhishek Bhatia, Tomas McIntee, John Powers
-- © 2025, The University of North Carolina at Chapel Hill. Permission is granted to use in accordance with the MIT license.
-- The code is licensed under the MIT license.


@transform_pandas(
    Output(rid="ri.foundry.main.dataset.6008ea8f-40c7-47a8-ab22-4cd0e518c22e"),
    All_test=Input(rid="ri.foundry.main.dataset.72d0c73f-b1f5-4625-92cf-86b9497e4faf"),
    Conditions_blacked_out=Input(rid="ri.foundry.main.dataset.3ef4e2e9-6d40-4e6a-804f-2d31faf7aafd"),
    go_button=Input(rid="ri.foundry.main.dataset.d1a7fac6-74e1-4a94-b8f5-160cd6e38fba")
)
SELECT All_test.person_id,
    Conditions_blacked_out.condition_concept_id,
    Conditions_blacked_out.condition_start_date,
    Conditions_blacked_out.condition_end_date,
    All_test.window_start,
    All_test.window_end
FROM Conditions_blacked_out INNER JOIN All_test ON Conditions_blacked_out.person_id = All_test.person_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.49a2ada3-8b26-4d6c-a612-57014b0aba3d"),
    selected_conditions=Input(rid="ri.foundry.main.dataset.86306689-eec1-425b-9875-688ddeb81ddf")
)
SELECT *
FROM selected_conditions
WHERE (condition_start_date BETWEEN window_start AND window_end)
    OR (condition_end_date BETWEEN window_start AND window_end)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c3ffb7bf-1b02-45cb-afe9-414c6d361637"),
    date_filtered_conditions=Input(rid="ri.foundry.main.dataset.49a2ada3-8b26-4d6c-a612-57014b0aba3d")
)
SELECT DISTINCT date_filtered_conditions.person_id,
    date_filtered_conditions.condition_concept_id,
    CASE WHEN MAX(date_filtered_conditions.condition_start_date) = MIN(date_filtered_conditions.condition_start_date) AND
        MAX(date_filtered_conditions.condition_end_date) = MIN(date_filtered_conditions.condition_end_date) THEN 1
        WHEN MIN(date_filtered_conditions.condition_start_date) + 90 > MAX(date_filtered_conditions.condition_end_date) THEN 1
    ELSE 1--Ternary will be 2
    END AS one_or_many_conds
FROM date_filtered_conditions
GROUP BY date_filtered_conditions.person_id, date_filtered_conditions.condition_concept_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.728fc9a6-2a44-4f10-81b4-e70084cd4c58"),
    selected_conditions_pvc=Input(rid="ri.foundry.main.dataset.0bf6fa5c-5f73-4ccb-a128-014f90e83f8b")
)
SELECT DISTINCT *
FROM selected_conditions_pvc
WHERE (condition_start_date BETWEEN window_start AND window_end)
    OR (condition_end_date BETWEEN window_start AND window_end)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.816b3fc0-57ac-4f11-b3af-8b0e1264fd7d"),
    datefilter_pvc=Input(rid="ri.foundry.main.dataset.728fc9a6-2a44-4f10-81b4-e70084cd4c58")
)
SELECT DISTINCT datefilter_pvc.person_id,
    datefilter_pvc.condition_concept_id,
    CASE WHEN MAX(datefilter_pvc.condition_start_date) = MIN(datefilter_pvc.condition_start_date) AND
        MAX(datefilter_pvc.condition_end_date) = MIN(datefilter_pvc.condition_end_date) THEN 1
        WHEN MIN(datefilter_pvc.condition_start_date) + 90 > MAX(datefilter_pvc.condition_end_date) THEN 1
    ELSE 1--Ternary will be 2
    END AS one_or_many_conds
FROM datefilter_pvc
GROUP BY datefilter_pvc.person_id, datefilter_pvc.condition_concept_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.5d36ccd6-4154-4e9f-83e8-e8d9e064704f"),
    All_test=Input(rid="ri.foundry.main.dataset.72d0c73f-b1f5-4625-92cf-86b9497e4faf"),
    go_button=Input(rid="ri.foundry.main.dataset.d1a7fac6-74e1-4a94-b8f5-160cd6e38fba"),
    model_scores_cfs=Input(rid="ri.foundry.main.dataset.8ee62ba2-93df-4c6d-8ca4-24ced51beebe"),
    model_scores_pasc=Input(rid="ri.foundry.main.dataset.77352559-2c91-4621-812f-95e04e964ce0")
)
SELECT All_test.*,
    model_scores_cfs.model_score AS CFS_score,
    model_scores_pasc.model_score AS PASC_score
FROM model_scores_cfs INNER JOIN model_scores_pasc ON model_scores_cfs.person_id = model_scores_pasc.person_id
    INNER JOIN All_test ON model_scores_cfs.person_id = All_test.person_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.d1a7fac6-74e1-4a94-b8f5-160cd6e38fba"),
    All_test=Input(rid="ri.foundry.main.dataset.72d0c73f-b1f5-4625-92cf-86b9497e4faf")
)
SELECT *
FROM All_test

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.76b937dc-1ed8-4bc2-9c24-62eba04323e5"),
    base_subset=Input(rid="ri.foundry.main.dataset.048712c7-d1a1-4992-957b-9e98c4647703"),
    processed_for_models=Input(rid="ri.foundry.main.dataset.02fa90ae-9d83-488b-8391-964b1d89322b")
)
SELECT processed_for_models.*
FROM processed_for_models INNER JOIN base_subset ON processed_for_models.person_id = base_subset.person_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.86306689-eec1-425b-9875-688ddeb81ddf"),
    Selected_features=Input(rid="ri.foundry.main.dataset.76c1f9c9-60a4-487c-9ee4-5c10262800b3"),
    rolled_conditions=Input(rid="ri.foundry.main.dataset.50a25c96-de02-434a-946a-688c88ba5eda")
)
SELECT Selected_features.condition_concept_id,
    rolled_conditions.person_id,
    rolled_conditions.condition_start_date,
    rolled_conditions.condition_end_date,
    rolled_conditions.window_start,
    rolled_conditions.window_end
FROM rolled_conditions INNER JOIN Selected_features ON rolled_conditions.condition_concept_id = Selected_features.condition_concept_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.0bf6fa5c-5f73-4ccb-a128-014f90e83f8b"),
    Selected_features=Input(rid="ri.foundry.main.dataset.76c1f9c9-60a4-487c-9ee4-5c10262800b3"),
    rolled_conditions=Input(rid="ri.foundry.main.dataset.50a25c96-de02-434a-946a-688c88ba5eda")
)
SELECT Selected_features.condition_concept_id,
    rolled_conditions.person_id,
    rolled_conditions.condition_start_date,
    rolled_conditions.condition_end_date,
    rolled_conditions.window_start,
    rolled_conditions.window_end
FROM rolled_conditions INNER JOIN Selected_features ON rolled_conditions.condition_concept_id = Selected_features.condition_concept_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.971f17b1-c01d-4912-8e5e-c4e431b5d385"),
    base_subset=Input(rid="ri.foundry.main.dataset.048712c7-d1a1-4992-957b-9e98c4647703"),
    processed_pvc=Input(rid="ri.foundry.main.dataset.424a338b-0144-41d6-a80b-1bdb7b7818e5")
)
SELECT processed_pvc.*
FROM processed_pvc INNER JOIN base_subset ON processed_pvc.person_id = base_subset.person_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.a6a1fee4-b2de-4beb-8613-d51c5fbefe49"),
    base_subset=Input(rid="ri.foundry.main.dataset.048712c7-d1a1-4992-957b-9e98c4647703"),
    model_scores_cfs_tiny=Input(rid="ri.foundry.main.dataset.172dd54c-ecee-4a6f-b138-89ea228b2b53"),
    model_scores_pasc_tiny=Input(rid="ri.foundry.main.dataset.22d02628-8b0a-4f69-9fc0-ca99a790fca6"),
    model_scores_pvc_small=Input(rid="ri.foundry.main.dataset.11c90969-5002-4908-ba7d-264d5da8d08f")
)
SELECT base_subset.*,
    model_scores_cfs_tiny.model_score AS CFS_score,
    model_scores_pasc_tiny.model_score AS PASC_score,
    model_scores_pvc_small.model_score AS PVC_score,
    model_scores_cfs_tiny.model_score > 0.5 AS CP_MECFS,
    model_scores_pasc_tiny.model_score > 0.5 AS CP_PASC,
    model_scores_pvc_small.model_score > 0.75 AS CP_MECFS_NOT_PASC
    
FROM model_scores_cfs_tiny INNER JOIN model_scores_pasc_tiny ON model_scores_cfs_tiny.person_id = model_scores_pasc_tiny.person_id
    INNER JOIN base_subset ON model_scores_cfs_tiny.person_id = base_subset.person_id
    INNER JOIN model_scores_pvc_small ON model_scores_pvc_small.person_id = base_subset.person_id

