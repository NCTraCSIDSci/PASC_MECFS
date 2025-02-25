-- Authors: Abhishek Bhatia, Tomas McIntee, John Powers
-- © 2025, The University of North Carolina at Chapel Hill. Permission is granted to use in accordance with the MIT license.
-- The code is licensed under the MIT license.


@transform_pandas(
    Output(rid="ri.foundry.main.dataset.fdf4daf6-6ae9-40b8-8e87-5e1c98d78796"),
    Full_cohort_w_ml_vars=Input(rid="ri.foundry.main.dataset.5aa7b269-ea4c-4978-9cf5-5dbf92373cca"),
    concept=Input(rid="ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772"),
    date_filtered_conditions_full=Input(rid="ri.foundry.main.dataset.29a84b5b-67dd-45a8-8ad3-c48da2beb9e7")
)
-- SELECT c.concept_name as condition_concept_name, d.person_id, d.condition_start_date,cohort_label
SELECT DISTINCT c.concept_name as condition_concept_name, d.person_id, cohort_label
FROM date_filtered_conditions_full as d
LEFT JOIN concept as c
ON d.condition_concept_id = c.concept_id
LEFT JOIN Full_cohort_w_ml_vars as f
ON f.person_id = d.person_id
WHERE f.cohort_label = 'PASC_CFS' OR f.cohort_label = 'PASC' OR f.cohort_label = 'CFS' 

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.7f63a854-3aa7-4b37-89cb-d86a8002c4e7"),
    Conditions_blacked_out=Input(rid="ri.foundry.main.dataset.3ef4e2e9-6d40-4e6a-804f-2d31faf7aafd"),
    Set_window_dates=Input(rid="ri.foundry.main.dataset.2f6de052-a84e-491d-9329-6ac7d2dddd10")
)
SELECT Set_window_dates.person_id,
    Conditions_blacked_out.condition_concept_id,
    Conditions_blacked_out.condition_start_date,
    Conditions_blacked_out.condition_end_date,
    Set_window_dates.window_start,
    Set_window_dates.window_end
FROM Conditions_blacked_out INNER JOIN Set_window_dates ON Conditions_blacked_out.person_id = Set_window_dates.person_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.29a84b5b-67dd-45a8-8ad3-c48da2beb9e7"),
    selected_conditions=Input(rid="ri.foundry.main.dataset.46fec3a3-db8d-45d5-842e-8c0e0238f44f")
)
SELECT *
FROM selected_conditions
WHERE (condition_start_date BETWEEN window_start AND window_end)
    OR (condition_end_date BETWEEN window_start AND window_end)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.32909008-285f-488a-8b48-5872ce2d7884"),
    Full_test_cohort_scored=Input(rid="ri.foundry.main.dataset.5d36ccd6-4154-4e9f-83e8-e8d9e064704f"),
    concept=Input(rid="ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772"),
    date_filtered_conditions=Input(rid="ri.foundry.main.dataset.49a2ada3-8b26-4d6c-a612-57014b0aba3d")
)
SELECT c.concept_name as condition_concept_name, d.person_id, d.condition_start_date
FROM date_filtered_conditions as d
LEFT JOIN concept as c
ON d.condition_concept_id = c.concept_id
LEFT JOIN Full_test_cohort_scored as f
ON f.person_id = d.person_id
WHERE f.cohort_label = 'CFS'

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.d21aa532-5b69-4965-9b38-858e6445e862"),
    Full_test_cohort_scored=Input(rid="ri.foundry.main.dataset.5d36ccd6-4154-4e9f-83e8-e8d9e064704f"),
    concept=Input(rid="ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772"),
    date_filtered_conditions=Input(rid="ri.foundry.main.dataset.49a2ada3-8b26-4d6c-a612-57014b0aba3d")
)
SELECT 
    c.concept_name as condition_concept_name, 
    d.person_id, 
    CASE WHEN f.CFS_score > 0.5 AND f.PASC_score > 0.5 THEN 'PASC_ME/CFS'
    WHEN f.CFS_score > 0.5 THEN 'ME/CFS'
    WHEN f.PASC_score > 0.5 THEN 'PASC' END AS cohort_label
FROM date_filtered_conditions as d
LEFT JOIN concept as c
ON d.condition_concept_id = c.concept_id
LEFT JOIN Full_test_cohort_scored as f
ON f.person_id = d.person_id
WHERE f.CFS_score > 0.5 OR f.PASC_score > 0.5

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.84caf9d0-befc-4629-b5a9-49743bf12f55"),
    Full_test_cohort_scored=Input(rid="ri.foundry.main.dataset.5d36ccd6-4154-4e9f-83e8-e8d9e064704f"),
    concept=Input(rid="ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772"),
    date_filtered_conditions=Input(rid="ri.foundry.main.dataset.49a2ada3-8b26-4d6c-a612-57014b0aba3d")
)
SELECT c.concept_name as condition_concept_name, d.person_id, d.condition_start_date
FROM date_filtered_conditions as d
LEFT JOIN concept as c
ON d.condition_concept_id = c.concept_id
LEFT JOIN Full_test_cohort_scored as f
ON f.person_id = d.person_id
WHERE f.cohort_label = 'PASC'

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b5c67ad3-778b-4499-81ad-9fc79ef92f93"),
    Full_test_cohort_scored=Input(rid="ri.foundry.main.dataset.5d36ccd6-4154-4e9f-83e8-e8d9e064704f"),
    concept=Input(rid="ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772"),
    date_filtered_conditions=Input(rid="ri.foundry.main.dataset.49a2ada3-8b26-4d6c-a612-57014b0aba3d")
)
SELECT c.concept_name as condition_concept_name, d.person_id, d.condition_start_date
FROM date_filtered_conditions as d
LEFT JOIN concept as c
ON d.condition_concept_id = c.concept_id
LEFT JOIN Full_test_cohort_scored as f
ON f.person_id = d.person_id
WHERE f.cohort_label = 'PASC_CFS'

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.7d89afd1-61a4-4cfb-8baf-fe8ebeda5d3d"),
    Feature_importances=Input(rid="ri.foundry.main.dataset.a59c701f-f7f0-4978-b9ec-4ca4f047f82a"),
    sampling_both=Input(rid="ri.foundry.main.dataset.1beae739-b571-4cd4-bc0f-4ce0cf928c70")
)
SELECT
  sb.condition_concept_name,
  CAST(COUNT(DISTINCT person_id) AS INT) AS pat_count,
  (COUNT(DISTINCT person_id) / 9428) AS pat_pct
FROM
  sampling_both sb INNER JOIN Feature_importances fi ON sb.condition_concept_name = fi.Condition
GROUP BY
  sb.condition_concept_name
ORDER BY
  pat_count DESC

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.5df1c9af-092f-4d85-9043-ff4ad1cc94b1"),
    Full_cohort_w_ml_vars=Input(rid="ri.foundry.main.dataset.5aa7b269-ea4c-4978-9cf5-5dbf92373cca"),
    date_filtered_conditions_full=Input(rid="ri.foundry.main.dataset.29a84b5b-67dd-45a8-8ad3-c48da2beb9e7")
)
-- get in-window dyspnea occurrences for ME/CFS-only and R5382 patients
SELECT DISTINCT 
  d.person_id, 
  d.condition_concept_id,
  d.condition_start_date,
  f.cohort_label
FROM date_filtered_conditions_full as d
  INNER JOIN Full_cohort_w_ml_vars as f
ON f.person_id = d.person_id
WHERE (f.cohort_label = 'CFS' OR f.cohort_label = 'R5382')
  AND d.condition_concept_id = 312437

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.35d3e988-bc37-49a5-9458-74442bc8b328"),
    sampling_both=Input(rid="ri.foundry.main.dataset.1beae739-b571-4cd4-bc0f-4ce0cf928c70")
)
SELECT DISTINCT person_id, cohort_label
FROM sampling_both

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.46fec3a3-db8d-45d5-842e-8c0e0238f44f"),
    Selected_features=Input(rid="ri.foundry.main.dataset.76c1f9c9-60a4-487c-9ee4-5c10262800b3"),
    apply_rollup=Input(rid="ri.foundry.main.dataset.1b98aa72-961f-48ca-a8eb-9945e1635961")
)
SELECT Selected_features.condition_concept_id,
    apply_rollup.person_id,
    apply_rollup.condition_start_date,
    apply_rollup.condition_end_date,
    apply_rollup.window_start,
    apply_rollup.window_end
FROM apply_rollup INNER JOIN Selected_features ON apply_rollup.condition_concept_id = Selected_features.condition_concept_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.81cca64e-8fac-43ca-8d97-ea66e3bf1466"),
    filtered_conditions_cfs=Input(rid="ri.foundry.main.dataset.32909008-285f-488a-8b48-5872ce2d7884")
)
SELECT
  condition_concept_name,
  CAST(COUNT(DISTINCT person_id) AS INT) AS pat_count
FROM
  filtered_conditions_cfs
GROUP BY
  condition_concept_name
HAVING
  COUNT(DISTINCT person_id) / CAST((SELECT COUNT(DISTINCT person_id) FROM filtered_conditions_cfs) AS FLOAT) >= 0.05
ORDER BY
  pat_count DESC

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.21e21123-d244-4084-bff7-857701926e74"),
    filtered_conditions_pasc=Input(rid="ri.foundry.main.dataset.84caf9d0-befc-4629-b5a9-49743bf12f55")
)
SELECT
  condition_concept_name,
  CAST(COUNT(DISTINCT person_id) AS INT) AS pat_count
FROM
  filtered_conditions_pasc
GROUP BY
  condition_concept_name
HAVING
  COUNT(DISTINCT person_id) / CAST((SELECT COUNT(DISTINCT person_id) FROM filtered_conditions_pasc) AS FLOAT) >= 0.05
ORDER BY
  pat_count DESC

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.1345f383-f6ae-4264-a1d7-cc56e5d2ab83"),
    filtered_conditions_pasc_cfs=Input(rid="ri.foundry.main.dataset.b5c67ad3-778b-4499-81ad-9fc79ef92f93")
)
SELECT
  condition_concept_name,
  CAST(COUNT(DISTINCT person_id) AS INT) AS pat_count
FROM
  filtered_conditions_pasc_cfs
GROUP BY
  condition_concept_name
HAVING
  COUNT(DISTINCT person_id) / CAST((SELECT COUNT(DISTINCT person_id) FROM filtered_conditions_pasc_cfs) AS FLOAT) >= 0.05
ORDER BY
  pat_count DESC

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.2c11529a-b8d3-4339-828e-0213b2378f95"),
    sampling_both=Input(rid="ri.foundry.main.dataset.1beae739-b571-4cd4-bc0f-4ce0cf928c70")
)
SELECT
  condition_concept_name,
  CAST(COUNT(DISTINCT person_id) AS INT) AS pat_count
FROM
  sampling_both
GROUP BY
  condition_concept_name
HAVING
  COUNT(DISTINCT person_id) / CAST((SELECT COUNT(DISTINCT person_id) FROM sampling_both) AS FLOAT) >= 0.05
ORDER BY
  pat_count DESC

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.ac70f275-4b39-49a5-80b0-afe146127052"),
    Features_05_threshold=Input(rid="ri.foundry.main.dataset.b6428f8c-c169-483b-8a0e-073bf8312df1"),
    sampling_both=Input(rid="ri.foundry.main.dataset.1beae739-b571-4cd4-bc0f-4ce0cf928c70")
)
SELECT
  sb.condition_concept_name,
  CAST(COUNT(DISTINCT sb.person_id) AS INT) AS pat_count
FROM
  sampling_both sb INNER JOIN Features_05_threshold f ON sb.condition_concept_name = f.condition_concept_name
GROUP BY
  sb.condition_concept_name
ORDER BY
  pat_count DESC

