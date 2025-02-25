# Authors: Abhishek Bhatia, Tomas McIntee, John Powers
# © 2025, The University of North Carolina at Chapel Hill. Permission is granted to use in accordance with the MIT license.
# The code is licensed under the MIT license.


@transform_pandas(
    Output(rid="ri.foundry.main.dataset.d398f855-59f7-415c-b426-d55ca1affcd7"),
    selected_features=Input(rid="ri.foundry.main.dataset.76c1f9c9-60a4-487c-9ee4-5c10262800b3"),
    shap_summary_cfs=Input(rid="ri.foundry.main.dataset.806018de-b2fd-4aaa-81d8-23306f8e4f8e")
)
require(tidyverse)
cfs_shapleys <- function(selected_features, shap_summary_cfs) 
{
    features_sorted <- selected_features %>%
        arrange(condition_concept_id)
    features_vec <- c(features_sorted$condition_concept_name,"age_bin","is_female")
    placeholder <- shap_summary_cfs
    names(placeholder) <- features_vec
    placeholder <- placeholder %>%
        pivot_longer(cols = everything(), names_to = "condition_concept_name") %>%
        group_by(condition_concept_name) %>%
        summarize(cfs_mean_shapley = mean(value),
            cfs_median_shapley = median(value),
            cfs_sd_shapley = sd(value))
    return(placeholder)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.acd2302f-e989-4d58-838e-dd0a9b27b528"),
    combined_feature_importances=Input(rid="ri.foundry.main.dataset.f80fa8d4-6417-4a2e-a78d-f7cf3b2fffcf")
)
require(tidyverse)
feature_importance <- function(combined_feature_importances) 
{
    composite_scores <- combined_feature_importances %>%
        mutate(cfs_importance = importance_cfs*(-1+2*(cfs_mean_shapley > 0)),
            pasc_importance = importance_pasc*(-1+2*(pasc_mean_shapley > 0)),
            composite_cfs_importance = cfs_importance/sd(cfs_importance) +
                cfs_mean_shapley/sd(cfs_mean_shapley)+
                cfs_median_shapley/sd(cfs_median_shapley)+
                cfs_sd_shapley/sd(cfs_sd_shapley)*(-1+2*(cfs_mean_shapley > 0)),
            composite_pasc_importance = (importance_pasc)/sd(importance_pasc)*(-1+2*(pasc_mean_shapley > 0))+
                pasc_mean_shapley/sd(pasc_mean_shapley)+
                pasc_median_shapley/sd(pasc_median_shapley)+
                pasc_sd_shapley/sd(pasc_sd_shapley)*(-1+2*(pasc_mean_shapley > 0)),
            indicator = case_when(composite_cfs_importance > 0 & composite_pasc_importance > 0 ~ "positive",
                composite_cfs_importance > 0 & composite_pasc_importance <= 0 ~ "cfs_indicator",
                composite_cfs_importance <= 0 & composite_pasc_importance > 0 ~ "pasc_indicator",
                composite_cfs_importance <= 0 & composite_pasc_importance <= 0 ~ "negative"),
            diff_mean_shapley = cfs_mean_shapley - pasc_mean_shapley) %>%
        select(condition_concept_name,
            indicator,
            composite_cfs_importance,
            composite_pasc_importance,
            cfs_mean_shapley,
            pasc_mean_shapley,
            diff_mean_shapley,
            cfs_importance,
            pasc_importance)
    return(composite_scores)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.8e524689-b720-4f54-a13b-c0ab7275e0ad"),
    CFS_train=Input(rid="ri.foundry.main.dataset.6cf47362-6f83-4aec-96e9-b72095a38154"),
    CTRL_train=Input(rid="ri.foundry.main.dataset.1a5c5bce-6d55-41e6-b1f8-ddeabace3bf6")
)
require(tidyverse)
require(MatchIt)

matched_CFS <- function(CFS_train, CTRL_train) 
{
    CFS_train <- CFS_train %>% 
        filter(index_date == cfs_index)
    training_set <- CTRL_train %>%
        rbind(CFS_train) %>%
        #filter(pre_covid == TRUE) %>% #This is the one line that controls whether or not CFS is restricted to pre_covid.
        mutate(sick = (cohort_label == "CFS" | cohort_label == "PASC_CFS"))
    #Set seed for replicability
    set.seed(37)
    #Actual training and testing size probably around 75% with exact random matching in this case.
    #return(training_set)
    #Run matching. Exact matching on already-coarse demographic variables is in this case is very easy.
    matchy_train <- matchit(formula = sick ~ 1,
        data = training_set,
        exact = ~ ethnicity + race + is_female + age_bin + pre_covid + cci_score,
        replace = FALSE,
        m.order = "random")
        print("matching complete")
    matched_train <- get_matches(matchy_train,data = training_set)
    return(matched_train)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.9f95b1fc-c284-4492-8e78-0c08e17b1b73"),
    CTRL_train=Input(rid="ri.foundry.main.dataset.1a5c5bce-6d55-41e6-b1f8-ddeabace3bf6"),
    PASC_train=Input(rid="ri.foundry.main.dataset.69d89ac0-f1d2-464d-a87c-988f1e0f81ed")
)
require(tidyverse)
require(MatchIt)

matched_PASC <- function(CTRL_train, PASC_train) 
{
    #Use COVID-era training data
    PASC_train <- PASC_train %>% 
        filter(index_date == pasc_index)
    training_set <- CTRL_train %>%
        rbind(PASC_train) %>%
        filter(pre_covid == FALSE) %>% 
        mutate(sick = (cohort_label == "PASC" | cohort_label == "PASC_CFS"))
    #Set seed for replicability
    set.seed(37)
    #Actual training and testing size probably around 75% with exact random matching in this case.
    
    #Run matching. Exact matching on already-coarse demographic variables is in this case is very easy.
    matchy_train <- matchit(formula = sick ~ 1,
        data = training_set,
        exact = ~ ethnicity + race + is_female + age_bin + cci_score + pre_covid,
        replace = FALSE,
        m.order = "random")
    print("matching complete")
    matched_train <- get_matches(matchy_train,data = training_set)
    return(matched_train)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.7cb1ce32-b984-407b-9064-0ac46535d157"),
    matched_CFS=Input(rid="ri.foundry.main.dataset.8e524689-b720-4f54-a13b-c0ab7275e0ad"),
    matched_PASC=Input(rid="ri.foundry.main.dataset.9f95b1fc-c284-4492-8e78-0c08e17b1b73")
)
require(tidyverse)
require(MatchIt)

matched_pvc <- function(matched_CFS,matched_PASC) 
{
    matched_CFS <- matched_CFS %>% 
        filter(cohort_label == "CFS") %>%
        select(person_id,cohort_label,index_date,window_start,window_end,is_female,age_bin,ethnicity,race,cci_score,pre_covid)
    training_set <- matched_PASC %>%
        select(person_id,cohort_label,index_date,window_start,window_end,is_female,age_bin,ethnicity,race,cci_score,pre_covid) %>% 
        filter(cohort_label == "PASC") %>%
        rbind(matched_CFS) %>%
        mutate(sick = (cohort_label == "CFS"))
    #Set seed for replicability
    set.seed(37)
    #Actual training and testing size probably around 75% with exact random matching in this case.
    #return(training_set)
    #Run matching. Exact matching on already-coarse demographic variables is in this case is very easy.
    matchy_train <- matchit(formula = sick ~ 1,
        data = training_set,
        exact = ~ ethnicity + race + is_female + age_bin + pre_covid + cci_score,
        replace = FALSE,
        m.order = "random")
        print("matching complete")
    matched_train <- get_matches(matchy_train,data = training_set)
    return(matched_train)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.14274e39-6303-4fe3-a40b-771436859932"),
    selected_features=Input(rid="ri.foundry.main.dataset.76c1f9c9-60a4-487c-9ee4-5c10262800b3"),
    shap_summary_PASC=Input(rid="ri.foundry.main.dataset.92fac7ea-c839-4761-8d70-6db2d57006d8")
)
require(tidyverse)
pasc_shapleys <- function(selected_features, shap_summary_PASC) 
{
    features_sorted <- selected_features %>%
        arrange(condition_concept_id)
    features_vec <- c(features_sorted$condition_concept_name,"age_bin","is_female")
    placeholder <- shap_summary_PASC
    names(placeholder) <- features_vec
    placeholder <- placeholder %>%
        pivot_longer(cols = everything(), names_to = "condition_concept_name") %>%
        group_by(condition_concept_name) %>%
        summarize(pasc_mean_shapley = mean(value),
            pasc_median_shapley = median(value),
            pasc_sd_shapley = sd(value))
    return(placeholder)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f86845e3-dc9f-480f-91fa-6861acbb2085"),
    selected_features=Input(rid="ri.foundry.main.dataset.76c1f9c9-60a4-487c-9ee4-5c10262800b3"),
    shap_summary_PVC=Input(rid="ri.foundry.main.dataset.3696bc1f-d12b-43b2-8867-ce0df945c0cb")
)
require(tidyverse)
pvc_shapleys <- function(selected_features, shap_summary_PVC) 
{
    features_sorted <- selected_features %>%
        arrange(condition_concept_id)
    features_vec <- c(features_sorted$condition_concept_name,"age_bin","is_female")
    placeholder <- shap_summary_PVC
    names(placeholder) <- features_vec
    placeholder <- placeholder %>%
        pivot_longer(cols = everything(), names_to = "condition_concept_name") %>%
        group_by(condition_concept_name) %>%
        summarize(pvc_mean_shapley = mean(value),
            pvc_median_shapley = median(value),
            pvc_sd_shapley = sd(value))
    return(placeholder)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.a1fa9b1d-9fca-4600-ae5d-afb71f75da03"),
    feature_importance_cfs=Input(rid="ri.foundry.main.dataset.6bd8d2a5-17cc-47e2-b1fe-2f8e5ef07d9e"),
    feature_importance_pasc=Input(rid="ri.foundry.main.dataset.b96eb1ef-ab6d-423f-a3f9-b45fe5f0d71d"),
    feature_importance_pvc=Input(rid="ri.foundry.main.dataset.7efa8345-979d-4175-9b72-058ab250ad34"),
    selected_features=Input(rid="ri.foundry.main.dataset.76c1f9c9-60a4-487c-9ee4-5c10262800b3")
)
require(tidyverse)
stored_importance <- function(feature_importance_cfs, selected_features, feature_importance_pasc, feature_importance_pvc) 
{
    selected_features <- selected_features %>%
        arrange(condition_concept_id) %>%
        add_case(condition_concept_name = 'age_bin') %>%
        add_case(condition_concept_name = 'is_female')
    selected_features$importance_cfs = feature_importance_cfs[[1]]
    selected_features$importance_pvc = feature_importance_pvc[[1]]
    selected_features$importance_pasc= feature_importance_pasc[[1]]
    return(selected_features)
}

