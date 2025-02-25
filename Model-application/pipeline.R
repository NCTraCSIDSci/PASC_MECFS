# Authors: Abhishek Bhatia, Tomas McIntee, John Powers
# © 2025, The University of North Carolina at Chapel Hill. Permission is granted to use in accordance with the MIT license.
# The code is licensed under the MIT license.

require(tidyverse)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.a147bf8e-e7f8-47e3-b3ef-bdebaaac47ec"),
    small_test_subset=Input(rid="ri.foundry.main.dataset.a6a1fee4-b2de-4beb-8613-d51c5fbefe49")
)
library(tidyverse)
explore_plots_copied <- function(small_test_subset) 
{
    mutated <- small_test_subset %>%
        mutate(label = paste0(cohort_label,"_pre_covid_",pre_covid))
    g_scatter <- ggplot(mutated, aes(x = CFS_score, y = PASC_score, color = label))+
        geom_point(alpha = 0.1)
    print(g_scatter)
    g_box <- ggplot(mutated, aes(x = label, y = CFS_score))+
        geom_boxplot()
    print(g_box)
    g_box <- ggplot(mutated, aes(x = label, y = PASC_score))+
        geom_boxplot()
    g_box <- ggplot(mutated, aes(x = label, y = PVC_score))+
        geom_boxplot()
    print(g_box)
    return(NULL)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.5b1c4660-03c0-4084-964d-577041a648f7"),
    matched_test_cfs=Input(rid="ri.foundry.main.dataset.14e71fe9-d249-488e-b465-e1ff4e3f7c2f"),
    matched_test_pasc=Input(rid="ri.foundry.main.dataset.9a1004fe-f04b-4357-b204-3eba0c2ce47d")
)
library(tidyverse)
explore_plots_matched_copied <- function(matched_test_pasc, matched_test_cfs) 
{
    mutated <- matched_test_pasc %>%
        rbind(matched_test_cfs) %>%
        mutate(label = paste0(cohort_label,"_pre_covid_",pre_covid))
    g_scatter <- ggplot(mutated, aes(x = CFS_score, y = PASC_score, color = label))+
        geom_point(alpha = 0.1)
    print(g_scatter)
    g_box <- ggplot(mutated, aes(x = label, y = CFS_score))+
        geom_boxplot()
    print(g_box)
    g_box <- ggplot(mutated, aes(x = label, y = PASC_score))+
        geom_boxplot()
    print(g_box)
    return(NULL)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.bbd15d45-f672-4f28-96cd-d2d0637e7c2c"),
    matched_test_cfs=Input(rid="ri.foundry.main.dataset.14e71fe9-d249-488e-b465-e1ff4e3f7c2f"),
    matched_test_pasc=Input(rid="ri.foundry.main.dataset.9a1004fe-f04b-4357-b204-3eba0c2ce47d")
)
matched_rocs_copied <- function(matched_test_cfs, matched_test_pasc) 
{
        w_calcs <- matched_test_cfs %>% 
        mutate(age_bin = factor(age_bin),
            age = recode(age_bin,
                "1" = "Under 20",
                "2" = "20-29",
                "3" = "30-39",
                "4" = "40-49",
                "5" = "50-59",
                "6" = "60-69",
                "7" = "70-79",
                "8" = "80-89",
                .default = "90+")) %>%
        filter(cohort_label != "PASC") %>%
        arrange(CFS_score) %>% #Package that supplies the convenient "geom_roc()" not available. Direct calculation follows.
        mutate(sick = (cohort_label == "CFS" | cohort_label == "PASC_CFS"),
            pos = sick / sum(sick), #Case weights could optionally be applied here.
            neg = (1-sick) / (n()-sum(sick)),
            TPR = 1 - cumsum(pos), #True positive rate for setting threshold at this case's value.
            FPR = 1 - cumsum(neg)) #False positive rate for setting threshold at this case's value.
    print("Initialized data")
    #Calculate AUC
    TPR <- c(w_calcs$TPR,0)
    FPR_delta <- c(1, w_calcs$FPR) - c(w_calcs$FPR,0)
    AUC <-sum(TPR*FPR_delta)
    print(paste0("AUC is ",AUC))
    #Plot: 
    g_plot <- ggplot(w_calcs,aes(x = FPR, y = TPR, group=0, color = CFS_score))+
        geom_path()+
        coord_equal()+
        labs(title = "Receiver operating characteristic curve (ROC)", 
            subtitle = paste0("AUC = ",AUC))+
            scale_color_gradientn(colors = c("black","red","orange","yellow","green","blue","purple")) 
    print(g_plot)
    #and now again:
    w_calcs <- matched_test_pasc %>% 
        mutate(age_bin = factor(age_bin),
            age = recode(age_bin,
                "1" = "Under 20",
                "2" = "20-29",
                "3" = "30-39",
                "4" = "40-49",
                "5" = "50-59",
                "6" = "60-69",
                "7" = "70-79",
                "8" = "80-89",
                .default = "90+")) %>%
        arrange(PASC_score) %>% #Package that supplies the convenient "geom_roc()" not available. Direct calculation follows.
        filter(cohort_label != "CFS") %>%
        mutate(sick = (cohort_label == "PASC" | cohort_label == "PASC_CFS"),
            pos = sick / sum(sick), #Case weights could optionally be applied here.
            neg = (1-sick) / (n()-sum(sick)),
            TPR = 1 - cumsum(pos), #True positive rate for setting threshold at this case's value.
            FPR = 1 - cumsum(neg)) #False positive rate for setting threshold at this case's value.
    print("Initialized data")
    #Calculate AUC
    TPR <- c(w_calcs$TPR,0)
    FPR_delta <- c(1, w_calcs$FPR) - c(w_calcs$FPR,0)
    AUC <-sum(TPR*FPR_delta)
    print(paste0("AUC is ",AUC))
    #Plot: 
    g_plot <- ggplot(w_calcs,aes(x = FPR, y = TPR, group=0, color = PASC_score))+
        geom_path()+
        coord_equal()+
        labs(title = "Receiver operating characteristic curve (ROC)", 
            subtitle = paste0("AUC = ",AUC))+
            scale_color_gradientn(colors = c("black","red","orange","yellow","green","blue","purple")) 
    print(g_plot)
    return(w_calcs)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.14e71fe9-d249-488e-b465-e1ff4e3f7c2f"),
    small_test_subset=Input(rid="ri.foundry.main.dataset.a6a1fee4-b2de-4beb-8613-d51c5fbefe49")
)
require(tidyverse)
require(MatchIt)

matched_test_cfs <- function(small_test_subset) 
{
    CFS_test <- small_test_subset %>%
        arrange(person_id) %>%
        filter(cohort_label != "PASC") %>%
        mutate(sick = (cohort_label == "CFS" | cohort_label == "PASC_CFS"))
    #Set seed for replicability
    set.seed(37)
    #Actual training and testing size probably around 75% with exact random matching in this case.
    #return(training_set)
    #Run matching. Exact matching on already-coarse demographic variables is in this case is very easy.
    matchy_base <- matchit(formula = sick ~ 1,
        data = CFS_test,
        exact = ~ ethnicity + race + is_female + age_bin + cci_score, #Note not matching on pre-covid status, as we want to have both in this test sample!
        replace = FALSE,
        m.order = "random")
        print("matching complete")
    matchy <- get_matches(matchy_base,data = CFS_test)
    return(matchy)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.9a1004fe-f04b-4357-b204-3eba0c2ce47d"),
    small_test_subset=Input(rid="ri.foundry.main.dataset.a6a1fee4-b2de-4beb-8613-d51c5fbefe49")
)
require(tidyverse)
require(MatchIt)

matched_test_pasc <- function(small_test_subset) 
{
    PASC_test <- small_test_subset %>%
        arrange(person_id) %>%
        filter(cohort_label != "CFS") %>%
        mutate(sick = (cohort_label == "PASC" | cohort_label == "PASC_CFS"))
    #Set seed for replicability
    set.seed(37)
    #Actual training and testing size probably around 75% with exact random matching in this case.
    #return(training_set)
    #Run matching. Exact matching on already-coarse demographic variables is in this case is very easy.
    matchy_base <- matchit(formula = sick ~ 1,
        data = PASC_test,
        exact = ~ ethnicity + race + is_female + age_bin + cci_score, #Note not matching on pre-covid status, as we want to have both in this test sample!
        replace = FALSE,
        m.order = "random")
        print("matching complete")
    matchy <- get_matches(matchy_base,data = PASC_test)
    return(matchy)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.a3e3dd97-3c5a-4a6d-8794-eb9da6460fb4"),
    small_test_subset=Input(rid="ri.foundry.main.dataset.a6a1fee4-b2de-4beb-8613-d51c5fbefe49")
)
roc_cfs <- function(small_test_subset) 
{
        w_calcs <- small_test_subset %>% 
        mutate(age_bin = factor(age_bin),
            age = recode(age_bin,
                "1" = "Under 20",
                "2" = "20-29",
                "3" = "30-39",
                "4" = "40-49",
                "5" = "50-59",
                "6" = "60-69",
                "7" = "70-79",
                "8" = "80-89",
                .default = "90+")) %>%
        filter(cohort_label != "PASC") %>%
        arrange(CFS_score) %>% #Package that supplies the convenient "geom_roc()" not available. Direct calculation follows.
        mutate(sick = (cohort_label == "CFS" | cohort_label == "PASC_CFS"),
            pos = sick / sum(sick), #Case weights could optionally be applied here.
            neg = (1-sick) / (n()-sum(sick)),
            TPR = 1 - cumsum(pos), #True positive rate for setting threshold at this case's value.
            FPR = 1 - cumsum(neg)) #False positive rate for setting threshold at this case's value.
    print("Initialized data")
    #Calculate AUC
    TPR <- c(w_calcs$TPR,0)
    FPR_delta <- c(1, w_calcs$FPR) - c(w_calcs$FPR,0)
    AUC <-sum(TPR*FPR_delta)
    print(paste0("AUC is ",AUC))
    #Plot: 
    g_plot <- ggplot(w_calcs,aes(x = FPR, y = TPR, group=0, color = CFS_score))+
        geom_path()+
        coord_equal()+
        labs(title = "Receiver operating characteristic curve (ROC)", 
            subtitle = paste0("AUC = ",AUC))+
            scale_color_gradientn(colors = c("black","red","orange","yellow","green","blue","purple")) 
    print(g_plot)
    w_calcs <- w_calcs %>%
        mutate(Youden = TPR-FPR)
    acc_plot <- ggplot(w_calcs %>% filter(FPR > 0), aes(x = CFS_score, y = Youden))+
        geom_point()+
        ggtitle("Youden index")
    max_youden <- max(w_calcs$Youden)
    print(acc_plot)
    max_score <- w_calcs$CFS_score[w_calcs$Youden == max_youden]
    print(paste0("Max Youden's J: ",max_youden," at ", median(max_score)))
    return(w_calcs)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.1a8b029c-6178-4000-8e3d-fb6d25cfcd65"),
    matched_test_cfs=Input(rid="ri.foundry.main.dataset.14e71fe9-d249-488e-b465-e1ff4e3f7c2f")
)
roc_cfs_1 <- function(matched_test_cfs) 
{
        w_calcs <- matched_test_cfs %>% 
        mutate(age_bin = factor(age_bin),
            age = recode(age_bin,
                "1" = "Under 20",
                "2" = "20-29",
                "3" = "30-39",
                "4" = "40-49",
                "5" = "50-59",
                "6" = "60-69",
                "7" = "70-79",
                "8" = "80-89",
                .default = "90+")) %>%
        filter(cohort_label != "PASC") %>%
        arrange(CFS_score) %>% #Package that supplies the convenient "geom_roc()" not available. Direct calculation follows.
        mutate(sick = (cohort_label == "CFS" | cohort_label == "PASC_CFS"),
            pos = sick / sum(sick), #Case weights could optionally be applied here.
            neg = (1-sick) / (n()-sum(sick)),
            TPR = 1 - cumsum(pos), #True positive rate for setting threshold at this case's value.
            FPR = 1 - cumsum(neg)) #False positive rate for setting threshold at this case's value.
    print("Initialized data")
    #Calculate AUC
    TPR <- c(w_calcs$TPR,0)
    FPR_delta <- c(1, w_calcs$FPR) - c(w_calcs$FPR,0)
    AUC <-sum(TPR*FPR_delta)
    print(paste0("AUC is ",AUC))
    #Plot: 
    g_plot <- ggplot(w_calcs,aes(x = FPR, y = TPR, group=0, color = CFS_score))+
        geom_path()+
        coord_equal()+
        labs(title = "Receiver operating characteristic curve (ROC)", 
            subtitle = paste0("AUC = ",AUC))+
            scale_color_gradientn(colors = c("black","red","orange","yellow","green","blue","purple")) 
    print(g_plot)
    w_calcs <- w_calcs %>%
        mutate(Youden = TPR-FPR)
    acc_plot <- ggplot(w_calcs %>% filter(FPR > 0), aes(x = CFS_score, y = Youden))+
        geom_point()+
        ggtitle("Youden index")
    max_youden <- max(w_calcs$Youden)
    print(acc_plot)
    max_score <- w_calcs$CFS_score[w_calcs$Youden == max_youden]
    print(paste0("Max Youden's J: ",max_youden," at ", median(max_score)))
    return(w_calcs)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.633d99a8-5201-4582-bb23-1ec5299da2b2"),
    small_test_subset=Input(rid="ri.foundry.main.dataset.a6a1fee4-b2de-4beb-8613-d51c5fbefe49")
)
roc_pasc <- function(small_test_subset) 
{
    #and now again:
    w_calcs <- small_test_subset %>% 
        mutate(age_bin = factor(age_bin),
            age = recode(age_bin,
                "1" = "Under 20",
                "2" = "20-29",
                "3" = "30-39",
                "4" = "40-49",
                "5" = "50-59",
                "6" = "60-69",
                "7" = "70-79",
                "8" = "80-89",
                .default = "90+")) %>%
        arrange(PASC_score) %>% #Package that supplies the convenient "geom_roc()" not available. Direct calculation follows.
        filter(cohort_label != "CFS") %>%
        mutate(sick = (cohort_label == "PASC" | cohort_label == "PASC_CFS"),
            pos = sick / sum(sick), #Case weights could optionally be applied here.
            neg = (1-sick) / (n()-sum(sick)),
            TPR = 1 - cumsum(pos), #True positive rate for setting threshold at this case's value.
            FPR = 1 - cumsum(neg)) #False positive rate for setting threshold at this case's value.
    print("Initialized data")
    #Calculate AUC
    TPR <- c(w_calcs$TPR,0)
    FPR_delta <- c(1, w_calcs$FPR) - c(w_calcs$FPR,0)
    AUC <-sum(TPR*FPR_delta)
    print(paste0("AUC is ",AUC))
    #Plot: 
    g_plot <- ggplot(w_calcs,aes(x = FPR, y = TPR, group=0, color = PASC_score))+
        geom_path()+
        coord_equal()+
        labs(title = "Receiver operating characteristic curve (ROC)", 
            subtitle = paste0("AUC = ",AUC))+
            scale_color_gradientn(colors = c("black","red","orange","yellow","green","blue","purple")) 
    print(g_plot)
    w_calcs <- w_calcs %>%
        mutate(Youden = TPR-FPR)
    acc_plot <- ggplot(w_calcs %>% filter(FPR > 0), aes(x = PASC_score, y = Youden))+
        geom_point()+
        ggtitle("Youden index")
    max_youden <- max(w_calcs$Youden)
    print(acc_plot)
    max_score <- w_calcs$PASC_score[w_calcs$Youden == max_youden]
    print(paste0("Max Youden's J: ",max_youden," at ", median(max_score)))
    return(w_calcs)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.6ab7e7fe-e0ad-4643-be0f-43b1c3848246"),
    matched_test_pasc=Input(rid="ri.foundry.main.dataset.9a1004fe-f04b-4357-b204-3eba0c2ce47d")
)
roc_pasc_1 <- function(matched_test_pasc) 
{
    #and now again:
    w_calcs <- matched_test_pasc %>% 
        mutate(age_bin = factor(age_bin),
            age = recode(age_bin,
                "1" = "Under 20",
                "2" = "20-29",
                "3" = "30-39",
                "4" = "40-49",
                "5" = "50-59",
                "6" = "60-69",
                "7" = "70-79",
                "8" = "80-89",
                .default = "90+")) %>%
        arrange(PASC_score) %>% #Package that supplies the convenient "geom_roc()" not available. Direct calculation follows.
        filter(cohort_label != "CFS") %>%
        mutate(sick = (cohort_label == "PASC" | cohort_label == "PASC_CFS"),
            pos = sick / sum(sick), #Case weights could optionally be applied here.
            neg = (1-sick) / (n()-sum(sick)),
            TPR = 1 - cumsum(pos), #True positive rate for setting threshold at this case's value.
            FPR = 1 - cumsum(neg)) #False positive rate for setting threshold at this case's value.
    print("Initialized data")
    #Calculate AUC
    TPR <- c(w_calcs$TPR,0)
    FPR_delta <- c(1, w_calcs$FPR) - c(w_calcs$FPR,0)
    AUC <-sum(TPR*FPR_delta)
    print(paste0("AUC is ",AUC))
    #Plot: 
    g_plot <- ggplot(w_calcs,aes(x = FPR, y = TPR, group=0, color = PASC_score))+
        geom_path()+
        coord_equal()+
        labs(title = "Receiver operating characteristic curve (ROC)", 
            subtitle = paste0("AUC = ",AUC))+
            scale_color_gradientn(colors = c("black","red","orange","yellow","green","blue","purple")) 
    print(g_plot)
    w_calcs <- w_calcs %>%
        mutate(Youden = TPR-FPR)
    acc_plot <- ggplot(w_calcs %>% filter(FPR > 0), aes(x = PASC_score, y = Youden))+
        geom_point()+
        ggtitle("Youden index")
    max_youden <- max(w_calcs$Youden)
    print(acc_plot)
    max_score <- w_calcs$PASC_score[w_calcs$Youden == max_youden]
    print(paste0("Max Youden's J: ",max_youden," at ", median(max_score)))
    return(w_calcs)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.4bcb5e11-e89b-4a58-b9da-936da6aaec58"),
    small_test_subset=Input(rid="ri.foundry.main.dataset.a6a1fee4-b2de-4beb-8613-d51c5fbefe49")
)
roc_pvc_2 <- function(small_test_subset) 
{
    #and now again:
    w_calcs <- small_test_subset %>% 
        mutate(age_bin = factor(age_bin),
            age = recode(age_bin,
                "1" = "Under 20",
                "2" = "20-29",
                "3" = "30-39",
                "4" = "40-49",
                "5" = "50-59",
                "6" = "60-69",
                "7" = "70-79",
                "8" = "80-89",
                .default = "90+")) %>%
        arrange(PVC_score) %>% #Package that supplies the convenient "geom_roc()" not available. Direct calculation follows.
        filter(cohort_label == "CFS" | cohort_label == "PASC") %>%
        mutate(sick = (cohort_label == "CFS"),
            pos = sick / sum(sick), #Case weights could optionally be applied here.
            neg = (1-sick) / (n()-sum(sick)),
            TPR = 1 - cumsum(pos), #True positive rate for setting threshold at this case's value.
            FPR = 1 - cumsum(neg)) #False positive rate for setting threshold at this case's value.
    print("Initialized data")
    #Calculate AUC
    TPR <- c(w_calcs$TPR,0)
    FPR_delta <- c(1, w_calcs$FPR) - c(w_calcs$FPR,0)
    AUC <-sum(TPR*FPR_delta)
    print(paste0("AUC is ",AUC))
    #Plot: 
    g_plot <- ggplot(w_calcs,aes(x = FPR, y = TPR, group=0, color = PVC_score))+
        geom_path()+
        coord_equal()+
        labs(title = "Receiver operating characteristic curve (ROC)", 
            subtitle = paste0("AUC = ",AUC))+
            scale_color_gradientn(colors = c("black","red","orange","yellow","green","blue","purple")) 
    print(g_plot)
    w_calcs <- w_calcs %>%
        mutate(Youden = TPR-FPR)
    acc_plot <- ggplot(w_calcs %>% filter(FPR > 0), aes(x = PVC_score, y = Youden))+
        geom_point()+
        ggtitle("Youden index")
    max_youden <- max(w_calcs$Youden)
    print(acc_plot)
    max_score <- w_calcs$PVC_score[w_calcs$Youden == max_youden]
    print(paste0("Max Youden's J: ",max_youden," at ", median(max_score)))
    return(w_calcs)
}

