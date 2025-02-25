# Authors: Abhishek Bhatia, Tomas McIntee, John Powers
# © 2025, The University of North Carolina at Chapel Hill. Permission is granted to use in accordance with the MIT license.
# The code is licensed under the MIT license.

require(tidyverse)
require(VennDiagram)
require(zoo)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.fc80da73-ad6c-40b8-9b22-80336b6f406c"),
    subgroup_sizes=Input(rid="ri.foundry.main.dataset.29d1cbfc-23b9-4150-8d27-37085cd5f857")
)

S2 <- function( subgroup_sizes) 
{
    grid.newpage()                  # Creating venn diagram with VennDiagram
    draw.pairwise.venn(area1 = sum(subgroup_sizes$CP_PASC * subgroup_sizes$count_in_subgroup),
        area2 = sum(subgroup_sizes$CP_ME_CFS * subgroup_sizes$count_in_subgroup),
        cross.area = sum(subgroup_sizes$count_in_subgroup * (subgroup_sizes$CP_ME_CFS & subgroup_sizes$CP_PASC)),
        category = c("Computational phenotype - PASC","Computational phenotype - ME/CFS"))

    grid.newpage()                  # Creating venn diagram with VennDiagram
    draw.pairwise.venn(area1 = sum((subgroup_sizes$cohort_label == 'PASC' | subgroup_sizes$cohort_label == 'PASC_CFS')*(subgroup_sizes$count_in_subgroup)),
        area2 = sum((subgroup_sizes$cohort_label == 'CFS' | subgroup_sizes$cohort_label == 'PASC_CFS')*(subgroup_sizes$count_in_subgroup)),
        cross.area = sum((subgroup_sizes$cohort_label == 'PASC_CFS') * subgroup_sizes$count_in_subgroup),
        category = c("Diagnosed - PASC","Diagnosed - ME/CFS"))
    return(subgroup_sizes)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.463087f9-cb7a-473d-acd2-35cdf282a0ae"),
    panel_A=Input(rid="ri.foundry.main.dataset.2aee3f87-d0db-4825-a4f7-bd31264f04a3")
)
figure_1_panel_A <- function(panel_A) 
{
    x_quarters <- panel_A %>%
        mutate(index_date = paste0(year_index,"-Q",quarter_index)) %>%
        group_by(index_date) %>%
        mutate(prevalence = number / sum(number)) %>%
        ungroup() %>%
        filter(cohort_label != "CTRL" & cohort_label != "R5382")
    x_quarters$index_date <- factor(x_quarters$index_date)
    g <- ggplot(x_quarters,aes(x = index_date, y = prevalence))+
        geom_line(aes(linetype = cohort_label, color = cohort_label, group = cohort_label))+
        theme(axis.text.x = element_text(angle = 45))
    print(g)
    return(x_quarters)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.cf71f743-481d-4ce5-9dc9-240c083f1d6f"),
    panel_C=Input(rid="ri.foundry.main.dataset.97e710d7-ab82-40be-ab8b-1f6446451c7a")
)
figure_1_panel_C <- function(panel_C) 
{
    x_quarters <- panel_C %>%
        mutate(index_date = paste0(year_index,"-Q",quarter_index),
        groups = ifelse(CP_PASC & CP_ME_CFS,"Both",ifelse(CP_PASC,"PASC computable phenotype",ifelse(CP_ME_CFS,"ME/CFS computable phenotype","Neither")))) %>%
        group_by(index_date) %>%
        mutate(prevalence = number / sum(number)) %>%
        ungroup() %>%
        filter(groups != "Neither")
    x_quarters$index_date <- factor(x_quarters$index_date)
    g <- ggplot(x_quarters,aes(x = index_date, y = prevalence))+
        geom_line(aes(linetype = groups, color = groups, group = groups))+
        theme(axis.text.x = element_text(angle = 45))
    print(g)
    return(x_quarters)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.35f353eb-01a1-4429-9d72-99f604060e3c"),
    data_switch_container=Input(rid="ri.foundry.main.dataset.ff35d9f3-f359-4aca-9d8a-3806c8a2675a")
)
require(tableone)

from_pkg_tableone <- function(data_switch_container) 
{
    #Generate tableone object    
    table <- CreateTableOne(data=data_switch_container,
        vars=c('sex', 'race', 'ethnicity'),
        factorVars=c('cci_score','age_bin'),
        strata='cohort_label')
    #Convert to matrix
    matrix_one <- print(table)
    #Convert matrix to data frame
    table_one = as.data.frame(matrix_one)
    #Make row names into column
    table_one$groupings <- row.names(matrix_one)
    table_one <- table_one[c(length(table_one),(1:(length(table_one)-1)))]
    return(table_one)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b554cb32-8eb5-4994-b746-3dba25a06875"),
    age_descriptive=Input(rid="ri.foundry.main.dataset.fe059d7e-eeee-4ee4-8f59-75e9d86f4502"),
    by_race_descriptive=Input(rid="ri.foundry.main.dataset.bd34215a-88e4-4872-a363-ff598dc921c5"),
    by_sex_descriptive=Input(rid="ri.foundry.main.dataset.e813678c-3284-44db-98d2-3ded54cf1754"),
    cci_descriptive=Input(rid="ri.foundry.main.dataset.e032fb0e-0d41-46f7-ad93-684828cdff3c"),
    ethnicity_descriptive=Input(rid="ri.foundry.main.dataset.9cb42b5e-ab85-40a3-b8f8-02db5ff3790c"),
    insurance_descriptive=Input(rid="ri.foundry.main.dataset.7963cecf-19ad-4197-9105-ed97c90101c8"),
    poverty_descriptive=Input(rid="ri.foundry.main.dataset.fae9099d-0de9-42c9-bf30-b0436ec6017a")
)
table_one <- function(by_sex_descriptive, by_race_descriptive, ethnicity_descriptive, cci_descriptive, age_descriptive, poverty_descriptive, insurance_descriptive) 
{
    bound_tables <- by_sex_descriptive %>%
        bind_rows(by_race_descriptive) %>%
        bind_rows(ethnicity_descriptive) %>%
        bind_rows(cci_descriptive) %>%
        bind_rows(age_descriptive) %>%
        bind_rows(poverty_descriptive) %>%
        bind_rows(insurance_descriptive) %>%
        group_by(row_name, subtable, cohort_label) %>%
        summarize(count_in_bin = sum(count_in_bin,na.rm = TRUE)) %>%
        group_by(subtable,cohort_label) %>%
        mutate(pct = count_in_bin/sum(count_in_bin)) %>%
        # mutate(entry = ifelse(count_in_bin >= 20, paste0(count_in_bin, " (",round(100*pct,digits = 1),"%)"), "* (< 20)")) %>%
        mutate(entry = paste0(round(100*pct,digits = 1),"%")) %>%
        select(cohort_label, row_name, entry) %>%
        ungroup() %>%
        arrange(subtable, row_name) %>%
        pivot_wider(names_from = cohort_label, values_from = entry) %>%
        arrange(subtable,row_name)

    PASC_CP <- by_sex_descriptive %>%
        bind_rows(by_race_descriptive) %>%
        bind_rows(ethnicity_descriptive) %>%
        bind_rows(cci_descriptive) %>%
        bind_rows(age_descriptive) %>%
        bind_rows(poverty_descriptive) %>%
        bind_rows(insurance_descriptive) %>%
        group_by(row_name, subtable) %>%
        summarize(PASC_CP = sum(PASC_CP,na.rm = TRUE), PASC_CP_total = sum(PASC_CP_total,na.rm = TRUE)) %>%
        mutate(pct = sum(PASC_CP)/sum(PASC_CP_total)) %>%
        mutate(PASC_CP_pct = paste0(round(100*pct,digits = 1),"%")) %>%
        select(subtable, row_name, PASC_CP_pct) %>%
        arrange(subtable,row_name)

    MECFS_CP <- by_sex_descriptive %>%
        bind_rows(by_race_descriptive) %>%
        bind_rows(ethnicity_descriptive) %>%
        bind_rows(cci_descriptive) %>%
        bind_rows(age_descriptive) %>%
        bind_rows(poverty_descriptive) %>%
        bind_rows(insurance_descriptive) %>%
        group_by(row_name, subtable) %>%
        summarize(MECFS_CP = sum(MECFS_CP,na.rm = TRUE), MECFS_CP_total = sum(MECFS_CP_total,na.rm = TRUE)) %>%
        mutate(pct = sum(MECFS_CP)/sum(MECFS_CP_total)) %>%
        mutate(MECFS_CP_pct = paste0(round(100*pct,digits = 1),"%")) %>%
        select(subtable, row_name, MECFS_CP_pct) %>%
        arrange(subtable,row_name)

    final_1 <- merge(bound_tables, PASC_CP, on=c("subtable", "row_name"))
    final_2 <- merge(final_1, MECFS_CP, on=c("subtable", "row_name"))

    return(final_2)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.a36d91b6-30d7-4350-a0c5-6edd7d6ee666"),
    S2=Input(rid="ri.foundry.main.dataset.fc80da73-ad6c-40b8-9b22-80336b6f406c")
)
unnamed <- function( S2) 
{
    summary_stats <- S2 %>%
        summarize(CP_PASC_sensitivity = sum(count_in_subgroup*CP_PASC*cohort_label %in% c("PASC","PASC_CFS"))/ sum(count_in_subgroup*cohort_label %in% c("PASC","PASC_CFS")),
            CP_ME_CFS_sensitivity = sum(count_in_subgroup*CP_ME_CFS*cohort_label %in% c("CFS","PASC_CFS"))/ sum(count_in_subgroup*cohort_label %in% c("CFS","PASC_CFS")),
            CP_PASC_specificity = sum(count_in_subgroup*(!CP_PASC)*(!cohort_label %in% c("PASC","PASC_CFS")))/ sum(count_in_subgroup*!(cohort_label %in% c("PASC","PASC_CFS"))),
            CP_ME_CFS_specificity = sum(count_in_subgroup*(!CP_ME_CFS)*(!cohort_label %in% c("CFS","PASC_CFS")))/ sum(count_in_subgroup*(!cohort_label %in% c("CFS","PASC_CFS"))))
    return(summary_stats)
}

