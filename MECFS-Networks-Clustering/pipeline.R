# Authors: Abhishek Bhatia, Tomas McIntee, John Powers
# © 2025, The University of North Carolina at Chapel Hill. Permission is granted to use in accordance with the MIT license.
# The code is licensed under the MIT license.

library(dplyr)
library(magrittr)
library(tidygraph)
library(igraph)
#library(statnet)
#library (intergraph)
#library(circlize)
library(ggraph)
#library(bootnet)
library(treemap)

@transform_pandas(
    Output(rid="ri.vector.main.execute.8d85ebf2-9293-41ef-a7da-0d06f2881301"),
    clusters_and_labels_cp=Input(rid="ri.foundry.main.dataset.2ee23deb-c256-4b2e-815a-ce1c500b68b4")
)
library(ggplot2)
library(dplyr)
library(tidyr)

cp_heatmap_3cats <- function(clusters_and_labels_cp) {
    df <- clusters_and_labels_cp

    df_long <- df %>%
    pivot_longer(cols = starts_with("sub_"), names_to = "cluster", values_to = "value") %>%
    mutate(cluster = case_when(
      cluster == "sub_1" ~ "Cluster 1",
      cluster == "sub_2" ~ "Cluster 2",
      cluster == "sub_3" ~ "Cluster 3"
    )) %>%
    group_by(cluster, dx_label) %>%
    summarise(percentage = mean(value) * 100, .groups = 'drop') 

    p1 <- ggplot(df_long, aes(y = dx_label, x = cluster, fill = percentage, label = sprintf("%.1f%%", percentage))) +
    geom_tile() +
    geom_text(size = 8) +
    scale_fill_gradient(limits = c(10, 65), low = "white", high = "royalblue3", guide = "colourbar") +
    labs(x = "", y = "") +
    theme_minimal() +
    theme(axis.text.x = element_text(angle = 0, hjust = 0.5, size = 18), axis.title.x = element_text(size = 16), axis.ticks.x = element_blank(), axis.text.x.top = element_text(angle = 0, hjust = 0.5), axis.title.x.top = element_text(), legend.position = "right", legend.title = element_blank(), legend.text = element_text(size = 16), axis.text.y = element_text(angle = 0, size = 18)) +
    scale_x_discrete(position = "top") +
    scale_y_discrete(limits = rev)

    print(p1)

    return(NULL)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.b6494652-addb-44fe-9696-39efc989a56a"),
    subphenotype_summary_cp=Input(rid="ri.foundry.main.dataset.d1fd284f-250f-4165-95a6-3511c9012019")
)
library(ggplot2)
library(dplyr)
library(tidyr)

cp_heatmap_8cats <- function(subphenotype_summary_cp) {
    df <- subphenotype_summary_cp

    p1 <- ggplot(df, aes(y = dx_label, x = factor(subphenotype, level=c('None', '1', '2', '3', '1 & 2', '1 & 3', '2 & 3', 'All')), fill = percentage, label = sprintf("%.1f%%", percentage))) +
    geom_tile() +
    geom_text(size = 8) +
    scale_fill_gradient(limits = c(0, 50), low = "white", high = "royalblue3", guide = "colourbar") +
    labs(x = "Subphenotype \n", y = "") +
    theme_minimal() +
    theme(axis.text.x = element_text(angle = 0, hjust = 0.5, size = 18), axis.title.x = element_text(size = 20), axis.ticks.x = element_blank(), axis.text.x.top = element_text(angle = 0, hjust = 0.5), axis.title.x.top = element_text(), legend.position = "right", legend.title = element_blank(), legend.text = element_text(size = 16), axis.text.y = element_text(angle = 0, size = 18)) +
    scale_x_discrete(position = "top") +
    scale_y_discrete(limits = rev)

    print(p1)

    return(NULL)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.a07de311-6bcd-4a2c-ab80-b4a5bb5847df"),
    filtered_conditions_cfs=Input(rid="ri.foundry.main.dataset.32909008-285f-488a-8b48-5872ce2d7884"),
    topdiagnoses_cfs=Input(rid="ri.foundry.main.dataset.81cca64e-8fac-43ca-8d97-ea66e3bf1466")
)
create_network_cfs <- function(topdiagnoses_cfs, filtered_conditions_cfs) {
  # Pre-filter to include only relevant diagnoses
  relevant_conditions <- filtered_conditions_cfs %>%
    inner_join(topdiagnoses_cfs, by = 'condition_concept_name')
  
  # Prepare data for self-join to minimize size
  conditions_renamed <- relevant_conditions %>%
    select(person_id, condition_concept_name2 = condition_concept_name)
  
  # Perform self-join with a focus on reducing memory footprint
  final <- relevant_conditions %>%
    inner_join(conditions_renamed, by = "person_id") %>%
    filter(condition_concept_name > condition_concept_name2) %>%
    group_by(condition_concept_name, condition_concept_name2) %>%
    summarise(weight = n() * 20, .groups = 'drop') %>%
    ungroup() %>%
    mutate(from = condition_concept_name, to = condition_concept_name2) %>%
    select(condition_concept_name, to, from, weight) %>%
    arrange(from, to)
  
  return(final)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.1505c954-b821-468c-95d6-d6f6b1653611"),
    filtered_conditions_pasc=Input(rid="ri.foundry.main.dataset.84caf9d0-befc-4629-b5a9-49743bf12f55"),
    topdiagnoses_pasc=Input(rid="ri.foundry.main.dataset.21e21123-d244-4084-bff7-857701926e74")
)
create_network_pasc <- function(topdiagnoses_pasc, filtered_conditions_pasc) {
  # Limit to just top 30 diagnoses and prepare for self-join by selecting necessary columns once
  filtered_conditions <- filtered_conditions_pasc %>%
    select(person_id, condition_concept_name) %>%
    inner_join(topdiagnoses_pasc, by = 'condition_concept_name')
  
  # Perform self-join with renamed column for the second dataset on the fly, filter, and calculate weights
  final <- filtered_conditions %>%
    inner_join(filtered_conditions %>% rename(condition_concept_name2 = condition_concept_name), by = "person_id") %>%
    filter(condition_concept_name > condition_concept_name2) %>%
    count(condition_concept_name, condition_concept_name2, name = "weight") %>%
    mutate(weight = weight * 20, from = condition_concept_name, to = condition_concept_name2) %>%
    select(condition_concept_name, to, from, weight) %>%
    arrange(from, to)
  
  return(final)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.34c08f79-5f7e-408d-81a4-668ae4677eea"),
    filtered_conditions_pasc_cfs=Input(rid="ri.foundry.main.dataset.b5c67ad3-778b-4499-81ad-9fc79ef92f93"),
    topdiagnoses_pasc_cfs=Input(rid="ri.foundry.main.dataset.1345f383-f6ae-4264-a1d7-cc56e5d2ab83")
)
create_network_pasc_cfs <- function(topdiagnoses_pasc_cfs, filtered_conditions_pasc_cfs) {
  # Limit to just top 30 diagnoses and prepare for self-join by selecting necessary columns once
  filtered_conditions <- filtered_conditions_pasc_cfs %>%
    select(person_id, condition_concept_name) %>%
    inner_join(topdiagnoses_pasc_cfs, by = 'condition_concept_name')
  
  # Perform self-join with renamed column for the second dataset on the fly, filter, and calculate weights
  final <- filtered_conditions %>%
    inner_join(filtered_conditions %>% rename(condition_concept_name2 = condition_concept_name), by = "person_id") %>%
    filter(condition_concept_name > condition_concept_name2) %>%
    count(condition_concept_name, condition_concept_name2, name = "weight") %>%
    mutate(weight = weight * 20, from = condition_concept_name, to = condition_concept_name2) %>%
    select(condition_concept_name, to, from, weight) %>%
    arrange(from, to)
  
  return(final)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.104def9c-77b3-4b9a-9439-1d9ff5567ad5"),
    sampling_both=Input(rid="ri.foundry.main.dataset.1beae739-b571-4cd4-bc0f-4ce0cf928c70"),
    topdiagnoses_pasc_or_cfs=Input(rid="ri.foundry.main.dataset.2c11529a-b8d3-4339-828e-0213b2378f95")
)
create_network_pasc_or_cfs <- function(topdiagnoses_pasc_or_cfs, sampling_both) {
  # Limit to just top diagnoses and prepare for self-join by selecting necessary columns once
  filtered_conditions <- sampling_both %>%
    select(person_id, condition_concept_name) %>%
    inner_join(topdiagnoses_pasc_or_cfs, by = 'condition_concept_name')
  
  # Perform self-join with renamed column for the second dataset on the fly, filter, and calculate weights
  final <- filtered_conditions %>%
    inner_join(filtered_conditions %>% rename(condition_concept_name2 = condition_concept_name), by = "person_id") %>%
    filter(condition_concept_name > condition_concept_name2) %>%
    count(condition_concept_name, condition_concept_name2, name = "weight") %>%
    mutate(weight = weight * 20, from = condition_concept_name, to = condition_concept_name2) %>%
    select(condition_concept_name, to, from, weight) %>%
    arrange(from, to)
  
  return(final)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.33ab3a54-b64a-4822-961a-aad7e00c5fb1"),
    sampling_both=Input(rid="ri.foundry.main.dataset.1beae739-b571-4cd4-bc0f-4ce0cf928c70"),
    topfeatures_pasc_or_mecfs=Input(rid="ri.foundry.main.dataset.ac70f275-4b39-49a5-80b0-afe146127052")
)
create_network_pasc_or_cfs_1 <- function(sampling_both, topfeatures_pasc_or_mecfs) {
  # Limit to just top diagnoses and prepare for self-join by selecting necessary columns once
  filtered_conditions <- sampling_both %>%
    select(person_id, condition_concept_name) %>%
    inner_join(topfeatures_pasc_or_mecfs, by = 'condition_concept_name')
  
  # Perform self-join with renamed column for the second dataset on the fly, filter, and calculate weights
  final <- filtered_conditions %>%
    inner_join(filtered_conditions %>% rename(condition_concept_name2 = condition_concept_name), by = "person_id") %>%
    filter(condition_concept_name > condition_concept_name2) %>%
    count(condition_concept_name, condition_concept_name2, name = "weight") %>%
    mutate(weight = weight * 20, from = condition_concept_name, to = condition_concept_name2) %>%
    select(condition_concept_name, to, from, weight) %>%
    arrange(from, to)
  
  return(final)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.fc084baf-dc02-467a-b689-ca5131868572"),
    clusters_and_labels_diagnostic=Input(rid="ri.foundry.main.dataset.8209cadc-4f2f-4ada-bd32-4af8a2b9fb00")
)
library(ggplot2)
library(dplyr)
library(tidyr)

dx_heatmap_3cats <- function(clusters_and_labels_diagnostic) {
    df <- clusters_and_labels_diagnostic

    df_long <- df %>%
    pivot_longer(cols = starts_with("sub_"), names_to = "cluster", values_to = "value") %>%
    mutate(cluster = case_when(
      cluster == "sub_1" ~ "Cluster 1",
      cluster == "sub_2" ~ "Cluster 2",
      cluster == "sub_3" ~ "Cluster 3"
    )) %>%
    group_by(cluster, dx_label) %>%
    summarise(percentage = mean(value) * 100, .groups = 'drop') 

    p1 <- ggplot(df_long, aes(y = dx_label, x = cluster, fill = percentage, label = sprintf("%.1f%%", percentage))) +
    geom_tile() +
    geom_text(size = 8) +
    scale_fill_gradient(limits = c(10, 65), low = "white", high = "royalblue3", guide = "colourbar") +
    labs(x = "", y = "") +
    theme_minimal() +
    theme(axis.text.x = element_text(angle = 0, hjust = 0.5, size = 18), axis.title.x = element_text(size = 16), axis.ticks.x = element_blank(), axis.text.x.top = element_text(angle = 0, hjust = 0.5), axis.title.x.top = element_text(), legend.position = "right", legend.title = element_blank(), legend.text = element_text(size = 16), axis.text.y = element_text(angle = 0, size = 18)) +
    scale_x_discrete(position = "top") +
    scale_y_discrete(limits = rev)

    print(p1)

    return(NULL)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.5a9ca4e4-6833-492f-97ab-e838bcf72b82"),
    subphenotype_summary=Input(rid="ri.foundry.main.dataset.c01e74ca-a0d8-4d6e-83d6-3c2f7ed4cf97")
)
library(ggplot2)
library(dplyr)
library(tidyr)

dx_heatmap_8cats <- function(subphenotype_summary) {
    df <- subphenotype_summary

    p1 <- ggplot(df, aes(y = dx_label, x = factor(subphenotype, level=c('None', '1', '2', '3', '1 & 2', '1 & 3', '2 & 3', 'All')), fill = percentage, label = sprintf("%.1f%%", percentage))) +
    geom_tile() +
    geom_text(size = 8) +
    scale_fill_gradient(limits = c(0, 50), low = "white", high = "royalblue3", guide = "colourbar") +
    labs(x = "Subphenotype \n", y = "") +
    theme_minimal() +
    theme(axis.text.x = element_text(angle = 0, hjust = 0.5, size = 18), axis.title.x = element_text(size = 20), axis.ticks.x = element_blank(), axis.text.x.top = element_text(angle = 0, hjust = 0.5), axis.title.x.top = element_text(), legend.position = "right", legend.title = element_blank(), legend.text = element_text(size = 16), axis.text.y = element_text(angle = 0, size = 18)) +
    scale_x_discrete(position = "top") +
    scale_y_discrete(limits = rev)

    print(p1)

    return(NULL)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.2d0b5eba-92cf-4f7a-9912-619d0f17a666"),
    create_network_cfs=Input(rid="ri.foundry.main.dataset.a07de311-6bcd-4a2c-ab80-b4a5bb5847df"),
    filtered_conditions_cfs=Input(rid="ri.foundry.main.dataset.32909008-285f-488a-8b48-5872ce2d7884"),
    topdiagnoses_cfs=Input(rid="ri.foundry.main.dataset.81cca64e-8fac-43ca-8d97-ea66e3bf1466")
)
network_metrics_cfs <-
  function(create_network_cfs,
           topdiagnoses_cfs,
           filtered_conditions_cfs) {
    create_network_cfs <-
      create_network_cfs %>% arrange(condition_concept_name)
    
    print(create_network_cfs$pat_count)
    
    total_pat_count <-
      length(unique(filtered_conditions_cfs$person_id))
    print(total_pat_count)
    
    tdy_net <- as_tbl_graph(create_network_cfs, directed = FALSE)
    
    tdy_net <- tdy_net %>%
      activate(nodes) %>%
      arrange(name) %>%
      mutate(pat_count = topdiagnoses_cfs$pat_count) %>%
      mutate(percentage = pat_count / total_pat_count) %>%
      mutate(degree = centrality_degree()) %>%
      mutate(community = group_louvain(weights = weight)) %>%
      mutate(cent = centrality_betweenness()) %>%
      mutate(local_trans = local_transitivity(weights = weight)) %>%
      arrange(-degree)
    
    tdy_net <- tdy_net %>%
      activate(nodes) %>%
      as_tibble() %>%
      as.data.frame()
    
    
    tdy_net <- tdy_net %>%
      select(name,
             percentage,
             community,
             cent,
             degree,
             local_trans,
             pat_count)
    return(tdy_net)
  }

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.05ae9739-033a-4af4-8c23-2b44265a3583"),
    create_network_pasc=Input(rid="ri.foundry.main.dataset.1505c954-b821-468c-95d6-d6f6b1653611"),
    filtered_conditions_pasc=Input(rid="ri.foundry.main.dataset.84caf9d0-befc-4629-b5a9-49743bf12f55"),
    topdiagnoses_pasc=Input(rid="ri.foundry.main.dataset.21e21123-d244-4084-bff7-857701926e74")
)
network_metrics_pasc <-
  function(topdiagnoses_pasc,
           create_network_pasc,
           filtered_conditions_pasc) {
    create_network_pasc <-
      create_network_pasc %>% arrange(condition_concept_name)
    
    print(create_network_pasc$pat_count)
    
    total_pat_count <-
      length(unique(filtered_conditions_pasc$person_id))
    print(total_pat_count)
    
    tdy_net <- as_tbl_graph(create_network_pasc, directed = FALSE)
    
    tdy_net <- tdy_net %>%
      activate(nodes) %>%
      arrange(name) %>%
      mutate(pat_count = topdiagnoses_pasc$pat_count) %>%
      mutate(percentage = pat_count / total_pat_count) %>%
      mutate(degree = centrality_degree()) %>%
      mutate(community = group_louvain(weights = weight)) %>%
      mutate(cent = centrality_betweenness()) %>%
      mutate(local_trans = local_transitivity(weights = weight)) %>%
      arrange(-degree)
    
    tdy_net <- tdy_net %>%
      activate(nodes) %>%
      as_tibble() %>%
      as.data.frame()
    
    
    tdy_net <- tdy_net %>%
      select(name,
             percentage,
             community,
             cent,
             degree,
             local_trans,
             pat_count)
    return(tdy_net)
  }

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.228c12c3-2ece-4543-b2e9-6d9a5b66b225"),
    create_network_pasc_cfs=Input(rid="ri.foundry.main.dataset.34c08f79-5f7e-408d-81a4-668ae4677eea"),
    filtered_conditions_pasc_cfs=Input(rid="ri.foundry.main.dataset.b5c67ad3-778b-4499-81ad-9fc79ef92f93"),
    topdiagnoses_pasc_cfs=Input(rid="ri.foundry.main.dataset.1345f383-f6ae-4264-a1d7-cc56e5d2ab83")
)
network_metrics_pasc_cfs <-
  function(create_network_pasc_cfs,
           topdiagnoses_pasc_cfs,
           filtered_conditions_pasc_cfs) {
    create_network_pasc_cfs <-
      create_network_pasc_cfs %>% arrange(condition_concept_name)
    
    print(create_network_pasc_cfs$pat_count)
    
    total_pat_count <-
      length(unique(filtered_conditions_pasc_cfs$person_id))
    print(total_pat_count)
    
    tdy_net <- as_tbl_graph(create_network_pasc_cfs, directed = FALSE)
    
    tdy_net <- tdy_net %>%
      activate(nodes) %>%
      arrange(name) %>%
      mutate(pat_count = topdiagnoses_pasc_cfs$pat_count) %>%
      mutate(percentage = pat_count / total_pat_count) %>%
      mutate(degree = centrality_degree()) %>%
      mutate(community = group_louvain(weights = weight)) %>%
      mutate(cent = centrality_betweenness()) %>%
      mutate(local_trans = local_transitivity(weights = weight)) %>%
      arrange(-degree)
    
    tdy_net <- tdy_net %>%
      activate(nodes) %>%
      as_tibble() %>%
      as.data.frame()
    

    tdy_net <- tdy_net %>%
      select(name,
             percentage,
             community,
             cent,
             degree,
             local_trans,
             pat_count)
    return(tdy_net)
  }

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.28907b00-d6de-4174-b381-dd4e834508de"),
    create_network_pasc_or_cfs=Input(rid="ri.foundry.main.dataset.104def9c-77b3-4b9a-9439-1d9ff5567ad5"),
    sampling_both=Input(rid="ri.foundry.main.dataset.1beae739-b571-4cd4-bc0f-4ce0cf928c70"),
    topdiagnoses_pasc_or_cfs=Input(rid="ri.foundry.main.dataset.2c11529a-b8d3-4339-828e-0213b2378f95")
)
library(tidygraph)
library(dplyr)
library(igraph) 

network_metrics_pasc_or_cfs <- function(create_network_pasc_or_cfs,
                                        topdiagnoses_pasc_or_cfs,
                                        sampling_both) {
  # Ensure create_network_pasc_or_cfs is sorted if needed
  create_network_pasc_or_cfs <- create_network_pasc_or_cfs %>%
    arrange(condition_concept_name)
  
  # Convert create_network_pasc_or_cfs to a tbl_graph
  tdy_net <- as_tbl_graph(create_network_pasc_or_cfs, directed = FALSE)
  
  # Calculate total unique person_id count
  total_pat_count <- length(unique(sampling_both$person_id))
  
  # Ensure topdiagnoses_pasc_or_cfs has unique rows for each condition
  topdiagnoses_pasc_or_cfs_unique <- topdiagnoses_pasc_or_cfs %>%
    distinct(condition_concept_name, .keep_all = TRUE)
  
  # Join patient counts to nodes data based on condition_concept_name or equivalent
  tdy_net <- tdy_net %>%
    activate(nodes) %>%
    left_join(topdiagnoses_pasc_or_cfs_unique, by = c("name" = "condition_concept_name")) %>%
    mutate(percentage = ifelse(is.na(pat_count), 0, pat_count / total_pat_count),
           degree = centrality_degree(),
           community = group_louvain(weights = weight),
           cent = centrality_betweenness(),
           local_trans = local_transitivity(weights = weight)) %>%
    arrange(-degree) %>%
    as_tibble() %>%
    select(name, percentage, community, cent, degree, local_trans, pat_count)
  
  return(tdy_net)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.40f1d0d6-c071-484a-9d78-7c199afd7b39"),
    create_network_pasc_or_cfs_1=Input(rid="ri.foundry.main.dataset.33ab3a54-b64a-4822-961a-aad7e00c5fb1"),
    sampling_both=Input(rid="ri.foundry.main.dataset.1beae739-b571-4cd4-bc0f-4ce0cf928c70"),
    topfeatures_pasc_or_mecfs=Input(rid="ri.foundry.main.dataset.ac70f275-4b39-49a5-80b0-afe146127052")
)
library(tidygraph)
library(dplyr)
library(igraph) 

network_metrics_pasc_or_cfs_1 <- function(
                                        sampling_both, topfeatures_pasc_or_mecfs, create_network_pasc_or_cfs_1) {
  # Ensure create_network_pasc_or_cfs_1 is sorted if needed
  create_network_pasc_or_cfs <- create_network_pasc_or_cfs_1 %>%
    arrange(condition_concept_name)
  
  # Convert create_network_pasc_or_cfs to a tbl_graph
  tdy_net <- as_tbl_graph(create_network_pasc_or_cfs, directed = FALSE)
  
  # Calculate total unique person_id count
  total_pat_count <- length(unique(sampling_both$person_id))
  
  # Ensure topfeatures_pasc_or_mecfs has unique rows for each condition
  topdiagnoses_pasc_or_cfs_unique <- topfeatures_pasc_or_mecfs %>%
    distinct(condition_concept_name, .keep_all = TRUE)
  
  # Join patient counts to nodes data based on condition_concept_name or equivalent
  tdy_net <- tdy_net %>%
    activate(nodes) %>%
    left_join(topdiagnoses_pasc_or_cfs_unique, by = c("name" = "condition_concept_name")) %>%
    mutate(percentage = ifelse(is.na(pat_count), 0, pat_count / total_pat_count),
           degree = centrality_degree(),
           community = group_louvain(weights = weight),
           cent = centrality_betweenness(),
           local_trans = local_transitivity(weights = weight)) %>%
    arrange(-degree) %>%
    as_tibble() %>%
    select(name, percentage, community, cent, degree, local_trans, pat_count)
  
  return(tdy_net)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.1beae739-b571-4cd4-bc0f-4ce0cf928c70"),
    Filtered_conditions_pasc_or_cfs=Input(rid="ri.foundry.main.dataset.fdf4daf6-6ae9-40b8-8e87-5e1c98d78796")
)
library(dplyr)

sampling_both <- function(Filtered_conditions_pasc_or_cfs) {
  # Filter rows for each category
  pasc_cfs_rows <- filter(Filtered_conditions_pasc_or_cfs, cohort_label == 'PASC_CFS')
  pasc_rows <- filter(Filtered_conditions_pasc_or_cfs, cohort_label == 'PASC')
  cfs_rows <- filter(Filtered_conditions_pasc_or_cfs, cohort_label == 'CFS')
  
  # Count the unique person_ids in CFS
  num_unique_cfs_persons <- cfs_rows %>% 
    distinct(person_id) %>% 
    nrow()
  
  # Sample unique person_ids from PASC to match the number of unique person_ids in CFS
  sampled_pasc_person_ids <- pasc_rows %>% 
    distinct(person_id) %>% 
    sample_n(size = num_unique_cfs_persons) %>% 
    pull(person_id)
  
  # Filter PASC rows for the sampled person_ids
  sampled_pasc_rows <- pasc_rows %>% 
    filter(person_id %in% sampled_pasc_person_ids)
  
  # Combine PASC_CFS, CFS, and sampled PASC rows
  balanced_df <- bind_rows(pasc_cfs_rows, cfs_rows, sampled_pasc_rows)
  
  # Return the balanced dataframe
  return(balanced_df)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.428f8407-e1e9-4b49-aba0-1093008a841d"),
    network_metrics_cfs=Input(rid="ri.foundry.main.dataset.2d0b5eba-92cf-4f7a-9912-619d0f17a666")
)
library(treemap)

treemapforcommunities_cfs <- function( network_metrics_cfs) {
  my_colour_pal <- c("#004c99", "#cc0000", "#009900", "#FFC300")
  network_metrics_cfs <- network_metrics_cfs
  #%>%
  # mutate(community=if_else(community==3,"Respiratory Cluster", if_else(community==2, "Metabolic & Obesity-related Cluster", "Neurological Cluster" )))
  p <- treemap(
    network_metrics_cfs,
    index = c("community", "name"),
    vColor = "community",
    vSize = "pat_count",
    type = "manual",
    palette = my_colour_pal,
    title = "Clustering Results Based on the Louvain Algorithm: ME/CFS Only",
    fontsize.title = 14,
    border.lwds = c(7, 2),
    fontsize.labels = 10,
    
    #mapping = c(min(pat_count), 100, max(pat_count)),
    align.labels = list(c("left", "top"), c("right", "bottom")) # keep labels from overlapping with other text
  )
  print(p)
  
  return(NULL)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.3815ee7e-24b6-483b-ad4d-c204363ba403"),
    network_metrics_pasc=Input(rid="ri.foundry.main.dataset.05ae9739-033a-4af4-8c23-2b44265a3583")
)
library(treemap)

treemapforcommunities_pasc <- function(network_metrics_pasc) {
  my_colour_pal <- c("#004c99", "#cc0000", "#009900", "#FFC300")
  network_metrics_pasc <- network_metrics_pasc
  #%>%
  # mutate(community=if_else(community==3,"Respiratory Cluster", if_else(community==2, "Metabolic & Obesity-related Cluster", "Neurological Cluster" )))
  p <- treemap(
    network_metrics_pasc,
    index = c("community", "name"),
    vColor = "community",
    vSize = "pat_count",
    type = "manual",
    palette = my_colour_pal,
    title = "Clustering Results Based on the Louvain Algorithm: PASC Only",
    fontsize.title = 14,
    border.lwds = c(7, 2),
    fontsize.labels = 10,
    
    #mapping = c(min(pat_count), 100, max(pat_count)),
    align.labels = list(c("left", "top"), c("right", "bottom")) # keep labels from overlapping with other text
  )
  print(p)
  
  return(NULL)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.90389c70-fa27-4ebd-8c6b-6d6c9ee7bcdf"),
    network_metrics_pasc_cfs=Input(rid="ri.foundry.main.dataset.228c12c3-2ece-4543-b2e9-6d9a5b66b225")
)
library(treemap)

treemapforcommunities_pasc_cfs <- function(network_metrics_pasc_cfs) {
  my_colour_pal <- c("#004c99", "#cc0000", "#009900", "#FFC300")
  network_metrics_pasc_cfs <- network_metrics_pasc_cfs
  #%>%
  # mutate(community=if_else(community==3,"Respiratory Cluster", if_else(community==2, "Metabolic & Obesity-related Cluster", "Neurological Cluster" )))
  p <- treemap(
    network_metrics_pasc_cfs,
    index = c("community", "name"),
    vColor = "community",
    vSize = "pat_count",
    type = "manual",
    palette = my_colour_pal,
    title = "Clustering Results ased on the Louvain Algorithm: PASC and CFS",
    fontsize.title = 14,
    border.lwds = c(7, 2),
    fontsize.labels = 10,
    
    #mapping = c(min(pat_count), 100, max(pat_count)),
    align.labels = list(c("left", "top"), c("right", "bottom")) # keep labels from overlapping with other text
  )
  print(p)
  
  return(NULL)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.15828f05-fd63-4c63-b8d4-f6c3db9b6a2b"),
    network_metrics_pasc_or_cfs=Input(rid="ri.foundry.main.dataset.28907b00-d6de-4174-b381-dd4e834508de")
)
library(treemap)

treemapforcommunities_pasc_or_mecfs <- function(network_metrics_pasc_or_cfs) {
  my_colour_pal <- c("#004c99", "#cc0000", "#009900", "#FFC300")
  network_metrics_pasc_or_cfs <- network_metrics_pasc_or_cfs

  p <- treemap(
    network_metrics_pasc_or_cfs,
    index = c("community", "name"),
    vColor = "community",
    vSize = "pat_count",
    type = "manual",
    palette = my_colour_pal,
    title = "Clustering Results: Combined Group of PASC and/or ME/CFS",
    fontsize.title = 14,
    border.lwds = c(7, 2),
    fontsize.labels = 10,
    position.legend = "none",
    #mapping = c(min(pat_count), 100, max(pat_count)),
    align.labels = list(c("left", "top"), c("right", "bottom")) # keep labels from overlapping with other text
  )
  print(p)
  
  return(NULL)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.910addf6-20b7-4570-a1bf-9aa9be4a886e"),
    network_metrics_pasc_or_cfs_1=Input(rid="ri.foundry.main.dataset.40f1d0d6-c071-484a-9d78-7c199afd7b39")
)
library(treemap)

treemapforcommunities_pasc_or_mecfs_1 <- function(network_metrics_pasc_or_cfs_1) {
  my_colour_pal <- c("#004c99", "#cc0000", "#009900", "#FFC300")
  network_metrics_pasc_or_cfs_1 <- network_metrics_pasc_or_cfs_1

  p <- treemap(
    network_metrics_pasc_or_cfs_1,
    index = c("community", "name"),
    vColor = "community",
    vSize = "pat_count",
    type = "manual",
    palette = my_colour_pal,
    title = "Clustering Results: Combined Group of PASC and/or ME/CFS",
    fontsize.title = 14,
    border.lwds = c(7, 2),
    fontsize.labels = 10,
    position.legend = "none",
    #mapping = c(min(pat_count), 100, max(pat_count)),
    align.labels = list(c("left", "top"), c("right", "bottom")) # keep labels from overlapping with other text
  )
  print(p)
  
  return(NULL)
}

