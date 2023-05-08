###########################################################################################
########################### Preamble
###########################################################################################

### Generic preamble
rm(list=ls(all.names = TRUE)); gc()
set.seed(1337)

library(tidyverse)
library(data.table)
library(magrittr)
library(arrow)
library(Matrix)

# multicore
library(future.apply)
library(progressr)

# load functions
source('XX_functions.R')

###########################################################################################
########################### skills
###########################################################################################

file_list <- list.files(path = '../temp/embeddings/', pattern = 'skill_embeddings.*parquet', full.names = TRUE)
skill_embeddings <- read_parquet(file_list[1])
colnames(skill_embeddings) <- c(paste0('V_', 1:(ncol(skill_embeddings)-1)), 'skill_id')

skill_embeddings %<>% 
  relocate(skill_id) 

mat_skill <- skill_embeddings %>%
  column_to_rownames(var = "skill_id") %>%
  data.matrix()

rm(skill_embeddings)

###########################################################################################
########################### jobs
###########################################################################################

calc_similarity <- function(x){ # , y, cutoff = 0.5
  cutoff = 0.6
  x <- x %>% text2vec::sim2(mat_skill, method = "cosine", norm = "l2")
  x[x <= cutoff] <- 0
  x <- Matrix(x, sparse = TRUE) 
  el = Matrix::summary(x) %>% data.frame()
  
  #  sim_el <- tibble(job_id = rownames(x)[el$i], skill_id = colnames(x)[el$j], sim = el$x)%>%
  #    mutate(job_id = job_id %>% str_remove('\\|.*')) %>%
  #    group_by(job_id, skill_id) %>%
  #    summarise(sim = max(sim)) %>%
  #    ungroup()
  
  # Convert data.frame to data.table
  sim_el <- setDT(el)
  
  # Equivalent of tidyr::separate
  sim_el[, c("job_id", "suffix") := tstrsplit(rownames(x)[i], "\\|", fixed = TRUE)]
  sim_el[, skill_id := colnames(x)[j]]
  
  # Equivalent of dplyr::group_by and dplyr::summarise
  sim_el <- sim_el[, .(sim = max(x)), by = .(job_id, skill_id)]
  
  rm(x, el)
  gc()
  return(sim_el)
}

### Load files

file_list <- list.files(path = '../temp/embeddings/', pattern = 'job_embeddings.*parquet', full.names = TRUE)


for(i in 1:length(file_list)){
  # i = 1
  
  job_embeddings <- read_parquet(file_list[1])
  colnames(job_embeddings) <- c(paste0('V_', 1:(ncol(job_embeddings)-1)), 'job_id')
  
  job_embeddings %<>% 
    group_by(job_id) %>%
    mutate(job_id2 = paste(job_id, 1:n(), sep = '|')) %>%
    ungroup() %>%
    relocate(job_id, job_id2) %>%
    nest(.by = job_id)
  
  n_iter = 100
  split_corpus <- split(job_embeddings, seq(1, nrow(job_embeddings), by = n_iter))
  split_corpus <- split_corpus %>%
    lapply(unnest, cols = everything()) %>%
    lapply(select, -job_id) %>%
    lapply(column_to_rownames, var = "job_id2") %>%
    lapply(data.matrix)
  
  rm(job_embeddings)
  
  cores= parallel::detectCores()
  handlers(global = TRUE); handlers("progress")
  
  gc()
  plan(multisession, workers = cores - 1) 
  with_progress({
    p <- progressor(steps = length(split_corpus))
    dfs <- future_lapply(split_corpus,future.seed=TRUE, FUN=function(x) 
    {
      p()
      Sys.sleep(.1)
      calc_similarity(x)
    })
  })
  
  dfs <- data.table::rbindlist(dfs) %>% setDF() 
  
  dfs %>% saveRDS(paste0('../temp/skills/list_skills_sbert_', i, '.rds'))
  rm(dfs, split_corpus)
  gc()
}