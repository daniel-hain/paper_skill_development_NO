###########################################################################################
########################### Preamble
###########################################################################################

### Generic preamble
rm(list=ls())
set.seed(1337)

library(tidyverse)
library(magrittr)

# multicore
library(future.apply)
library(progressr)

# NLP
library(tidytext)
library(udpipe)

# UD models
#udpipe_download_model(language = "norwegian-bokmaal", model_dir ='../data/models', 
#                      udpipe_model_repo = "jwijffels/udpipe.models.ud.2.4", overwrite = TRUE)
#udpipe_download_model(language = "english", model_dir ='../data/models', 
#                      udpipe_model_repo = "jwijffels/udpipe.models.ud.2.4", overwrite = TRUE)
udmodel_no <- udpipe_load_model(file = '../data/models/norwegian-bokmaal-ud-2.4-190531.udpipe')
udmodel_en <- udpipe_load_model(file = '../data/models/english-ewt-ud-2.4-190531.udpipe')

# load functions
source('XX_functions.R')

###########################################################################################
########################### Skill_list
###########################################################################################

stopwords_no <- tibble(word = stopwords::stopwords("no"))

#### Skills
data <- read_csv('../data/skills_no.csv')

# Some preprocessing
data %<>% mutate(skill_id = conceptUri %>% str_remove('http://data.europa.eu/esco/skill/')) %>%
  mutate(skill_green = inScheme %>% str_detect('green'),
         skill_it = inScheme %>% str_detect('6c930acd-c104-4ece-acf7-f44fd7333036'),
         text = ifelse(is.na(altLabels), preferredLabel, paste(preferredLabel, altLabels, sep = '\n'))
  ) %>%
  rename(skill_label = preferredLabel) %>%
  select(skill_id, skill_label, text, skill_green,skill_it) %>%
  distinct(skill_id, .keep_all = TRUE) 

# Extract skill s
skill_labels <- data %>% 
  select(skill_id, text) %>%
  mutate(text = text %>% str_split('\n')) %>%
  unnest(text) %>%
  group_by(skill_id) %>%
  mutate(doc_id = paste(skill_id, 1:n(), sep = '|')) %>%
  ungroup() %>%
  mutate(text = text %>% text_preprocess()) %>%
  text_lemmatize(stopwords = stopwords_no) %>%
  mutate(skill_id = doc_id %>% str_remove('\\|.*')) %>%
  filter(text %>% str_count("\\S+") <= 4) %>%
  distinct(skill_id, text, .keep_all = TRUE) %>%
  left_join(data %>% select(-text), by = 'skill_id') %>%
  select(skill_id, skill_label, text, skill_green, skill_it)

skill_vocab <- skill_labels %>%
  select(text) %>% 
  unnest_tokens(word, text) %>%
  distinct(word)

skill_labels %>% saveRDS('../temp/skill_labels.rds')
skill_vocab %>% saveRDS('../temp/skill_vocab.rds')

rm(data)

###########################################################################################
########################### Job postings
###########################################################################################

skill_labels <- read_rds('../temp/skill_labels.rds')
skill_vocab <- read_rds('../temp/skill_vocab.rds')


data <- read_rds('../data/job_postings_all.rds')
# colnames(data) <- c('job_id', 'job_titel','job_text', 'year', 'prob', 'language')


year_range <- c(2002:2021)

for (i in c(1:length(year_range))) {
  
  stopwords_no <- tibble(word = stopwords::stopwords("no"))
  # udmodel_no <- udpipe_load_model(file = '../data/models/norwegian-bokmaal-ud-2.4-190531.udpipe')
  
  var_year = year_range[i]
  
  print(paste0('::::: Starting annotating year', var_year, '::::::::'))
  
  # filter for year to do it sequentially
  data_year <- data %>% filter(year == var_year, 
                               language == "no") %>%
    mutate(text = ifelse(is.na(job_title), job_text, paste(job_title, job_text, sep = '. '))) %>%
    select(job_id, text)
  
  # multicore processing
  cores= parallel::detectCores()
  n_iter = 1000
  
  handlers(global = TRUE)
  handlers("progress")
  split_corpus <- split(data_year, seq(1, nrow(data_year), by = n_iter))
  
  plan(multisession, workers = cores - 1) 
  with_progress({
    p <- progressor(steps = length(split_corpus))
    dfs <- future_lapply(split_corpus,future.seed=TRUE, FUN=function(x) 
    {
      p()
      Sys.sleep(.1)
      process_skills(x)
    })
  })
  
  job_skills <- data.table::rbindlist(dfs)
  
  job_skills %<>% mutate(year = var_year) %>%
    distinct(job_id, skill_label, word, .keep_all = TRUE)
  
  job_skills %>% saveRDS(paste0('../temp/list_skills_', var_year, '.rds'))
  rm(dfs, split_corpus)
  
  print(paste0('::::: Finished annotating year', var_year, '::::::::'))
}

# Save them all
file_list <- list.files(path = '../temp/', pattern = 'list_skills_.....rds', full.names = TRUE)
data <- tibble()
for (i in c(1:length(file_list))) { data %<>% bind_rows(read_rds(file_list[i]))}

data %>% saveRDS('../temp/list_complete_skills.rds')
rm(data)


###########################################################################################
########################### META DATA
###########################################################################################

#TODO:Update for all years

file_list <- list.files(path = '../data/job_meta', pattern = 'ledige_stillinger.*.csv', full.names = TRUE)

data <- file_list[1] %>% read_csv2()
colnames(data) <- data %>% colnames() %>% str_to_lower() %>% str_replace_all(' ', '_')

data %<>%
  select(stilling_id, yrke, yrke_grovgruppe, arbeidssted_kommune)

colnames(data) <- c('job_id', 'occupation_name', 'occupation_group', 'workplace_komune')

data %>% saveRDS('../data/job_meta/job_meta_all.rds')


###########################################################################################
########################### Text export for Embedding creation
###########################################################################################

# Export skill text

skills <- read_csv('../data/skills_no.csv')
skills_export <- skills %<>% 
  mutate(skill_id = conceptUri %>% str_remove('http://data.europa.eu/esco/skill/')) %>%
  select(skill_id, description) %>%
  rename(text = description)

skills_export %>% write_csv('../temp/text_skills_plain.csv')

# Export job påosting text

clean_data <- function(x){
  # BRief text clean function
  {{x}} %>%
    mutate(job_title = job_title %>% text_preprocess_1(),
           job_text = job_text %>% text_preprocess_1() ) %>%
    mutate(language = job_text %>% cld2::detect_language())
}

year_range <- c(2002:2021)
data_merged <- tibble()

for (i in c(1:length(year_range))) {
  #i= 20
  year_i <- year_range[i]
  data <- read_csv2(paste0('../data/job_postings/stillinger_', year_i, '_tekst.csv'))
  
  # Special cases for all the years
  if(year_i == 2019){x <- tibble(job_id = data[,1] %>% pull(), id = NA, job_title = data[,3] %>% pull(), job_text = data[,4] %>% pull()); data <- x; rm(x) }
  if(year_i == 2020){x <- tibble(job_id = data[,2] %>% pull(), id = NA, job_title = data[,3] %>% pull(), job_text = data[,4] %>% pull()); data <- x; rm(x) }
  if(year_i == 2021){x <- tibble(job_id = data[,1] %>% pull(), id = NA, job_title = NA, job_text = data[,4] %>% pull()); data <- x; rm(x) }
  
  colnames(data) <- c('job_id', 'id', 'job_title','job_text')
  
  # multicore processing
  cores= parallel::detectCores()
  n_iter = 10000
  
  handlers(global = TRUE)
  handlers("progress")
  split_corpus <- split(data, seq(1, nrow(data), by = n_iter))
  
  plan(multisession, workers = cores - 1) 
  with_progress({
    p <- progressor(steps = length(split_corpus))
    dfs <- future_lapply(split_corpus,future.seed=TRUE, FUN=function(x) 
    {
      p()
      Sys.sleep(.1)
      clean_data(x)
    })
  })
  
  data <- data.table::rbindlist(dfs); rm(dfs, split_corpus)
  
  data %<>%
    mutate(year = year_i) %>%
    filter(language %in% c('en', 'no')) %>%
    select(job_id, job_title, job_text, year, language) %>%
    drop_na(job_id, job_text, language)
  
  data_merged %<>% bind_rows(data)
  
  print(paste0('::::: Year ', year_i, ' finished ::::::'))
}

#
data_merged %>% write_rds('../data/job_postings_all.rds')
rm(clean_data)

data_merged <- read_rds('../data/job_postings_all.rds')

# get some text out for testing embeddings
data_export <-  data_merged %>% 
  select(job_id, job_text) %>%
  mutate(job_text = job_text %>% str_split(pattern = '\\.')) %>%
  unnest(job_text) %>%
  mutate(job_text = job_text %>% str_squish()) %>%
  rename(text = job_text) %>%
  filter(str_length(text) >20) %>%
  drop_na()

data_export %>% write_csv('../temp/text_job_plain.csv')
data_export %>% arrow::write_feather('../temp/text_job_plain.arrow') # For excange with python

