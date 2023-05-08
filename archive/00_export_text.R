###########################################################################################
########################### Preamble
###########################################################################################

### Generic preamble
rm(list=ls())
set.seed(1337)

library(tidyverse)
library(magrittr)

# NLP
library(tidytext)

source('XX_functions.R')

###########################################################################################
########################### Skill_list
###########################################################################################

data <- read_rds('../data/dat_text_clean.rds') # TODO
colnames(data) <- c('job_id', 'job_titel','job_text', 'year', 'prob', 'language')

# det some text out for testing embeddings
data_export <- data %>% 
  filter(year == 2021) %>%
  select(job_id, job_text) %>%
  mutate(job_text = job_text %>% str_replace_all('[:space:]*[:punct:]', '.') %>% str_split(pattern = '\\.')) %>%
  unnest(job_text) %>%
  mutate(job_text = job_text %>% str_squish()) %>%
  rename(text = job_text) %>%
  filter(str_length(text) >20)
           
  # unnest_tokens(output = text, input = job_text, token = "sentences") 

data_export %>% write_csv('../temp/text_job_plain.csv')

skills <- read_csv('../data/skills_no.csv')
skills_export <- skills %<>% 
  mutate(skill_id = conceptUri %>% str_remove('http://data.europa.eu/esco/skill/')) %>%
  select(skill_id, description) %>%
  rename(text = description)

skills_export %>% write_csv('../temp/text_skills_plain.csv')
  