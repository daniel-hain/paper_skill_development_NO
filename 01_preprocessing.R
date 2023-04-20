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
library(spacyr)
# spacy_install()  # Do the first time to install in a miniconda
# spacy_download_langmodel("nb") # Download norwegian model
spacy_initialize('nb_core_news_sm')

library(future.apply)
library(progressr)


###########################################################################################
########################### some functions
###########################################################################################

# Function for text preprocessing
text_preprocess <- function(x){
  {{x}} %>%
    str_to_lower() %>%
    textclean::replace_hash() %>%
    textclean::replace_tag() %>%
    textclean::replace_url() %>%
    textclean::replace_html() %>%
    str_replace_all("[^[:alpha:]]", " ") %>%
    str_squish()
}

# Function to do the lemmatization
lemmatize_spacy <- function(x, stopwords = tidytext::stop_words, vocab = NULL){
  # requires columns: doc_id, text
  x <- x %>%
    as.data.frame() %>%
    spacy_parse(lemma = TRUE, pos = FALSE, tag = FALSE, entity = FALSE, dependency = FALSE, nounphrase = FALSE) %>%
    select(doc_id, lemma) %>%
    as_tibble() %>%
    anti_join(stopwords, by = c('lemma' = 'word')) 
  
  if(!is.null(vocab)){x <- x %>% semi_join(vocab, by = c('lemma' = 'word')) }

  x %>%
    group_by(doc_id) %>% 
    summarise(text  = paste(lemma, collapse =" ")) 
}

# function to 
process_skills <-function(data){
  data %>% select(job_id, text) %>% 
    mutate(text = text %>% text_preprocess() ) %>%
    rename(doc_id = job_id) %>%
    lemmatize_spacy(stopwords = stopwords_no, vocab = skill_vocab) %>%
    rename(job_id = doc_id) %>%
    unnest_ngrams(word, text, ngram_delim = ' ', n_min = 1, n = 4) %>%
    inner_join(skill_labels %>% select(text, skill_label), by = c('word' = 'text'))
}

###########################################################################################
########################### Variable definitions
###########################################################################################

stopwords_no <- tibble(word = stopwords::stopwords("no"))

###########################################################################################
########################### Skill_list
###########################################################################################

#### Skills
data <- read_csv('../data/skills_no.csv')

data %<>% mutate(skill_id = conceptUri %>% str_remove('http://data.europa.eu/esco/skill/')) %>%
  mutate(skill_green = inScheme %>% str_detect('green'),
         skill_it = inScheme %>% str_detect('6c930acd-c104-4ece-acf7-f44fd7333036'),
         text = ifelse(is.na(altLabels), preferredLabel, paste(preferredLabel, altLabels, sep = '\n'))
  ) %>%
  rename(skill_label = preferredLabel) %>%
  select(skill_id, skill_label, text, skill_green,skill_it) %>%
  distinct(skill_id, .keep_all = TRUE) 

skill_labels <- data %>% 
  select(skill_id, text) %>%
  mutate(text = text %>% str_split('\n')) %>%
  unnest(text) %>%
  group_by(skill_id) %>%
  mutate(doc_id = paste(skill_id, 1:n(), sep = '|')) %>%
  ungroup() %>%
  mutate(text = text %>% text_preprocess() ) %>%
  lemmatize_spacy(stopwords = stopwords_no) %>%
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

# Tryout with ngrams in labels
skill_labels %<>%
  unnest_ngrams(word, text, ngram_delim = ' ', n_min = 2, n = 4)  %>%
  rename(text = word) %>%
  distinct()




#files = paste0('../data/job_postings/stillinger_', year, '_tekst.csv')
#data <- read_delim(files[1], locale = locale(encoding = "UTF-8"),delim=";")
#n = 1000; data <- data[1:n,] 

data <- read_rds('../data/job_postings/dat_text_clean.rds')
colnames(data) <- c('job_id', 'job_titel','job_text', 'year', 'prob', 'language')


year_range <- c(2002:2021)

for (i in length(year_range)) {
  
  var_year = year_range[i]
  
  # filter for year to do it sequentially
  data_year <- data %>% filter(year == var_year,
                               language == "no") %>%
    mutate(text = paste(job_titel, job_text, sep = '. ')) %>%
    select(job_id, text)
  
  # multicore processing
  cores= parallel::detectCores() - 1
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
  
  job_skills %<>% mutate(year = var_year)
  job_skills %>% saveRDS(paste0('../temp/list_skills_', var_year, '_ngrams.rds'))
  rm(dfs, split_corpus)
  
}


spacy_finalize()

# Some basic data adaptation and cleaning 

#  data %<>% 
#  mutate(job_id = job_id %>% as.character()) %>%
#  # correct that sometimes stillingsnummer and id are switched
#  mutate(prob = ifelse(grepl( x = id, pattern = "-") == T, 1, 0),
#         id1 = ifelse(prob == 0, job_id, id),
#         job_id = ifelse(prob == 0, id, job_id),
#         id = id1) %>% 
#  # merged text field
#  mutate(text = paste(job_titel, job_text, sep = '. ')) %>%
#  drop_na(job_id, text) %>%
#  select(job_id, text) %>%
#  distinct(job_id, .keep_all = TRUE)
