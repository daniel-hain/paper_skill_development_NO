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


###########################################################################################
########################### Variable definitions
###########################################################################################

rm(list=ls())

# Load stilling descriptions ----
year = 2018
files = paste0('../data/stillinger_', year, '_tekst.csv')

data <- read_delim(files[1], locale = locale(encoding = "UTF-8"),delim=";")
data <- data[1:10000,] 

colnames(data) <- c('id','job_id', 'job_titel','job_text')

# Some basic data adaptation and cleaning ----

data %<>% 
  mutate(job_id = job_id %>% as.character(),
         year = year) %>%
  # correct that sometimes stillingsnummer and id are switched
  mutate(prob = ifelse(grepl( x = id, pattern = "-") == T, 1, 0),
         id1 = ifelse(prob == 0, job_id, id),
         job_id = ifelse(prob == 0, id, job_id),
         id = id1) %>% 
  # merged text field
  mutate(text = paste(job_titel, job_text, sep = '. ')) %>%
  drop_na(job_id, text) %>%
  select(job_id, year, text) %>%
  distinct(job_id, .keep_all = TRUE)
  
 

# Remove non-norwegian languages ----
data %<>% mutate(language = text %>% cld2::detect_language())

data %<>% filter(language == "no") %>%
  select(-language)


# Function for text preprocessing
text_preprocess <- function(x){
  {{x}} %>%
    str_to_lower() %>%
    textclean::replace_hash() %>%
    textclean::replace_tag() %>%
    textclean::replace_url() %>%
    textclean::replace_html() %>%
    # textclean::replace_non_ascii() %>%
    str_replace_all("[^[:alpha:]\\.\\,\\s']", "") %>%
    tm::removeWords(stopwords::stopwords("no")) %>%
    str_squish()
}

text_tidy <- data %>%
  select(job_id, text) %>%
  mutate(text = text %>% text_preprocess())  %>% 
  unnest_ngrams(term, text, ngram_delim = ' ', n_min = 1, n = 4) 


#### Skills

data <- read_csv('../data/skills_no.csv')

data %<>% mutate(skill_id = conceptUri %>% str_remove('http://data.europa.eu/esco/skill/')) %>%
  mutate(skill_green = inScheme %>% str_detect('green'),
         skill_it = inScheme %>% str_detect('6c930acd-c104-4ece-acf7-f44fd7333036'),
         text = ifelse(is.na(altLabels), preferredLabel, paste(preferredLabel, altLabels, sep = '\n'))
         ) %>%
  rename(skill_label = preferredLabel) %>%
  select(skill_id, skill_label, text, skill_green,skill_it)

data %<>% 
  mutate(text = text %>% str_split('\n')) %>%
  unnest(text) 

data %<>% 
  mutate(text = text %>% text_preprocess())  %>% 
  filter(text %>% str_count("\\S+") <= 4) %>%
  distinct(skill_id, skill_label, .keep_all = TRUE)


data %>% pull(skill_label) %>% n_distinct()
  
  
test <- data %>% 
  semi_join(text_tidy, by = c('text' = 'term'))


test <- text_tidy %>% 
  inner_join(data, by = c('term' = 'text'))

test %>% pull(job_id) %>% n_distinct()

test %>%
  #filter(skill_green == TRUE) %>%
  distinct(job_id, skill_label) %>%
  count(skill_label, sort = TRUE) %>%
  print(n = 50)


test <- text_tidy %>% 
  anti_join(data, by = c('term' = 'text'))

test %>% count(term, sort = TRUE)
#### Glove embeddings

