###########################################################################################
########################### NLP preprocessing
###########################################################################################

text_preprocess_1 <- function(x){
  # Function for minimal text preprocessing
  {{x}} %>%
    textclean::replace_hash() %>%
    textclean::replace_tag() %>%
    textclean::replace_url() %>%
    textclean::replace_html() %>%
    str_squish()
}

text_preprocess_2 <- function(x){
  # Function for adittional text preprocessing for string matching
  {{x}} %>%
    str_to_lower() %>%
    str_replace_all("[^[:print:]]", "") %>%
    str_replace_all("[:punct:]", " ") %>%
    str_squish()
}

text_preprocess <- function(x){
  # Wraqpper around both
  {{x}} %>%
    text_preprocess_1() %>%
    text_preprocess_2() 
}

text_lemmatize <- function(x, stopwords = tidytext::stop_words, vocab = NULL){
  # Function to do the lemmatization
  # requires columns: doc_id, text
  x <- udpipe_annotate(object = udmodel_no, x = x$text, doc_id = x$doc_id, tagger = "default", parser = "none") %>%
    #spacy_parse(lemma = TRUE, pos = FALSE, tag = FALSE, entity = FALSE, dependency = FALSE, nounphrase = FALSE) %>% # Note: Dont use spacy anymore
    as.data.frame() %>% 
    select(doc_id, lemma) %>%
    as_tibble() %>%
    anti_join(stopwords, by = c('lemma' = 'word')) 
  
  if(!is.null(vocab)){x <- x %>% semi_join(vocab, by = c('lemma' = 'word')) }
  
  x %>%
    group_by(doc_id) %>% 
    summarise(text  = paste(lemma, collapse =" ")) 
}

process_skills <-function(data){
  # function to wrapp complete preprocessing pipeline
  data %>% select(job_id, text) %>% 
    mutate(text = text %>% text_preprocess_2() ) %>%
    rename(doc_id = job_id) %>%
    text_lemmatize(stopwords = stopwords_no, vocab = skill_vocab) %>%
    rename(job_id = doc_id) %>%
    unnest_ngrams(word, text, ngram_delim = ' ', n_min = 1, n = 4) %>%
    inner_join(skill_labels %>% select(text, skill_label), by = c('word' = 'text'))
}

