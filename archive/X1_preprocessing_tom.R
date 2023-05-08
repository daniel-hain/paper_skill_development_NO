library(tidyverse)
library(data.table)
library(tidytext)
library(openxlsx)
library(textclean)
library(stringi)
library(stringr)
library(cld2)
library(ggpubr)
library(stringr)
library(udpipe)
library(data.table)
library(future.apply)
library(progressr)

library(udpipe)
udmodel_no <- udpipe_download_model(language = "norwegian-bokmaal")
udmodel_no %>% saveRDS('../data/udmodel_no.rds')

udmodel <- udpipe_load_model(file = '../data/udmodel_no.rds')


# Load stilling descriptions ----
files = list.files(path = '../Original data/csv/stillinger_tekst_data')
files<-paste('../Original data/csv/stillinger_tekst_data/',files,sep="")
data <- read_delim(files[1], locale = locale(encoding = "UTF-8"),delim=";")

colnames(data) <- gsub(pattern=" ",replacement = "_",colnames(data))
data <- data %>% mutate(Stilling_Id=as.character(Stilling_Id),
                        year_post="2001")

for(i in 2:length(files))
{
  print(i)
  help <- read_delim(files[i], locale = locale(encoding = "UTF-8"), delim=";")
  colnames(help)<-gsub(pattern=" ",replacement = "_",colnames(help))
  if(files[i]=="../Original data/csv/stillinger_tekst_data/stillinger_2021_01_06_uten_FINN.csv" | files[i]=="../Original data/csv/stillinger_tekst_data/stillinger_2021_07_12_uten_FINN.csv")
  {
    help <- help %>% rename(Stilling_Id=Lk_Stilling_Uuid)
    help <- help %>% select(-Sensitiv_Status,-Stilling_kilde_kode)
  }
  help <- help %>% mutate(Stilling_Id=as.character(Stilling_Id))
  help$year_post<-substr(files[i],55,58)
  data <- bind_rows(data,help)
}

colnames(data) <- c('Stillingsnummer_nav_no','Stilling.id', 'jobb_tittel','text',"year_org")
rm(help)

data <- data %>% mutate(prob=ifelse(grepl(x=Stillingsnummer_nav_no,pattern="-")==T & is.na(Stillingsnummer_nav_no)==T,1,0))
data <- data %>% mutate(Stilling.id =ifelse(prob==0,Stillingsnummer_nav_no,Stilling.id))
data <- data %>% select(-Stillingsnummer_nav_no)
data <- data %>% filter(is.na(Stilling.id)==F)

# Remove non-norwegian languages ----
data <- data %>% mutate(language=detect_language(text))
languages <- data %>% count(language) %>% ungroup()
#write.xlsx(languages,file="dist_languages_vacc.xlsx",overwrite=T)
data <- data %>% filter(language=="no")

#Clean the data
data$text <- data$text %>%
  as_vector() %>%
  replace_hash() %>%
  replace_html() %>%
  replace_non_ascii() %>%
  replace_symbol() %>%
  replace_tag() %>%
  replace_url() %>%
  replace_white() %>%
  str_replace_all("[^[:alnum:]\\.\\,']", " ")
  
data <- data %>% mutate(text=str_squish(text))

data %>% saveRDS(file="../Data/dat_text_clean.rds")

#lemmazation and NLP
data <- readRDS("../Data/dat_text_clean.rds")
dat.text <- data.frame(doc_id=data$stillingsnummer,text=data$text) %>% distinct()

handlers(global = TRUE)
handlers("progress")
split_corpus <- split(dat.text, seq(1, nrow(dat.text), by = 100))

plan(multisession, workers = 7L) 
with_progress({
  p <- progressor(steps = length(split_corpus))
  dfs <- future_lapply(split_corpus,future.seed=TRUE, FUN=function(x) 
    {
    p()
    Sys.sleep(.2)
    udpipe(x,"norwegian-bokmaal",model_dir=getwd(),udpipe_model_repo = "jwijffels/udpipe.models.ud.2.4")
    })
})
dfs_df<-rbindlist(dfs)
saveRDS(dfs_df,file="../Data/dfs_df.rds")

#Clean results
dfs_df <- readRDS("../Data/dfs_df.rds")
dfs_df <- dfs_df %>% mutate(lemma=ifelse(substr(lemma,nchar(lemma),nchar(lemma))==".",substr(lemma,1,nchar(lemma)-1),lemma))

probs <- dfs_df %>% filter(is.na(upos)==T) #none
words <- dfs_df %>% select(lemma) %>% mutate(lemma=tolower(lemma)) %>% add_count(lemma) %>% distinct() %>% arrange(desc(n))
words.100 <- words %>% filter(n>10)
words.100 <- words %>% mutate(rank=1:n())

lm(log10(n) ~ log10(rank), data = words.100)

g <- words.100 %>% ggplot(aes(x=rank,y=n))+geom_point(size=.5,alpha=0.8) + 
      geom_abline(intercept = 9.996, slope = -1.677, color = "gray50", linetype = 2) +
      scale_x_log10() + scale_y_log10()+ 
  labs(title="Distribution of words in Norwegian job posts",x="Word rank (axis in logs)",y="Number of occurrences (axis in logs)")+
  theme_classic()
pdf("Rank_size.pdf")
g
dev.off()

#ICT n-gram matching

dfs_df <- readRDS("../Data/dfs_df.rds")
#dfs_df <- dfs_df %>% mutate(lemma_lower=tolower(lemma))

#upper and lower case is an issue
#gis <- dfs_df %>% filter(token=="GIS")

#https://bnosac.github.io/udpipe/docs/doc2.html

# Load ict phrases
ict <- read_delim("../Original data/ESCO/ESCO dataset - v1.1.1 - classification - no - csv/digitalSkillsWithNewLabels_no.csv",delim=",")
ict <- ict %>% slice(-nrow(ict))
ict <- ict %>% mutate(skill_id=1:n())

ict.dat <- data.frame(doc_id=ict$skill_id,text=ict$preferredLabel)
#ict.dat$text <- gsub("[^[:alnum:]\\.\\s]", " ",ict.dat$text)
ict.dat$text <- ict.dat$text %>%  as_vector() %>%
  replace_hash() %>%
  replace_html() %>%
  replace_non_ascii() %>%
  replace_symbol() %>%
  replace_tag() %>%
  replace_url() %>%
  replace_white() %>%
  str_replace_all("[^[:alnum:]\\.\\,']", " ")
ict.dat <- ict.dat %>% mutate(text=str_squish(text))

#Upper and lower case lever
dfs_df <- dfs_df %>% mutate(num.capital = str_count(token, "[A-Z]"))
dfs_df <- dfs_df %>% mutate(lemma=ifelse((num.capital==nchar(token)) & (num.capital<4), toupper(lemma), tolower(lemma)))

ict.1gram <- udpipe(ict.dat,"norwegian-bokmaal",model_dir=getwd(),udpipe_model_repo = "jwijffels/udpipe.models.ud.2.4")
ict.1gram <- ict.1gram %>% mutate(num.capital = str_count(token, "[A-Z]"))
ict.1gram <- ict.1gram %>% mutate(lemma=ifelse((num.capital==nchar(token)) & (num.capital<4),toupper(lemma),tolower(lemma)))
ict.1gram <- ict.1gram %>% select(doc_id,lemma)
ict.1gram <- ict.1gram %>% add_count(doc_id)

#Recombine cleaned ict 1-grams to full sentences again
ict.gram <- ict.1gram %>% group_by(doc_id) %>% summarize(cleaned_tokens = str_flatten(lemma, " "))
ict.1gram <- ict.1gram %>% filter(n==1)
ict.1gram <- ict.1gram %>% rename(ict_id = doc_id)
ict.1gram <- ict.1gram %>% filter(nchar(lemma)>1)
ict.1gram <- ict.1gram %>% distinct(lemma,.keep_all = T)

dfs_df <- left_join(dfs_df,ict.1gram,by=c("lemma"="lemma"))
#found <- dfs_df %>% filter(is.na(ict_id)==F) %>% select(doc_id,lemma) %>% distinct()
#what.found <- found %>% count(lemma) %>% arrange(desc(n))
found_1grams <- dfs_df %>% filter(is.na(ict_id)==F)
found_1grams %>% saveRDS("found_1grams_ict.rds")

#Recombine vaccancy data to sentences.
dfs_df_sent <- dfs_df %>% select(doc_id, sentence_id, lemma) %>% group_by(doc_id,sentence_id) %>% summarize(cleaned_tokens = str_flatten(lemma, " "))
dfs_df_sent %>% saveRDS("../Data/dfs_df_sent.rds")
#ict in sentences
#ict.gram

#bigram matching
dfs_df_bigrams <- dfs_df_sent %>% unnest_tokens(output=bigram, input=cleaned_tokens, token = "ngrams",n=2,n_min=2)
ict.1gram <- udpipe(ict.dat,"norwegian-bokmaal", model_dir=getwd(),udpipe_model_repo = "jwijffels/udpipe.models.ud.2.4")
ict.1gram <- ict.1gram %>% mutate(num.capital = str_count(token, "[A-Z]"))
ict.1gram <- ict.1gram %>% mutate(lemma=ifelse((num.capital==nchar(token)) & (num.capital<4),toupper(lemma),tolower(lemma)))
ict.1gram <- ict.1gram %>% select(doc_id,lemma)
ict.1gram <- ict.1gram %>% add_count(doc_id)
ict.2gram <- ict.1gram %>% filter(n==2)
ict.2gram <- ict.2gram %>% group_by(doc_id) %>% summarize(cleaned_tokens = str_flatten(lemma, " "))
ict.2gram <- ict.2gram %>% rename(ict_id = doc_id)
dfs_df_bigrams <- left_join(dfs_df_bigrams,ict.2gram,by=c("bigram"="cleaned_tokens"),na_matches="never", multiple="all")
found_bigrams <- dfs_df_bigrams %>% filter(is.na(ict_id)==F)
found_bigrams %>% saveRDS("found_2grams_ict.rds")

#found_bigrams <- readRDS("found_bigrams_ict.rds")

#trigram matching
dfs_df_trigrams <- dfs_df_sent %>% unnest_tokens(output=trigram, input=cleaned_tokens, token = "ngrams",n=3,n_min=3)
ict.3gram <- ict.1gram %>% filter(n==3)
ict.3gram <- ict.3gram %>% group_by(doc_id) %>% summarize(cleaned_tokens = str_flatten(lemma, " "))
ict.3gram <- ict.3gram %>% rename(ict_id = doc_id)
dfs_df_trigrams <- left_join(dfs_df_trigrams,ict.3gram,by=c("trigram"="cleaned_tokens"),na_matches="never", multiple="all")
found_trigrams <- dfs_df_trigrams %>% filter(is.na(ict_id)==F)
found_trigrams %>% saveRDS("found_3grams_ict.rds")

dfs_df_sent <- readRDS("../Data/dfs_df_sent.rds")

#fourgram matching
dfs_df_fourgrams <- dfs_df_sent %>% unnest_tokens(output=fourgram, input=cleaned_tokens, token = "ngrams",n=4,n_min=4)
ict.4gram <- ict.1gram %>% filter(n==4)
ict.4gram <- ict.4gram %>% group_by(doc_id) %>% summarize(cleaned_tokens = str_flatten(lemma, " "))
ict.4gram <- ict.4gram %>% rename(ict_id = doc_id)
dfs_df_fourgrams <- left_join(dfs_df_fourgrams,ict.4gram,by=c("fourgram"="cleaned_tokens"),na_matches="never", multiple="all")
found_fourgrams <- dfs_df_fourgrams %>% filter(is.na(ict_id)==F)
found_fourgrams %>% saveRDS("found_4grams_ict.rds")


dfs_df_5grams <- dfs_df_sent %>% unnest_tokens(output=fivegram, input=cleaned_tokens, token = "ngrams",n=5,n_min=5)
ict.5gram <- ict.1gram %>% filter(n==5)
ict.5gram <- ict.5gram %>% group_by(doc_id) %>% summarize(cleaned_tokens = str_flatten(lemma, " "))
ict.5gram <- ict.5gram %>% rename(ict_id = doc_id)
dfs_df_5grams <- left_join(dfs_df_5grams,ict.5gram,by=c("fivegram"="cleaned_tokens"),na_matches="never", multiple="all")
found_5grams <- dfs_df_5grams %>% filter(is.na(ict_id)==F)
found_5grams %>% saveRDS("found_5grams_ict.rds")

#no six gram matching

#compile full list of identified tokens
found.grams <- bind_rows(readRDS("found_1grams_ict.rds"),readRDS("found_2grams_ict.rds"),readRDS("found_3grams_ict.rds"))

found.grams <- found.grams %>% select(doc_id,ict_id) %>% mutate(ict_id=as.numeric(ict_id))
found.grams <- full_join(found.grams,ict,by=c("ict_id"="skill_id"))

not.found <- found.grams %>% filter(is.na(doc_id)==T)
found.grams <- found.grams %>% filter(is.na(doc_id)==F)
write.xlsx(not.found,file="not_found.xlsx")
length(unique(found.grams$ict_id))

overview.found <- found.grams %>% group_by(preferredLabel) %>% summarise(num.pos=n_distinct(doc_id)) %>% ungroup() %>% arrange(desc(num.pos))
write.xlsx(overview.found,file="found.ict.xlsx")

digi.found <- read.xlsx("../Help_data/found.ict_adapted.xlsx")

found.grams <- left_join(found.grams,digi.found,by="preferredLabel",na_matches="never", multiple="all")
found.grams <- found.grams %>% filter(out==0) %>% distinct()
saveRDS(found.grams,file="found_ict.rds")


#### Put together original information on stillinger with lemmatized text #### 

data <- readRDS("../Data/dfs_df_sent.rds")
data <- data %>% arrange(doc_id, sentence_id)
data <- data %>% group_by(doc_id) %>% summarise(text=paste(cleaned_tokens,collapse = " ")) %>% ungroup()

# Remove address data ----
data <- data %>% mutate(Arbeidssted = str_locate(string=text,pattern="arbeidssted")[,1],
                        sted = str_locate(string=text,pattern="sted ")[,1],
                        address = ifelse(is.na(Arbeidssted)==F,Arbeidssted,sted),
                        text.length=nchar(text),
                        pos.address=address/text.length)

g <- ggdensity(data, x = "pos.address",y="..count..", 
          fill = "#0073C2FF", color = "#0073C2FF",
          add = "mean", rug = TRUE)+
          scale_x_continuous(labels = scales::percent) +
          labs(title="Position with respect to text length",y="Frequency",x="Position in text") 

pdf("../Visualisations/Location_of_working_place_in_text.pdf")
print(g)
dev.off()

library(stringr)
data <- data %>% mutate(text = ifelse(pos.address>0.75 & is.na(address)==F,substr(text,1,(address-1)),text))
digi <- readRDS("../Data/found_ict.rds") %>% filter(out==0)

test <- str_locate(digi$preferredLabel,pattern=" ") %>% data.frame()
digi_mult <- digi %>% slice(which(is.na(test$start)==F)) %>% select(preferredLabel) %>% distinct()
digi_mult <- digi_mult %>% mutate(preferredLabel_n = gsub(" ","_",preferredLabel))

library(stringi)
data$text <- stri_replace_all_regex(data$text,
                                  pattern=digi_mult$preferredLabel,
                                  replacement=digi_mult$preferredLabel_n,
                                  vectorize=FALSE)

# # Match to digitalization terms ----

#### Tokenization ----

tidydata <- data %>% unnest_tokens(output=word, input=text, token = "words")

#replace lower cases tokens with digi terms (upper case letters)
digi <- digi %>% mutate(preferredLabel_n = gsub(" ","_",preferredLabel),
                        preferredLabel_lower = tolower(preferredLabel_n))

digi <- digi %>% select(doc_id,preferredLabel_lower,preferredLabel_n)

tidydata <- left_join(tidydata,digi,by=c("doc_id","word"="preferredLabel_lower"), na_matches="never",multiple="all")

tidydata <- tidydata %>% mutate(word=str_trim(word))
saveRDS(tidydata,file="tidydata_lemma.rds",compress = T)


tidydata <- readRDS("../../Occupation_data/Data/tidydata_lemma.rds")
#pos <- match(data$doc_id,tidydata$doc_id,nomatch = 0)
#f <- which(pos==0)
#f

#### Clean tokens ----
#remove all special characters and punctuation except for underscore
#tidydata <- readRDS("tidydata.rds")

# remove locations ----
locations <- read_delim("../Help_data/kommunenummer-alle.csv",delim=";")
locations <- locations %>% mutate(Description=tolower(Description)) %>% pull(Description) %>% unique()
tidydata <- tidydata %>% filter(!word %in% locations)
tokens <- tidydata %>% select(word) %>% distinct()
tokens <- tokens %>% mutate(location=ifelse(str_detect(string=tolower(word), 
                                                       pattern="veie|brautene|skogen|tunet|krossen|gata|vegen|bakken|stranda|krysset|gate|vågen|vannet|allé|terrasse|holm")==T,1,0))
places <- tokens %>% filter(location==1) 
places %>% write.xlsx(file="places.xlsx",overwrite = T)

tidydata <- left_join(tidydata, places,by="word")
tidydata <- tidydata %>% filter(is.na(location)==T)
tidydata <- tidydata %>% select(-location)


#Load ok_reg_nr_2020 data
files = list.files(path = '../Original data/meta data/')
files<-paste('../Original data/meta data/',files,sep="")
dat <- read_delim(files[1], locale = locale(encoding = "ISO-8859-1"),delim=",")

for(i in 2:length(files))
  {
  print(i)
  a<-0
  if(files[i]=="../Original data/meta data/ledige_stillinger_meldt_til_nav_2020_uten_FINN.csv")
    {
    help <- read_delim(files[i], locale = locale(encoding = "UTF-8"),delim=";")
    help <- help %>% mutate(Stillingsnummer=as.numeric(Stillingsnummer),
                                             `Nav enhet kode` = as.numeric(`Nav enhet kode`),
                                             `Registrert dato` = as.Date(`Registrert dato`, format="%d.%m.%Y"),
                                             `Yrkeskode`=as.numeric(`Yrkeskode`))
    a <- 1
    }
  if(files[i]=="../Original data/meta data/ledige_stillinger_meldt_til_nav_2021_uten_FINN.csv")
    {
    help <- read_delim(files[i], locale = locale(encoding = "UTF-8"),delim=";")
    help <- help %>% mutate(`Stillingsnummer nav no` =`Stilling id`,
                            `Stilling id`=as.numeric(`Stilling id`),
                            `Bedrift org nr`=as.character(`Bedrift org nr`),
                            `Foretak org nr`=as.character(`Foretak org nr`),
                           `Registrert dato` = as.Date(`Avgang dato`, format="%d.%m.%Y"),
                           `Yrkeskode`=as.numeric(`Yrkeskode`))
    a<-1
    }

  if(a==0)
    {
    help <- read_delim(files[i], locale = locale(encoding = "ISO-8859-1"),delim=",")
    if(ncol(help)==1)
      {
      help <- read_delim(files[i], locale = locale(encoding = "UTF-8"),delim=";")
      }
    }
  dat <- bind_rows(dat,help)
}
colnames(dat)<-gsub(pattern=" ",replacement = "_",colnames(dat))
dat <- dat %>% mutate(year=year(Registrert_dato))

# in dat there are multiple entries for an id, need to ensure to take the one with the lastet date

dat <- dat %>% group_by(Stilling_id) %>% mutate(Registrert_dato=max(Registrert_dato)) %>% ungroup()

tidydata <- left_join(tidydata,dat,by=c("doc_id"="Stilling_id"), na_matches="never",multiple="all")

dat.g <- dat %>% count(Siste_publisert_dato) 
dat.g <- dat.g %>% mutate(Siste_publisert_dato=as.Date(Siste_publisert_dato))
dat.g <- dat.g %>% filter(Siste_publisert_dato>as.Date("2001-01-01") & Siste_publisert_dato<as.Date("2022-1-1"))

g <- dat.g %>% ggplot(aes(x=as.Date(Siste_publisert_dato),y=n)) +geom_bar(stat="identity") + scale_x_date()+theme_classic()

pdf("stillinger_days_siste.pdf")
print(g)
dev.off()

saveRDS(tidydata,file="../../Occupation_data/Data/tidydata_with_info.rds")

tidydata <- readRDS("../../Occupation_data/Data/tidydata_with_info.rds")

#reduce to one word per stilling with number
num.words <- tidydata %>% count(doc_id, word)
tidydata <- tidydata %>% distinct()
tidydata <- left_join(tidydata,num.words,by=c("doc_id","word"))
rm(num.words)

num.tidydata <- nrow(tidydata)
#reduce to token with at least 50 appearances
tidydata <- tidydata %>% group_by(word) %>% mutate(in.stillinger=n_distinct(doc_id))
tidydata <- tidydata %>% filter(in.stillinger>50)

#remove stopwords
stops <- read.table("../Help_data/stopwords-no.txt")
colnames(stops) <-"stops"
tidydata <- anti_join(tidydata,stops,by=c("word"="stops"))

#add OkRegioner
regioner <- read.xlsx("../Help_data/kommuner_matched_R.xlsx") %>% select(Arbeidssted_kommunenummer, ok_reg_nr_2020, ok_reg_name_2020) %>% distinct()
regioner <- regioner %>% mutate(Arbeidssted_kommunenummer=ifelse(nchar(Arbeidssted_kommunenummer)==3, paste0("0", Arbeidssted_kommunenummer), Arbeidssted_kommunenummer))
tidydata <- tidydata %>% mutate(Arbeidssted_kommunenummer=ifelse(nchar(Arbeidssted_kommunenummer)==3, paste0("0", Arbeidssted_kommunenummer), Arbeidssted_kommunenummer))
tidydata <- left_join(tidydata, regioner,by="Arbeidssted_kommunenummer")

#Clean the data a bit
tidydata <- tidydata %>% select(-Arbeidssted,-sted,-address,-pos.address,-Offisiell_statistikk_flagg,-Stilling_kilde,-Aktiv_flagg, -Sistepubl_dato)
tidydata <- tidydata %>% group_by(year, word) %>% mutate(regions_with_word=n_distinct(ok_reg_nr_2020)) %>% ungroup()

tidydata <- tidydata %>% group_by(doc_id) %>% mutate(stilling.num.words=sum(n,na.rm=T)) %>% ungroup()
tidydata <- tidydata %>% mutate(share.stilling=n/stilling.num.words)
saveRDS(tidydata,file="../../Occupation_data/Data/tidydata_final_new.rds")
tidydata <- readRDS(file="../Data/tidydata_final_new.rds")

#Preferred label is in and can be used to indicate digital here.

tidydata <- tidydata %>% mutate(word=ifelse(is.na(preferredLabel_n)==F,preferredLabel_n,word))
#work at the level of Yrke-regions to calculate location coefficient of token with respect to this Yrke and Region
reg_data_yrke <- tidydata %>% group_by(year, ok_reg_nr_2020, ok_reg_name_2020, Yrke, word) %>% summarise(token_imp=sum(n,na.rm=T),
                                                                                                         word.in.stilling=max(stilling.num.words,na.rm=T),
                                                                                                         digi = ifelse(is.na(preferredLabel_n)==F,1,0),
                                                                                                         share.stilling=mean(share.stilling,na.rm=T))%>% ungroup()
reg_data_yrke <- reg_data_yrke %>% group_by(year, Yrke, ok_reg_nr_2020, ok_reg_name_2020) %>% mutate(token_imp_j=sum(token_imp)) %>% ungroup() #how many tokens in region
reg_data_yrke <- reg_data_yrke %>% group_by(year, Yrke, word) %>% mutate(token_imp_i=sum(token_imp)) %>% ungroup() # number of words of this kind 
reg_data_yrke <- reg_data_yrke %>% group_by(year, Yrke) %>% mutate(token_imp_a=sum(token_imp)) %>% ungroup() # total number of words
reg_data_yrke <- reg_data_yrke %>% mutate(loc_token_yrke=(token_imp/token_imp_j)/(token_imp_i/token_imp_a))
word_num_yrke <- tidydata %>% select(word, year, Yrke, regions_with_word) %>% distinct()
reg_data_yrke <- left_join(reg_data_yrke,word_num_yrke,by=c("word","year","Yrke"))
saveRDS(reg_data_yrke,file="../../Occupation_data/Data/reg_data_token_yrke.rds")


#work at the level of regions to calculate location coefficient of token with respect to this and Region
reg_data <- tidydata %>% group_by(year,ok_reg_nr_2020, ok_reg_name_2020, word) %>% summarise(token_imp=sum(n, na.rm=T),
                                                                                             word.in.stilling=max(stilling.num.words,na.rm=T),
                                                                                             digi = ifelse(is.na(preferredLabel_n)==F,1,0),
                                                                                             share.stilling=mean(share.stilling,na.rm=T)) %>% ungroup()
reg_data <- reg_data %>% group_by(year, ok_reg_nr_2020, ok_reg_name_2020) %>% mutate(token_imp_j=sum(token_imp)) %>% ungroup() #how many tokens in region
reg_data <- reg_data %>% group_by(year, word) %>% mutate(token_imp_i=sum(token_imp)) %>% ungroup() # number of words of this kind 
reg_data <- reg_data %>% group_by(year) %>% mutate(token_imp_a=sum(token_imp)) %>% ungroup() # total number of words
reg_data <- reg_data %>% mutate(loc_token=(token_imp/token_imp_j)/(token_imp_i/token_imp_a))
word_num <- tidydata %>% select(word,year,regions_with_word) %>% distinct()
reg_data <- left_join(reg_data,word_num,by=c("word","year"))
reg_data <- reg_data %>% arrange(desc(loc_token))

saveRDS(reg_data,file="../../Occupation_data/Data/reg_data_token.rds")


#Combine to aggreated data
test <- tidydata %>% group_by(year) %>% summarise(npost=n_distinct(Stillingsnummer)) %>% ungroup()

data <- readRDS("../Data/dfs_df_sent.rds")
data <- data %>% arrange(doc_id, sentence_id)
data <- data %>% group_by(doc_id) %>% summarise(text=paste(cleaned_tokens,collapse = " ")) %>% ungroup()

data <- left_join(data,dat,by=c("doc_id"="Stilling_id"),na_matches = "never",multiple = "first")

test <- data %>% group_by(year) %>% summarise(npost=n_distinct(Stillingsnummer)) %>% ungroup()

saveRDS(data,file="../../Occupation_data/Data/jointed_data.rds")

