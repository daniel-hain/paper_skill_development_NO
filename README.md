# paper_skill_development_NO

## Workflow

1. Create a folder temp, data, and output one level of hierarchy above this R project. 
  * In data, put the ESCO skill as well as the job postings csv's.
2. Run the 01_preprocessing.R to do a basic txt cleaning of the job posts.
3. Use the outputted cleaned job in (this Colab)[https://github.com/daniel-hain/paper_skill_development_NO/blob/main/10_embeddings_sbert_jobs.ipynb] (or run the ipynb locally) to create SBERT embeddings for all job postings and skill descriptions
4. run 02_embeddings_match.R to do semnatic similarity matching between skills and job postings.

## First EDA

* Comming soon



