setwd("~/Documents/replies_to_delegates/")

################################################################################

devtools::install_github("m-pilarski/klartext", quiet=TRUE)

################################################################################

library(fs)
library(tidyverse)
library(magrittr)
library(lubridate)
library(bit64)
library(klartext)
library(DBI)
library(RSQLite)
library(dbplyr)

source("./resources/utils.R")

`%|0|%` <- rlang:::`%|0|%`

################################################################################

dir_data <- dir_create("~/Documents/get_tweets/data")
dir_temp <- dir_create(path(dir_data, "temp"))

################################################################################

nrow_db <- function(.db){pull(tally(.db), n)}

################################################################################
################################################################################
################################################################################

tweets_db <- dbConnect(
  drv=SQLite(), dbname=path(dir_data, "tweets.db"), extended_types=TRUE
)

tweets_clean_pruned_txt_path <- path(dir_data, "tweets_clean_pruned", ext="txt")

################################################################################
################################################################################
################################################################################

local({
  
  if(!"tweets_clean" %in% dbListTables(tweets_db)){
    dbWriteTable(
      conn=tweets_db, name="tweets_clean", 
      value=tibble(tweet_status_id=integer64(), tweet_text=character())
    )
  }
  
  tweets_raw_uncleaned_chunk_size <- 1e5L
  
  tweets_raw_uncleaned_statement <- 
    "SELECT `tweet_status_id`, `tweet_text` " %s+%
    "FROM ( " %s+%
    "  SELECT * " %s+%
    "  FROM ( " %s+%
    "    SELECT * " %s+%
    "    FROM `tweets_raw` " %s+%
    "    WHERE (`tweet_lang` = 'en') " %s+%
    "  ) lhs" %s+%
    "  LEFT JOIN `tweets_clean` rhs " %s+%
    "  ON lhs.`tweet_status_id` = rhs.`tweet_status_id` " %s+%
    "  WHERE (rhs.`tweet_status_id` IS NULL) " %s+%
    ") "
  
  tweets_cleaned_counter <- 0L
  
  repeat{
    
    tweets_raw_uncleaned_query <- dbSendQuery(
      conn=tweets_db, statement=tweets_raw_uncleaned_statement
    )
    
    .chunk_raw <- dbFetch(
      res=tweets_raw_uncleaned_query, n=tweets_raw_uncleaned_chunk_size)
    
    dbClearResult(tweets_raw_uncleaned_query)
    
    if(nrow(.chunk_raw) == 0){break()}
    
    .chunk_mod <- mutate(.chunk_raw, across(tweet_text, function(..text_vec){
        
        ..text_vec %>%
          str_remove_reply_mentions() %>%
          str_convert_html() %>%
          str_to_lower() %>%
          str_to_ascii() %>%
          str_unify_spacing() %>%
          str_blur_numbers() %>%
          str_blur_url() %>%
          str_describe_emoji(.resolution="subgroup") %>%
          str_replace_all("([[:punct:]]|(<[A-Z_]+>))(?: *\\1)+", "\\1")
        
      }))
    
    dbWriteTable(
      conn=tweets_db, name="tweets_clean", value=.chunk_mod, append=TRUE
    )

    tweets_cleaned_counter <- tweets_cleaned_counter + nrow(.chunk_mod)
    
    rm(.chunk_raw, .chunk_mod)
    
    message(
      scales::number(tweets_cleaned_counter, prefix="\r"), 
      " tweets cleaned", appendLF=FALSE
    )
    
    gc()
    
  }
  
  message("")
  
  return(invisible(NULL))

})

################################################################################
################################################################################
################################################################################

tweets_clean_term_count <- local({
  
  tweets_clean_chunk_size <- 1e5L
  
  tweets_clean_statement <- "SELECT * FROM `tweets_clean`"
  
  tweets_term_count_counter <- 0L
  
  tweets_clean_term_count <- tibble()
  
  tweets_clean_query <- dbSendQuery(
    conn=tweets_db, statement=tweets_clean_statement
  )
  
  repeat{
    
    .chunk_raw <- dbFetch(res=tweets_clean_query, n=tweets_clean_chunk_size)
    
    if(nrow(.chunk_raw) == 0){break()}
    
    tweets_clean_term_count <- 
      .chunk_raw %>% 
      mutate(across(tweet_text, str_split, fixed(" "))) %>% 
      unnest_longer(tweet_text, values_to="tweet_term") %>% 
      group_by(tweet_term) %>% 
      summarize(
        term_freq = n(), tweet_freq = n_distinct(tweet_status_id), 
        .groups="drop"
      ) %>% 
      bind_rows(tweets_clean_term_count) %>% 
      group_by(tweet_term) %>% 
      summarize(across(c(term_freq, tweet_freq), sum))
    
    tweets_term_count_counter <- tweets_term_count_counter + nrow(.chunk_raw)
    
    rm(.chunk_raw)
    
    message(
      "\r", scales::number(tweets_term_count_counter), 
      " tweets' terms counted", appendLF=FALSE
    )
    
    gc()
    
  }
  
  dbClearResult(tweets_clean_query)
  
  message("")
  
  tweets_clean_term_count <- arrange(tweets_clean_term_count, -term_freq)
  
  return(tweets_clean_term_count)
  
})

################################################################################
################################################################################
################################################################################

local({
  
  terms_keep <- slice_max(tweets_clean_term_count, tweet_freq, n=5e4)
  
  print(slice_min(terms_keep, term_freq, n=10))
  
  dbWriteTable(
    conn=tweets_db, name="tweets_clean_pruned", overwrite=TRUE,
    value=tibble(tweet_status_id=integer64(), tweet_text=character())
  )
  
  tweets_clean_chunk_size <- 1e5
  
  tweets_clean_statement <- 
    "SELECT `tweet_status_id`, `tweet_text` " %s+%
    "FROM ( " %s+%
    "  SELECT *" %s+% 
    "  FROM `tweets_clean` lhs " %s+%
    "  LEFT JOIN `tweets_clean_pruned` rhs " %s+%
    "  ON lhs.`tweet_status_id` = rhs.`tweet_status_id` " %s+%
    "  WHERE (rhs.`tweet_status_id` IS NULL) " %s+% 
    ") "
  
  tweets_clean_pruned_counter <- 0L
  
  repeat{
    
    tweets_clean_query <- dbSendQuery(
      conn=tweets_db, statement=tweets_clean_statement
    )
    
    .chunk_raw <- dbFetch(res=tweets_clean_query, n=tweets_clean_chunk_size)
    
    dbClearResult(tweets_clean_query)
    
    if(nrow(.chunk_raw) == 0){break()}
    
    .chunk_mod <-
      .chunk_raw %>% 
      as_tibble() %>% 
      mutate(across(tweet_text, str_split, fixed(" "))) %>% 
      unnest_longer(
        tweet_text, values_to="tweet_term", indices_to="tweet_position"
      ) %>% 
      semi_join(terms_keep, by="tweet_term") %>% 
      group_by(tweet_status_id) %>% 
      summarize(
        tweet_text = str_c(tweet_term[order(tweet_position)], collapse=" "),
        .groups="drop"
      ) %>% 
      complete(
        tweet_status_id = pull(.chunk_raw, tweet_status_id),
        fill=list(tweet_text="")
      )
      
    dbWriteTable(
      conn=tweets_db, name="tweets_clean_pruned", value=.chunk_mod, append=TRUE
    )
    
    write_lines(
      x=discard(pull(.chunk_mod, tweet_text), `==`, ""), 
      file=tweets_clean_pruned_txt_path, 
      append=tweets_clean_pruned_counter > 0
    )
    
    tweets_clean_pruned_counter <- tweets_clean_pruned_counter + nrow(.chunk_mod)
    
    rm(.chunk_raw, .chunk_mod)
    
    message(
      scales::number(tweets_clean_pruned_counter, prefix="\r"), 
      " tweets pruned", appendLF=FALSE
    )
    
    gc()
    
  }
  
  message("")
  
  return(invisible(NULL))
  
})
