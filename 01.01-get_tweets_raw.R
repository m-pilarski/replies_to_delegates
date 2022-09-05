# TODO:
# - convert list columns from "serializejson" to "tojson" and adjust 
#   rtweet_list_to_json()

################################################################################

setwd("~/Documents/get_tweets")

################################################################################

if(packageVersion("rtweet") != "0.7.0"){
  devtools::install_version(
    "rtweet", version="0.7.0", repos="http://cran.us.r-project.org"
  )
}

################################################################################

library(fs)
library(tidyverse)
library(magrittr)
library(lubridate)
library(bit64)
library(rtweet)
library(DBI)
library(RSQLite)
library(dbplyr)

source("./resources/utils.R")

`%|0|%` <- rlang:::`%|0|%`

################################################################################

dir_data <- dir_create("~/Documents/get_tweets/data")
dir_temp <- dir_create(path(dir_data, "temp"))

################################################################################
################################################################################
################################################################################

tweet_df_rename_dict <- list(
  "tweet_status_id"="status_id",
  "tweet_user_id"="user_id",
  "tweet_created_at"="created_at", 
  "tweet_user_screen_name"="screen_name", 
  "tweet_lang"="lang", 
  "tweet_text"="text", 
  "tweet_reply_to_status_id"="reply_to_status_id", 
  "tweet_reply_to_user_id"="reply_to_user_id",
  "tweet_reply_to_user_screen_name"="reply_to_screen_name",
  "tweet_retweet_status_id"="retweet_status_id", 
  "tweet_retweet_created_at"="retweet_created_at", 
  "tweet_retweet_user_id"="retweet_user_id", 
  "tweet_retweet_user_screen_name"="retweet_screen_name", 
  "tweet_quote_status_id"="quoted_status_id", 
  "tweet_quote_created_at"="quoted_created_at",
  "tweet_quote_user_id"="quoted_user_id", 
  "tweet_quote_user_screen_name"="quoted_screen_name", 
  "tweet_quote_text"="quoted_text",
  "tweet_url_json"="urls_expanded_url", 
  "tweet_media_int_json"="media_expanded_url", 
  "tweet_media_int_type_json"="media_type", 
  "tweet_media_ext_json"="ext_media_expanded_url", 
  "tweet_media_ext_type_json"="ext_media_type"
)

################################################################################

tweet_df_coercion_funs <- list(
  "tweet_status_id"=as.integer64, 
  "tweet_convers_id"=as.integer64,
  "tweet_convers_user_screen_name"=as.character,
  "tweet_convers_user_id"=as.integer64,
  "tweet_user_id"=as.integer64, 
  "tweet_user_screen_name"=as.character, 
  "tweet_created_at"=as.POSIXct, 
  "tweet_lang"=as.character, 
  "tweet_text"=as.character, 
  "tweet_reply_to_status_id"=as.integer64, 
  "tweet_reply_to_user_id"=as.integer64,
  "tweet_reply_to_user_screen_name"=as.character,
  "tweet_retweet_status_id"=as.integer64, 
  "tweet_retweet_created_at"=as.POSIXct, 
  "tweet_retweet_user_id"=as.integer64, 
  "tweet_retweet_user_screen_name"=as.character, 
  "tweet_quote_status_id"=as.integer64, 
  "tweet_quote_created_at"=as.POSIXct,
  "tweet_quote_user_id"=as.integer64, 
  "tweet_quote_user_screen_name"=as.character, 
  "tweet_quote_text"=as.character,
  "tweet_url_json"=rtweet_list_to_json, 
  "tweet_media_int_json"=rtweet_list_to_json, 
  "tweet_media_int_type_json"=rtweet_list_to_json,
  "tweet_media_ext_json"=rtweet_list_to_json, 
  "tweet_media_ext_type_json"=rtweet_list_to_json
)

################################################################################

prep_rtweet_df <- function(.rtweet_df){
  
  colnames(.rtweet_df) <- recode(
    colnames(.rtweet_df),
    !!!structure(names(tweet_df_rename_dict), names=tweet_df_rename_dict),
    .default=NULL
  )
  .tweet_df <- imap_dfc(tweet_df_coercion_funs, function(..fun, ..name){
    ..fun(pluck(.rtweet_df, ..name, .default=NA))
  })
  if(nrow(.rtweet_df) == 0){
    .tweet_df <- slice(.tweet_df, integer(0))
  }
  return(.tweet_df)
  
}

################################################################################
################################################################################
################################################################################

tweets_db <- dbConnect(
  drv=SQLite(), dbname=path(dir_data, "tweets.db"), extended_types=TRUE
)

if(!"tweets_raw" %in% dbListTables(tweets_db)){
  dbWriteTable(
    conn=tweets_db, name="tweets_raw", value=prep_rtweet_df(data.frame())
  )
}

if(!"delegates_info" %in% dbListTables(tweets_db)){
  dbWriteTable(
    conn=tweets_db, name="tweets_raw", 
    value=tibble(
      tweet_convers_user_screen_name = character(0L),
      tweet_convers_user_id = integer64(0L),
      party = character(0L),
      role = character(0L),
      still_in_list = logical(0L),
      from_data_last_search_until_id = integer64(0L),
      repl_data_last_search_until_id = integer64(0L)
    )
  )
}

# dbExecute(
#   tweets_db,
#   str_c(
#     "ALTER TABLE `delegates_info` ",
#     "ADD COLUMN `repl_data_last_search_until_date` TIMESTAMP"
#   )
# )

################################################################################

token_list <- read_rds("./resources/token_list.rds")

token_env <- rlang::env()

################################################################################

local({
  
  .delegates_info <-
    tribble(
      ~party,       ~role,          ~list_id,
      "republican", "us_senate",    "559315",
      "republican", "us_house",     "817470159027929089",
      "democrats",  "us_senate",    "7465836",
      "democrats",  "us_house",     "6136726",
      "democrats",  "us_president", "1529460417738727429",
    ) %>%
    mutate(lists_members = map(list_id, rtweet::lists_members)) %>%
    select(-list_id) %>%
    unnest(lists_members) %>%
    group_by(
      tweet_convers_user_screen_name = screen_name, 
      tweet_convers_user_id = as.integer64(user_id)
    ) %>%
    summarize(
      across(c(party, role), function(.vec){
        if_else(n_distinct(.vec) == 1, unique(.vec), "multiple")
      }), 
      .groups="drop"
    ) %>%
    bind_rows(dbGetQuery(tweets_db, "SELECT * FROM `delegates_info`")) %>% 
    group_by(
      tweet_convers_user_screen_name, tweet_convers_user_id, party, role
    ) %>%
    summarize(
      still_in_list = n() > 1, 
      across(
        matches("^(from|repl)_data_last_(search)?_until_id$"),
        function(.vec){unique(discard(.vec, is.na)) %|0|% NA_character_}
      ),
      across(
        repl_data_last_search_until_date,
        function(.vec){unique(discard(.vec, is.na)) %|0|% NA_POSIXct_}
      ),
      .groups="drop"
    )
  
  DBI::dbWriteTable(
    conn=tweets_db, name="delegates_info", overwrite=TRUE, 
    value=.delegates_info
  )
  
})

################################################################################
################################################################################
################################################################################

get_twitter_data <- function(
  ..., .max_id=NULL, .since_id="1", .tweets_n_max=Inf, .endpoint
){
  
  .dots <- list(...)

  stopifnot(.endpoint %in% c("statuses/user_timeline", "search/tweets"))
  
  if(!rlang::env_has(token_env, .endpoint)){
    token_env[[.endpoint]] <- suppressMessages(best_token(.endpoint))
  }
  
  .param <- list(
    max_id=.max_id, check=TRUE, verbose=FALSE, tweet_mode="extended", count=100
  )

  if(.endpoint == "statuses/user_timeline"){
    
    stopifnot(".screen_name" %in% names(.dots))
    .type <- "timeline"
    .param <- .param %>% c(
      screen_name=.dots$.screen_name, home=FALSE, include_rts=FALSE, 
      exclude_replies=TRUE
    )
    
  }else if(.endpoint == "search/tweets"){
    
    stopifnot(".search_query" %in% names(.dots))
    .type <- "search"
    .param <- .param %>% c(
      q=.dots$.search_query, result_type="recent"
    )
    
  }else{
    
    stop("endpoint \"", .endpoint, "\" not supported")
    
  }

  .tweets_list <- list()
  .tweets <- prep_rtweet_df(tibble())

  .tweets_n <- 0L
  .resume <- TRUE

  while(.resume){

    .tweets_i <- NULL
    .tweets_i_try_count <- 1
    
    .tweets_rtweet_url_i <- rtweet:::make_url(
      query=.endpoint, param=.param
    )
    
    while(is_null(.tweets_i)){
      
      tryCatch({
        
        .tweets_i <- rtweet::tweets_with_users(rtweet:::scroller(
          url=.tweets_rtweet_url_i, n=rtweet:::unique_id_count(type=.type), 
          n.times=1, type=.type, token_env[[.endpoint]], httr::timeout(60)
        ))
        if(is.null(.tweets_i)){stop("rtweet returned NULL")}
        
      }, error=function(..error){
        
        if(str_detect(
          ..error$message, "(?i)(TIMEOUT WAS REACHED)|(COULD NOT RESOLVE HOST)"
        )){
          warning(Sys.time(), " - cannot connect", call.=FALSE)
          .tweets_i <<- NULL
          Sys.sleep(30)
        }else if(.tweets_i_try_count <= 5){
          .tweets_i <<- NULL
          .tweets_i_try_count <<- .tweets_i_try_count + 1
          Sys.sleep(10)
        }else{
          stop(..error$message, call.=FALSE)
        }
        
      }, warning = function(..warning){
        
        if(str_detect(..warning$message, "(?i)(RATE|TIME)[- ]?LIMIT")){
          token_env[[.endpoint]] <- suppressMessages(best_token(.endpoint))
          .tweets_i <<- NULL
        }else if(.tweets_i_try_count <= 5){
          .tweets_i <<- NULL
          .tweets_i_try_count <<- .tweets_i_try_count + 1
          Sys.sleep(10)
        }else{
          stop(..warning$message, call.=FALSE)
        }
        
      })
      
    }

    if(all("status_id" %in% colnames(.tweets_i), nrow(.tweets_i) > 0)){

      .tweets_i <- filter(
        .tweets_i, as.integer64(status_id) > as.integer64(.since_id)
      )
  
      .tweets_list <- append(.tweets_list, list(.tweets_i))

      .param$max_id <-
        .tweets_i %>%
        pluck("status_id", .default=NA_character_) %>%
        as.integer64() %>%
        min() %>%
        subtract(1) %>%
        as.character()

    }

    .tweets_n <- .tweets_n + nrow(.tweets_i)
    
    .resume <- all(.tweets_n < .tweets_n_max, nrow(.tweets_i) > 0)
    
    .append <- all(
      any(length(.tweets_list) == 1e3, !.resume), 
      nrow(first(.tweets_list)) > 0
    )
    
    if(.append){

      .tweets_append <-
        .tweets_list %>%
        keep(function(..tweets){nrow(..tweets) > 0}) %>%
        bind_rows() %>%
        prep_rtweet_df()

      .tweets_list <- list()

      .tweets <- bind_rows(.tweets, .tweets_append)
      
    }
    
    if(.resume){
      
      .tweets_i_min_date <- 
        min(pluck(.tweets_i, "created_at", .default=NA_POSIXct_))
      
      .GlobalEnv[[".message_pars"]][[.endpoint]] <- str_c(
        .tweets_n, replace_na(str_c(" (", .tweets_i_min_date, ")"), replace="")
      )
      
      print_message()
      
    }
    
  }

  return(.tweets)

}

get_timeline_mod <- 
  partial(get_twitter_data, ...=, .endpoint="statuses/user_timeline")
search_tweets_mod <- 
  partial(get_twitter_data, ...=, .endpoint="search/tweets")

################################################################################
################################################################################
################################################################################

if(FALSE){
  .transp_r <- 
    dbGetQuery(tweets_db, "SELECT * FROM `delegates_info`") %>% 
    filter(tweet_convers_user_screen_name == "RepEscobar") %>% 
    rowwise() %>% 
    group_map(function(.row, ...){as.list(.row)}) %>% 
    chuck(1)
}

get_tweets <- function(.transp_r, .verbose=FALSE){
  
  .GlobalEnv[[".message_pars"]] <- list(
    screen_name = str_c(
      .transp_r$tweet_convers_user_screen_name, " (",
      scales::percent(.transp_r$obs_id_rel_max, accuracy=1), ")"
    )
  )
  
  tryCatch({
    
    ########################
    # GET TWEETS FROM USER #
    ########################
    
    .from_data_db_tbl <- 
      tweets_db %>% 
      tbl("tweets_raw") %>% 
      filter(
        tweet_convers_user_id == tweet_user_id,
        tweet_convers_user_id == !!.transp_r$tweet_convers_user_id
      )
    
    .from_data_this_search_since_id <- 
      tweets_db %>% 
      tbl("delegates_info") %>% 
      filter(
        tweet_convers_user_id == !!.transp_r$tweet_convers_user_id
      ) %>% 
      collect() %>% 
      pluck("from_data_last_search_until_id", .default=bit64::NA_integer64_)
    
    if(is.na(.from_data_this_search_since_id)){
      
      if(pull(tally(.from_data_db_tbl), n) > 0){

        .from_data_this_search_since_id <-
          .from_data_db_tbl %>%
          summarise(across(tweet_status_id, max, na.rm=TRUE)) %>% 
          pull(tweet_status_id)
  
      }else{
  
        .from_data_this_search_since_id <-
          str_c(
            str_c(c(letters, as.character(0:9)), collapse=" OR "),
            " until:", as_date(now(tzone="UTC") - days(5))
          ) %>%
          search_tweets(n=1) %>% 
          prep_rtweet_df() %>% 
          pull(tweet_status_id)
  
      }
      
    }
    
    .from_data_this <- 
      get_timeline_mod(
        .screen_name=.transp_r$tweet_convers_user_screen_name, 
        .since_id=as.character(.from_data_this_search_since_id)
      ) %>% 
      mutate(
        tweet_convers_id = tweet_status_id,
        tweet_convers_user_id = tweet_user_id,
        tweet_convers_user_screen_name = tweet_user_screen_name
      )

    if(nrow(.from_data_this) > 0){

      .from_data_this_search_until_id <-
        .from_data_this %>%
        summarise(across(tweet_status_id, max)) %>% 
        pull(tweet_status_id)
      
      DBI::dbWriteTable(
        conn=tweets_db, name="tweets_raw", append=TRUE,
        value=.from_data_this
      )
    
      dbExecute(
        tweets_db, 
        str_c(
          "UPDATE `delegates_info` ",
          "SET `from_data_last_search_until_id` = ", 
          .from_data_this_search_until_id, " ",
          "WHERE `tweet_convers_user_id` = ",
          .transp_r$tweet_convers_user_id
        )
      )
      
    }  
    
    gc()
    
    if(!isTRUE(pull(tally(.from_data_db_tbl), n) > 0)){return(invisible(NULL))}

    #########################################
    # GET TWEETS REPLYING TO TWEETS BY USER #
    #########################################
    
    .repl_data_db_tbl <- 
      tweets_db %>% 
      tbl("tweets_raw") %>% 
      filter(
        tweet_convers_user_id == !!.transp_r$tweet_convers_user_id,
        tweet_convers_user_id != tweet_user_id
      )
    
    .repl_data_last_search_until_date <- 
      tweets_db %>% 
      tbl("delegates_info") %>% 
      filter(
        tweet_convers_user_id == !!.transp_r$tweet_convers_user_id
      ) %>% 
      pull(repl_data_last_search_until_date)
    
    .repl_data_this_search_until_date <- floor_date(
      now(tzone="UTC") - hours(12), "day"
    )
    
    .repl_data_this_search_since_id <- 
      tweets_db %>% 
      tbl("delegates_info") %>% 
      filter(
        tweet_convers_user_id == !!.transp_r$tweet_convers_user_id
      ) %>% 
      pull(repl_data_last_search_until_id)
    
    if(is.na(.repl_data_this_search_since_id)){
      
      if(pull(tally(.repl_data_db_tbl), n) > 0){
        
        .repl_data_this_search_since_id <-
          .repl_data_db_tbl %>%
          summarise(across(tweet_status_id, max, na.rm=TRUE)) %>% 
          pull(tweet_status_id)
        
      }else{
        
        .repl_data_this_search_since_id <- 
          .from_data_db_tbl %>% 
          summarise(across(tweet_status_id, min, na.rm=TRUE)) %>% 
          pull(tweet_status_id)
        
      }
      
    }
    
    .repl_data_this_search_is_due <- !isTRUE(
      .repl_data_this_search_until_date <= .repl_data_last_search_until_date
    )
    
    if(.repl_data_this_search_is_due){
      
      .repl_data_this_search_query <- str_glue(
        "@{.transp_r$tweet_convers_user_screen_name} filter:replies ",
        "until:{format(.repl_data_this_search_until_date, '%Y-%m-%d')}"
      )
      
      .repl_data_this <- search_tweets_mod(
        .search_query=.repl_data_this_search_query, 
        .since_id=as.character(.repl_data_this_search_since_id)
      )

    }else{
      
      .repl_data_this <- prep_rtweet_df(data.frame())
      
    }
    
    .repl_data_this_nrow_coll <- nrow(.repl_data_this)

    if(.repl_data_this_nrow_coll > 0){
      
      .repl_data_this_search_until_id <-
        .repl_data_this %>% 
        summarise(across(tweet_status_id, max)) %>% 
        pull(tweet_status_id)
      
      .repl_data_status_repl_id_this <-
        .repl_data_this %>%
        select(tweet_status_id, tweet_reply_to_status_id) %>% 
        drop_na()

      .repl_data_last_status_convers_id <-
        .repl_data_db_tbl %>%
        select(tweet_status_id, tweet_convers_id) %>% 
        collect() %>% 
        drop_na()
          
      .repl_data_status_id <- c(
        .repl_data_status_repl_id_this$tweet_status_id,
        .repl_data_last_status_convers_id$tweet_status_id
      )
        
      .convers_status_oldest <- 
        min(.repl_data_this$tweet_created_at) - weeks(4)
        
      .convers_status_id_this <-
        .from_data_db_tbl %>% 
        filter(
          is.na(tweet_retweet_status_id),
          as.integer(tweet_created_at) > !!as.integer(.convers_status_oldest)
        ) %>% 
        select(tweet_convers_id = tweet_status_id) %>%
        collect() %>% 
        drop_na() %>% 
        mutate(tweet_status_id = map(tweet_convers_id, function(..convers_id){

          ..status_id_last <-
            .repl_data_last_status_convers_id %>%
            filter(tweet_convers_id == ..convers_id) %>%
            pull(tweet_status_id)

          ..status_id_this <- integer64()
          ..status_id_this_i <- c(..convers_id, ..status_id_last)
          
          if(nrow(.repl_data_status_repl_id_this) > 0){
            while(length(..status_id_this_i) > 0){
              ..status_id_this_i <-
                .repl_data_status_repl_id_this %>%
                filter(tweet_reply_to_status_id %in% ..status_id_this_i) %>%
                pull(tweet_status_id)
              ..status_id_this <-
                append(..status_id_this, ..status_id_this_i)
            }
          }
          
          ..status_id_this <- unique(..status_id_this)
          
          return(..status_id_this)
          
        })) %>%
        as_tibble() %>%
        unnest_longer(tweet_status_id) %>%
        drop_na()

      .repl_data_this <- 
        select(.repl_data_this, -tweet_convers_id) %>% 
        dplyr::inner_join(
          .convers_status_id_this, by="tweet_status_id"
        ) %>% 
        mutate(
          tweet_convers_user_screen_name = 
            .transp_r$tweet_convers_user_screen_name,
          tweet_convers_user_id = 
            .transp_r$tweet_convers_user_id
        )
      
      dbWriteTable(
        conn=tweets_db, name="tweets_raw", append=TRUE,
        value=.repl_data_this
      )
      
      dbExecute(
        tweets_db, 
        str_c(
          "UPDATE `delegates_info` ",
          "SET `repl_data_last_search_until_id` = ", 
          .repl_data_this_search_until_id, " ",
          "WHERE `tweet_convers_user_id` = ",
          .transp_r$tweet_convers_user_id
        )
      )
      
      dbExecute(
        tweets_db, 
        str_c(
          "UPDATE `delegates_info` ",
          "SET `repl_data_last_search_until_date` = ", 
          .repl_data_this_search_until_date, " ",
          "WHERE `tweet_convers_user_id` = ",
          .transp_r$tweet_convers_user_id
        )
      )
      
      .GlobalEnv[[".message_pars"]][["valid reply share"]] <- 
        scales::percent(nrow(.repl_data_this) / .repl_data_this_nrow_coll)
      print_message()
      
    }

  }, warning = function(..warning){
    
    ..tweet_log_entry <- list(
      message=..warning$message, 
      screen_name=.transp_r$tweet_convers_user_screen_name,
      time=lubridate::now()
    )
    
    if(is.list(.GlobalEnv[["tweets_log"]])){
      modify_in(.GlobalEnv[["tweets_log"]], "warning", function(...l){
        compact(append(...l, ..tweet_log_entry))
      })
    }

    message(
      ..tweet_log_entry$time, " - Warning at ", ..tweet_log_entry$screen_name, 
      ": ", ..tweet_log_entry$message
    )
    
  }, error = function(..error){
    
    ..tweet_log_entry <- list(
      message=..error$message, 
      screen_name=.transp_r$tweet_convers_user_screen_name,
      time=lubridate::now()
    )
    
    if(is.list(.GlobalEnv[["tweets_log"]])){
      modify_in(.GlobalEnv[["tweets_log"]], "error", function(...l){
        compact(append(...l, ..tweet_log_entry))
      })
    }
    
    message(crayon::bold(crayon::cyan(
      ..tweet_log_entry$time, " - Error at ", ..tweet_log_entry$screen_name, 
      ": ", ..tweet_log_entry$message
    )))
    
    # stop()

  }, finally={
    
    gc()
    
  })
  
  return(invisible(NULL))
  
}

################################################################################
################################################################################
################################################################################

while(TRUE){
  
  tweets_log <- list(warning=list(), error=list())
  
  start_time <- lubridate::now()

  if(length(dir_ls(dir_temp)) > 0){dir_create(dir_delete(dir_temp))}

  tweets_raw_db <- 
    dbGetQuery(tweets_db, "SELECT * FROM `delegates_info`") %>% 
    slice_sample(prop=1) %>%
    mutate(obs_id=1:n(), obs_id_rel_max=obs_id/max(obs_id)) %>% 
    rowwise() %>% 
    group_map(function(.row, ...){as.list(.row)}) %>% 
    walk(get_tweets, .verbose=TRUE)
  
  wait_end_time <- with_tz(min(
    start_time + lubridate::hours(12),
    ceiling_date(now(tzone="UTC") - hours(12), "day") + hours(12)
  ), tzone="")
  
  tweets_raw_total_nrow <- 
    tweets_db %>% 
    tbl("tweets_raw") %>% 
    tally() %>% 
    pull(n)
  
  tweets_raw_total_size_disk <- 
    path(dir_data, "01-tweets_raw") %>% 
    dir_info(recurse=TRUE) %>% 
    pull(size) %>% 
    sum()
    
  .GlobalEnv[[".message_pars"]] <- list(
    `Total amount of tweets` = scales::number(tweets_raw_total_nrow),
    `Size on disk` = format(tweets_raw_total_size_disk, unit="GB"),
    `Waiting until` = format(wait_end_time)
  )
  
  print_message()
  
  print(tweets_log)
  
  gc()
  
  while(lubridate::now() < wait_end_time){
    
    naptime::naptime(min(
      wait_end_time, 
      `hour<-`(today(tzone="UTC"), 12),
      lubridate::now() + lubridate::minutes(10)
    ))
    
  }
  
}

