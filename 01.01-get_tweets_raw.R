# TODO:

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
  "user_id"="user_id",
  "user_screen_name"="screen_name", 
  "tweet_id"="status_id",
  "tweet_date"="created_at", 
  "tweet_lang"="lang", 
  "tweet_text"="text", 
  "tweet_reply_tweet_id"="reply_to_status_id", 
  "tweet_reply_user_id"="reply_to_user_id",
  "tweet_reply_user_screen_name"="reply_to_screen_name",
  "tweet_retweet_tweet_id"="retweet_status_id", 
  "tweet_retweet_tweet_date"="retweet_date", 
  "tweet_retweet_user_id"="retweet_user_id", 
  "tweet_retweet_user_screen_name"="retweet_screen_name", 
  "tweet_quote_tweet_id"="quoted_status_id", 
  "tweet_quote_tweet_date"="quoted_created_at",
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
  "convers_id"=as.integer64,
  "user_id"=as.integer64, 
  "user_screen_name"=as.character, 
  "tweet_id"=as.integer64, 
  "convers_user_screen_name"=as.character,
  "convers_user_id"=as.integer64,
  "tweet_date"=as.POSIXct, 
  "tweet_lang"=as.character, 
  "tweet_text"=as.character, 
  "tweet_reply_tweet_id"=as.integer64, 
  "tweet_reply_user_id"=as.integer64,
  "tweet_reply_user_screen_name"=as.character,
  "tweet_retweet_tweet_id"=as.integer64, 
  "tweet_retweet_tweet_date"=as.POSIXct, 
  "tweet_retweet_user_id"=as.integer64, 
  "tweet_retweet_user_screen_name"=as.character, 
  "tweet_quote_tweet_id"=as.integer64, 
  "tweet_quote_tweet_date"=as.POSIXct,
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

if(!"convers_info" %in% dbListTables(tweets_db)){
  dbWriteTable(
    conn=tweets_db, name="tweets_raw", 
    value=tibble(
      convers_user_screen_name = character(0L),
      convers_user_id = integer64(0L),
      party = character(0L),
      role = character(0L),
      still_in_list = logical(0L),
      from_data_last_search_until_id = integer64(0L),
      repl_data_last_search_until_id = integer64(0L)
    )
  )
}

################################################################################

token_list <- read_rds("./resources/token_list.rds")

token_env <- rlang::env()

################################################################################

local({
  
  .convers_info <-
    tribble(
      ~party,       ~role,          ~list_id,
      "republican", "us_senate",    "559315",
      "republican", "us_house",     "817470159027929089",
      "democrat",   "us_senate",    "7465836",
      "democrat",   "us_house",     "6136726",
      "democrat",   "us_president", "1529460417738727429",
    ) %>%
    mutate(lists_members = map(list_id, rtweet::lists_members)) %>%
    select(-list_id) %>%
    unnest(lists_members) %>%
    group_by(
      convers_user_screen_name = screen_name, 
      convers_user_id = as.integer64(user_id)
    ) %>%
    summarize(
      across(c(party, role), function(.vec){
        if_else(n_distinct(.vec) == 1, unique(.vec), "multiple")
      }), 
      .groups="drop"
    ) %>%
    bind_rows(dbGetQuery(tweets_db, "SELECT * FROM `convers_info`")) %>% 
    group_by(
      convers_user_screen_name, convers_user_id, party, role
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
    conn=tweets_db, name="convers_info", overwrite=TRUE, 
    value=.convers_info
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

get_tweets <- function(.pars, .verbose=FALSE){
  
  .GlobalEnv[[".message_pars"]] <- list(
    screen_name = str_c(
      .pars$convers_user_screen_name, " (",
      scales::percent(.pars$obs_id_rel_max, accuracy=1), ")"
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
        convers_user_id == user_id,
        convers_user_id == !!.pars$convers_user_id
      )
    
    .from_data_this_search_since_id <- 
      tweets_db %>% 
      tbl("convers_info") %>% 
      filter(
        convers_user_id == !!.pars$convers_user_id
      ) %>% 
      collect() %>% 
      arrange(-still_in_list) %>% 
      pluck("from_data_last_search_until_id", 1, .default=bit64::NA_integer64_)
    
    if(is.na(.from_data_this_search_since_id)){
      
      if(pull(tally(.from_data_db_tbl), n) > 0){
        
        .from_data_this_search_since_id <-
          .from_data_db_tbl %>%
          summarise(across(tweet_id, max, na.rm=TRUE)) %>% 
          pull(tweet_id)
        
      }else{
        
        .from_data_this_search_since_id <-
          str_c(str_c(c(letters, as.character(0:9)), collapse=" OR "),
                " until:", as_date(now(tzone="UTC") - days(5))) %>%
          search_tweets(n=1) %>% 
          prep_rtweet_df() %>% 
          pull(tweet_id)
        
      }
      
    }
    
    .from_data_this <- 
      get_timeline_mod(
        .screen_name=.pars$convers_user_screen_name, 
        .since_id=as.character(.from_data_this_search_since_id)
      ) %>% 
      mutate(
        convers_id = tweet_id,
        convers_user_id = user_id,
        convers_user_screen_name = user_screen_name
      )
    
    if(nrow(.from_data_this) > 0){
      
      .from_data_this_search_until_id <-
        .from_data_this %>%
        summarise(across(tweet_id, max)) %>% 
        pull(tweet_id)
      
      DBI::dbWriteTable(
        conn=tweets_db, name="tweets_raw", append=TRUE,
        value=.from_data_this
      )
      
      dbExecute(
        tweets_db, 
        str_c(
          "UPDATE `convers_info` ",
          "SET `from_data_last_search_until_id` = ", 
          .from_data_this_search_until_id, " ",
          "WHERE `convers_user_id` = ",
          .pars$convers_user_id
        )
      )
      
    }else{
      dbExecute(
        tweets_db, 
        str_c(
          "UPDATE `convers_info` ",
          "SET `from_data_last_search_until_id` = ", 
          .from_data_this_search_since_id, " ",
          "WHERE `convers_user_id` = ",
          .pars$convers_user_id
        )
      )
    }  
    
    gc()
    
    #########################################
    # GET TWEETS REPLYING TO TWEETS BY USER #
    #########################################
    
    .repl_data_db_tbl <- 
      tweets_db %>% 
      tbl("tweets_raw") %>% 
      filter(
        convers_user_id == !!.pars$convers_user_id,
        convers_user_id != user_id
      )
    
    .repl_data_last_search_until_date <- 
      tweets_db %>% 
      tbl("convers_info") %>% 
      filter(
        convers_user_id == !!.pars$convers_user_id
      ) %>% 
      pull(repl_data_last_search_until_date)
    
    .repl_data_this_search_until_date <- floor_date(
      now(tzone="UTC") - hours(12), "day"
    )
    
    .repl_data_this_search_since_id <- 
      tweets_db %>% 
      tbl("convers_info") %>% 
      filter(
        convers_user_id == !!.pars$convers_user_id
      ) %>% 
      collect() %>% 
      arrange(-still_in_list) %>% 
      pluck("repl_data_last_search_until_id", 1, .default=bit64::NA_integer64_)
    
    if(is.na(.repl_data_this_search_since_id)){
      
      if(pull(tally(.repl_data_db_tbl), n) > 0){
        
        .repl_data_this_search_since_id <-
          .repl_data_db_tbl %>%
          summarise(across(tweet_id, max, na.rm=TRUE)) %>% 
          pull(tweet_id)
        
      }else if(pull(tally(.from_data_db_tbl), n) > 0){
        
        .repl_data_this_search_since_id <- 
          .from_data_db_tbl %>% 
          summarise(across(tweet_id, min, na.rm=TRUE)) %>% 
          pull(tweet_id)
        
      }else{
        
        return(invisible(NULL))
        
      }
      
    }
    
    .repl_data_this_search_is_due <- !isTRUE(
      .repl_data_this_search_until_date <= .repl_data_last_search_until_date
    )
    
    if(.repl_data_this_search_is_due){
      
      .repl_data_this_search_query <- str_glue(
        "@{.pars$convers_user_screen_name} filter:replies ",
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
        summarise(across(tweet_id, max)) %>% 
        pull(tweet_id)
      
      .repl_data_status_repl_id_this <-
        .repl_data_this %>%
        select(tweet_id, tweet_reply_tweet_id) %>% 
        drop_na()
      
      .repl_data_last_status_convers_id <-
        .repl_data_db_tbl %>%
        select(tweet_id, convers_id) %>% 
        collect() %>% 
        drop_na()
      
      .repl_data_status_id <- c(
        .repl_data_status_repl_id_this$tweet_id,
        .repl_data_last_status_convers_id$tweet_id
      )
      
      .convers_status_oldest <- 
        min(.repl_data_this$tweet_date) - weeks(4)
      
      .convers_status_id_this <-
        .from_data_db_tbl %>% 
        filter(
          is.na(tweet_retweet_tweet_id),
          as.integer(tweet_date) > !!as.integer(.convers_status_oldest)
        ) %>% 
        select(convers_id = tweet_id) %>%
        collect() %>% 
        drop_na() %>% 
        mutate(tweet_id = map(convers_id, function(..convers_id){
          
          ..status_id_last <-
            .repl_data_last_status_convers_id %>%
            filter(convers_id == ..convers_id) %>%
            pull(tweet_id)
          
          ..status_id_this <- integer64()
          ..status_id_this_i <- c(..convers_id, ..status_id_last)
          
          if(nrow(.repl_data_status_repl_id_this) > 0){
            while(length(..status_id_this_i) > 0){
              ..status_id_this_i <-
                .repl_data_status_repl_id_this %>%
                filter(tweet_reply_tweet_id %in% ..status_id_this_i) %>%
                pull(tweet_id)
              ..status_id_this <-
                append(..status_id_this, ..status_id_this_i)
            }
          }
          
          ..status_id_this <- unique(..status_id_this)
          
          return(..status_id_this)
          
        })) %>%
        as_tibble() %>%
        unnest_longer(tweet_id) %>%
        drop_na()
      
      .repl_data_this <- 
        select(.repl_data_this, -convers_id) %>% 
        dplyr::inner_join(
          .convers_status_id_this, by="tweet_id"
        ) %>% 
        mutate(
          convers_user_screen_name = 
            .pars$convers_user_screen_name,
          convers_user_id = 
            .pars$convers_user_id
        )
      
      dbWriteTable(
        conn=tweets_db, name="tweets_raw", append=TRUE,
        value=.repl_data_this
      )
      
      dbExecute(
        tweets_db, 
        str_c(
          "UPDATE `convers_info` ",
          "SET `repl_data_last_search_until_id` = ", 
          .repl_data_this_search_until_id, " ",
          "WHERE `convers_user_id` = ",
          .pars$convers_user_id
        )
      )
      
      .GlobalEnv[[".message_pars"]][["valid reply share"]] <- 
        scales::percent(nrow(.repl_data_this) / .repl_data_this_nrow_coll)
      print_message()
      
    }else{
      
      dbExecute(
        tweets_db, 
        str_c(
          "UPDATE `convers_info` ",
          "SET `repl_data_last_search_until_id` = ", 
          .repl_data_this_search_since_id, " ",
          "WHERE `convers_user_id` = ",
          .pars$convers_user_id
        )
      )
      
    }
    
    dbExecute(
      tweets_db, 
      str_c(
        "UPDATE `convers_info` ",
        "SET `repl_data_last_search_until_date` = ", 
        format(.repl_data_this_search_until_date, "'%Y-%m-%d %H:%M:%S'"), " ",
        "WHERE `convers_user_id` = ",
        .pars$convers_user_id
      )
    )
    
  }, warning = function(..warning){
    
    ..tweet_log_entry <- list(
      message=..warning$message, 
      screen_name=.pars$convers_user_screen_name,
      time=lubridate::now()
    )
    
    if(is.list(.GlobalEnv[["tweet_log"]])){
      .GlobalEnv[["tweet_log"]][["warning"]] <- compact(c(
        .GlobalEnv[["tweet_log"]][["warning"]], list(..tweet_log_entry)
      ))
    }
    
    message(
      ..tweet_log_entry$time, " - Warning at ", ..tweet_log_entry$screen_name, 
      ": ", ..tweet_log_entry$message
    )
    
  }, error = function(..error){
    
    ..tweet_log_entry <- list(
      message=..error$message, 
      screen_name=.pars$convers_user_screen_name,
      time=lubridate::now()
    )
    
    if(is.list(.GlobalEnv[["tweet_log"]])){
      .GlobalEnv[["tweet_log"]][["error"]] <- compact(c(
        .GlobalEnv[["tweet_log"]][["error"]], list(..tweet_log_entry)
      ))
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
  
  tweet_log <- list(warning=list(), error=list())
  
  start_time <- lubridate::now()
  
  if(length(dir_ls(dir_temp)) > 0){dir_create(dir_delete(dir_temp))}
  
  tweets_raw_db <- 
    dbGetQuery(tweets_db, "SELECT * FROM `convers_info`") %>% 
    slice_sample(prop=1) %>%
    mutate(obs_id=1:n(), obs_id_rel_max=obs_id/max(obs_id)) %>% 
    rowwise() %>% 
    group_map(function(.row, ...){as.list(.row)}) %>% 
    walk(get_tweets, .verbose=TRUE)
  
  wait_end_time <- with_tz(min(
    start_time + lubridate::hours(3),
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
  
  print(tweet_log)
  
  gc()
  
  while(lubridate::now() < wait_end_time){
    
    naptime::naptime(min(
      wait_end_time, 
      `hour<-`(today(tzone="UTC"), 12),
      lubridate::now() + lubridate::minutes(10)
    ))
    
  }
  
}

