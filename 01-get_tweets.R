setwd("~/Documents/get_tweets")

library(fs)
library(glue)
library(tidyverse)
library(magrittr)
library(lubridate)
library(rtweet)
library(bit64)
library(disk.frame)

source("./resources/best_token.R")
source("./resources/disk.frame_extensions.R")

has_name <- rlang::has_name

################################################################################

# List of one or multiple Twitter OAuth token(s)
token_list <- as.list(rtweet::get_token())
# token_list <- read_rds("./resources/token_list.rds")

token_env <- rlang::env()

################################################################################

print_message <- function(...){
  
  if(!is.list(.GlobalEnv[[".message_pars"]])){
    .GlobalEnv[[".message_pars"]] <- list()
  }
  
  .message_pars <- discard(
    .GlobalEnv[[".message_pars"]], function(..el){isTRUE(all(is.na(..el)))}
  )
  
  .lines_last <- pluck(.message_pars, ".lines_last", .default=character(0L))
  
  .lines_this <- c(
    str_pad(
      format.Date(Sys.time(), " %Y-%m-%d %H:%M "), width=61, pad="#", 
      side="both"
    ),
    unname(imap_chr(
      .message_pars[str_detect(names(.message_pars), "^[^.]")],
      function(.value, .name){
        str_c(
          str_pad(str_trunc(.name, width=29), width=29, side="left", pad=" "),
          str_pad(str_trunc(.value, width=29), width=29, side="right", pad=" "),
          sep=" : "
        )
      }
    )),
    str_c(rep("#", 61), collapse=""),
    ""
  )
  
  .message_this <- str_c(
    if_else(
      length(.lines_last) > 0 & !rstudioapi::isAvailable(), 
      str_c("\033[", length(.lines_last)-1, "A"), 
      "", ""
    ),
    str_c(.lines_this, collapse="\n"), 
    sep=""
  )
  
  cat(.message_this)
  
  .GlobalEnv[[".message_pars"]][[".lines_last"]] <- .lines_this
  
  return(invisible(.message_this))

}


dir_data <- dir_create("~/Documents/get_tweets/data")
dir_temp <- dir_create(path(dir_data, "temp"))

################################################################################

if(!file_exists("./data/delegates_info.Rds")){
  write_rds(tibble(.rows=0), "./data/delegates_info.Rds")
}

delegates_info <-
  tribble(
    ~party,       ~role,          ~list_id,
    "republican", "us_senate",    "559315",
    "republican", "us_house",     "817470159027929089",
    "democrats",  "us_senate",    "7465836",
    "democrats",  "us_house",     "6136726",
    "democrats",  "us_president", "1529460417738727429",
    # "spd",        "bundestag",  "60502972",
    # "gruene",     "bundestag",  "1442481414738382862",
    # "fdp",        "bundestag",  "1002517865864589312",
    # "cdu_csu",    "bundestag",  "1442412695873986566",
    # "afd",        "bundestag",  "912313478706286593",
    # "linke",      "bundestag",  "1452539649520541699",
  ) %>%
  mutate(lists_members = map(list_id, rtweet::lists_members)) %>%
  select(-list_id) %>%
  unnest(lists_members) %>%
  group_by(screen_name) %>%
  summarize(across(c(party, role), function(.vec){
    if_else(length(.vec) == 1, .vec, "both")
  })) %>%
  bind_rows(read_rds("./data/delegates_info.Rds")) %>%
  group_by(party, role, screen_name) %>%
  summarize(still_in_list = n() > 1, .groups="drop") %>% 
  write_rds("./data/delegates_info.Rds")

summary(
  mutate(
    delegates_info, 
    across(c(where(is.character), where(is.logical)), as.factor)
  )
)

################################################################################
################################################################################
################################################################################

rtweet_to_df <- function(
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
  .tweets <- disk.frame(path=tmp_df())

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

      .tweets_i <- filter(.tweets_i, if_all(status_id, function(..status_id){
        as.integer64(..status_id) > as.integer64(.since_id)
      }))
  
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

    if(all(
      any(length(.tweets_list) == 1e3, !.resume), 
      nrow(first(.tweets_list)) > 0
    )){

      .tweets_append <-
        .tweets_list %>%
        keep(function(..tweets){nrow(..tweets) > 0}) %>%
        bind_rows() %>%
        mutate(across(where(is.list), map_chr, jsonlite::serializeJSON))

      .tweets_list <- list()

      .tweets <- add_chunk(.tweets, .tweets_append)
      
    
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
  partial(rtweet_to_df, ...=, .endpoint="statuses/user_timeline")
search_tweets_mod <- 
  partial(rtweet_to_df, ...=, .endpoint="search/tweets")

################################################################################
################################################################################
################################################################################

.transp_r <-
  delegates_info %>%
  transpose() %>%
  pluck(1)

get_tweets <- function(.transp_r, .verbose=FALSE){
  
  .transp_r <<- .transp_r
  
  .GlobalEnv[[".message_pars"]] <- list(
    screen_name = .transp_r$screen_name
  )
  
  .tweets_raw <- pluck(.transp_r, "tweets", .default=list(NULL))

  .screen_name_dir <- dir_create(
    path(dir_data, "01-tweets_raw", .transp_r$screen_name)
  )
  
  .from_data_last <- disk.frame(path(.screen_name_dir, "from.df"))
  .repl_data_last <- disk.frame(path(.screen_name_dir, "repl.df"))
  
  
  tryCatch({
    
    ########################
    # GET TWEETS FROM USER #
    ########################
    
    if(nrow(.from_data_last) > 0){

      if(file_exists(path(.screen_name_dir, "from_max_status_id_coll"))){
        .from_since_id_this <-
          read_file(path(.screen_name_dir, "from_max_status_id_coll"))
      }else{
        .from_since_id_this <-
          .from_data_last %>%
          collect_cols.df(.cols_want="status_id", .trans_fun=as.integer64) %>%
          pluck("status_id") %>%
          `[`(which.max(as.integer64(.))) %>%
          as.character()
      }

    }else{

      .from_since_id_this <-
        str_c(str_c(c(letters, as.character(0:9)), collapse=" OR "),
              " until:", as_date(now(tzone="UTC") - days(5))) %>%
        search_tweets(n=1) %>%
        pluck("status_id", 1)

    }

    .from_data_this <- get_timeline_mod(
      .screen_name=.transp_r$screen_name, .since_id=.from_since_id_this
    )

    if(nrow(.from_data_this) > 0){

      .from_data_this_max_status_id_coll <-
        .from_data_this %>%
        collect_cols.df(.cols_want="status_id") %>%
        pluck("status_id") %>%
        `[`(which.max(as.integer64(.)))

      .from_data <-
        list(.from_data_last, .from_data_this) %>%
        bind_rows_df(.outdir=tmp_df()) %>%
        write_disk.frame(outdir=path(.screen_name_dir, "from.df"),
                         overwrite=TRUE)

      invisible(cmap(.from_data, function(..chunk){
        invisible(..chunk); rm(..chunk); gc(); return(NULL)
      }, lazy=FALSE))

      write_file(
        .from_data_this_max_status_id_coll,
        path(.screen_name_dir, "from_max_status_id_coll")
      )

    }else{

      .from_data <- .from_data_last

    }

    gc()
    
    if(!isTRUE(nrow(.from_data) > 0)){return(NULL)}

    #########################################
    # GET TWEETS REPLYING TO TWEETS BY USER #
    #########################################

    .repl_data_last <- disk.frame(path(.screen_name_dir, "repl.df"))

    if(nrow(.repl_data_last) > 0){

      if(file_exists(path(.screen_name_dir, "repl_max_status_id_coll"))){
        .repl_since_id_this <-
          read_file(path(.screen_name_dir, "repl_max_status_id_coll"))
      }else{
        .repl_since_id_this <-
          .repl_data_last %>%
          collect_cols.df(.cols_want="status_id", .trans_fun=as.integer64) %>%
          pluck("status_id") %>%
          `[`(which.max(as.integer64(.))) %>%
          as.character()
      }

    }else{

      .repl_since_id_this <- "1"

    }

    gc()
    
    .repl_data_this <- search_tweets_mod(
      .search_query=glue(
        "@{.transp_r$screen_name} filter:replies ",
        "until:{as_date(now(tzone='UTC') - hours(12))}"
      ), 
      .since_id=.repl_since_id_this
    )

    gc()

    .repl_data_this_nrow_coll <- nrow(.repl_data_this)

    if(.repl_data_this_nrow_coll > 0){

      .repl_data_this_max_status_id_coll <-
        .repl_data_this %>%
        collect_cols.df(.cols_want="status_id", .trans_fun=as.integer64) %>%
        pluck("status_id") %>%
        `[`(which.max(as.integer64(.))) %>%
        as.character()

      if(all(
        c("status_id", "reply_to_status_id") %in% colnames(.repl_data_this),
        nrow(.from_data) > 0
      )){

        .repl_data_this_nrow_coll <- nrow(.repl_data_this)

        .repl_data_this_status_repl_id <-
          .repl_data_this %>%
          collect_cols.df(
            .cols_want=c("status_id", "reply_to_status_id"),
            .trans_fun=as.integer64
          ) %>%
          drop_na() %>%
          distinct()

        if(all(
          c("status_id", "reply_to_status_id") %in% colnames(.repl_data_last)
        )){
          
          .repl_data_last_status_convers_id <-
            .repl_data_last %>%
            collect_cols.df(
              .cols_want=c("status_id", "convers_id"),
              .trans_fun=as.integer64
            ) %>%
            drop_na() %>%
            distinct()
          
        }else{
          
          .repl_data_last_status_convers_id <- tibble(
            status_id=integer64(), convers_id=integer64()
          )
          
        }

        gc()

        .repl_data_status_id <- c(
          .repl_data_this_status_repl_id$status_id,
          .repl_data_last_status_convers_id$status_id
        )

        .convers_status_id_this <-
          .from_data %>%
          srckeep(c("status_id", "is_retweet", "created_at")) %>%
          cmap_dfr(function(..chunk){
            transmute(
              filter(..chunk, !is_retweet & created_at > now() - weeks(4)), 
              convers_id = as.integer64(status_id)
            )
          }) %>%
          drop_na() %>%
          distinct()

        .convers_status_id_this <-
          .convers_status_id_this %>%
          mutate(status_id = map(convers_id, function(..convers_id){
            
            if("convers_id" %in% colnames(.repl_data_last)){
              ..status_id_last <-
                .repl_data_last_status_convers_id %>%
                filter(convers_id == ..convers_id) %>%
                pull(status_id)
            }else{
              ..status_id_last <- character(0)
            }
            ..status_id_this <- character(0)
            ..status_id_this_i <- c(..convers_id, ..status_id_last)
            if(nrow(.repl_data_this_status_repl_id) > 0){
              while(length(..status_id_this_i) > 0){
                ..status_id_this_i <-
                  .repl_data_this_status_repl_id %>%
                  filter(reply_to_status_id %in% ..status_id_this_i) %>%
                  pull(status_id)
                ..status_id_this <-
                  append(..status_id_this, ..status_id_this_i)
              }
            }
            ..status_id_this <- unique(..status_id_this)
            
            return(..status_id_this)
            
          })) %>%
          as_tibble() %>%
          mutate(
            convers_id = as.character(convers_id),
            status_id = map(status_id, as.character)
          ) %>%
          unnest_longer(status_id) %>%
          drop_na()

        if(nrow(.convers_status_id_this) > 0){

          .repl_data_this <-
            .repl_data_this %>%
            resuming_cmap(function(..chunk){
              dplyr::inner_join(
                ..chunk, .convers_status_id_this, by="status_id"
              )
            })

        }else{

          .repl_data_this <- disk.frame(tmp_df())

        }
        
        .GlobalEnv[[".message_pars"]][["valid reply share"]] <- 
          scales::percent(nrow(.repl_data_this) / .repl_data_this_nrow_coll)
          print_message()
        
      }else{

        .repl_data_this <- disk.frame(tmp_df())

      }

      ####

      if(nrow(.repl_data_this) > 0){

        .repl_data <-
          list(.repl_data_last, .repl_data_this) %>%
          bind_rows_df(.outdir=path(.screen_name_dir, "repl.df"))

      }else{

        .repl_data <- .repl_data_last

      }


      if(nrow(.repl_data) > 0){

        invisible(cmap(.repl_data, function(..chunk){
          invisible(..chunk); rm(..chunk); gc(); return(NULL)
        }, lazy=FALSE))

      }

      if(.repl_data_this_nrow_coll > 0){
        write_file(
          .repl_data_this_max_status_id_coll,
          path(.screen_name_dir, "repl_max_status_id_coll")
        )
      }

      gc()

    }else{

      .repl_data <- disk.frame(path(.screen_name_dir, "repl.df"))

    }

    ##################
    # PREPARE OUTPUT #
    ##################

    .tweets_raw <- list(from_data = .from_data, repl_data = .repl_data)

  }, warning = function(..warning){
    
    ..tweet_log_entry <- list(
      message=..warning$message, screen_name=.transp_r$screen_name,
      time=lubridate::now()
    )
    
    if(is.list(.GlobalEnv[["tweets_logs"]])){
      modify_in(.GlobalEnv[["tweets_logs"]], "warning", function(...l){
        compact(append(...l, ..tweet_log_entry))
      })
    }

    message(
      ..tweet_log_entry$time, " - Warning at ", ..tweet_log_entry$screen_name, 
      ": ", ..tweet_log_entry$message
    )

  }, error = function(..error){
    
    ..tweet_log_entry <- list(
      message=..error$message, screen_name=.transp_r$screen_name,
      time=lubridate::now()
    )
    
    if(is.list(.GlobalEnv[["tweets_logs"]])){
      modify_in(.GlobalEnv[["tweets_logs"]], "error", function(...l){
        compact(append(...l, ..tweet_log_entry))
      })
    }
    
    message(crayon::bold(
      ..tweet_log_entry$time, " - Error at ", ..tweet_log_entry$screen_name, 
      ": ", ..tweet_log_entry$message
    ))
    
    .tweets_raw <<- list(
      from_data = .from_data_last, repl_data = .repl_data_last
    )
    
  }, finally = {

    suppressMessages({setup_disk.frame(workers=1)}); gc()

  })

  return(.tweets_raw)

}

################################################################################
################################################################################
################################################################################

while(TRUE){
  
  tweets_log <- list(warning=list(), error=list())
  
  start_time <- lubridate::now()

  if(length(dir_ls(dir_temp)) > 0){dir_create(dir_delete(dir_temp))}
  
  tweets_raw_db <-
    delegates_info %>% 
    slice_sample(prop=1) %>% 
    mutate(tweets_raw = map(transpose(.), get_tweets, .verbose=TRUE)) %>%
    write_rds("./data/tweets_raw_db.rds")
  
  wait_end_time <- min(
    start_time + lubridate::hours(12),
    ceiling_date(now(tzone="UTC") - hours(12), "day") + hours(12)
  )
  
  tweets_raw_total_nrow <- 
    tweets_raw_db %>% 
    pull(tweets_raw) %>% 
    map_dbl(function(.tweets_raw){
      sum(map_dbl(keep(.tweets_raw, is_disk.frame), nrow))
    }) %>% 
    sum()
  
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
  
  while(lubridate::now() < wait_end_time){
    naptime::naptime(
      min(
        wait_end_time, `hour<-`(today(tzone="UTC"), 12),
        lubridate::now() + lubridate::minutes(10)
      )
    )
  }
  
}
