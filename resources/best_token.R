if(!"naptime" %in% installed.packages()){install.packages("naptime")}

`%>%` <- magrittr::`%>%`
# 
# .token_list <- readr::read_rds("~/Documents/uni/masterarbeit/analysis/resources/token_list.rds")
# .query <- "search/tweets"

################################################################################
################################################################################
################################################################################

best_token <- function(.request="search/tweets", .token_list=token_list){
  
  .best_token <- NULL
  
  while(is.null(.best_token)){
    
    try({
      
      .token_df <- NULL
      
      while(all(
        purrr::map_lgl(purrr::pluck(.token_df, "rate_limit"), is.null)
      )){
        
        .token_df <- 
          tibble::tibble(token = sample(.token_list)) %>% 
          dplyr::mutate(rate_limit = purrr::map(token, function(..token){
            
            ..rate_limit <- NULL
            tryCatch({
              ..rate_limit <- rtweet::rate_limit(token=..token, query=.request)
            }, error=function(...error){
              if(str_detect(
                ...error$message, 
                "(?i)(TIMEOUT WAS REACHED)|(COULD NOT RESOLVE HOST)"
              )){
                warning(Sys.time(), " - cannot connect", call.=FALSE)
                naptime::naptime(lubridate::seconds(30))
                ..rate_limit <<- NULL
              }
            }, warning = function(...warning){
              if(str_detect(
                ...warning$message, 
                "(?i)RATE[- ]?LIMIT"
              )){
                ..rate_limit <<- NULL
              }
            })
            return(..rate_limit)
          }))
        
        if(all(
          purrr::map_lgl(purrr::pluck(.token_df, "rate_limit"), is.null)
        )){
          message("Cannot determine rate limit reset timers. Retrying in 120s")
          naptime::naptime(lubridate::seconds(120))
        }
        
      }
      
      .token_df <- tidyr::unnest(.token_df, rate_limit)
      
      if(!any(.token_df$remaining > 0)){
        
        message("Waiting for rate limit resets at ", min(.token_df$reset_at))
        naptime::naptime(min(.token_df$reset_at) + lubridate::seconds(1))
        .token_df <- NULL
        
      }else{
        
        message("found token for \"", .request, "\"")
        .best_token <- purrr::pluck(
          dplyr::arrange(.token_df, -remaining), "token", 1
        )
        
      }
      
    })
  
  }
  
  return(.best_token)
  
}

