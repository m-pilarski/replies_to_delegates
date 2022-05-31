if(!"furrr" %in% installed.packages()){install.packages("furrr")}
if(!"progressr" %in% installed.packages()){install.packages("progressr")}
if(!"pbmcapply" %in% installed.packages()){install.packages("pbmcapply")}

library(furrr)
library(progressr); invisible(handlers("pbmcapply"))

################################################################################

best_map <- function(.x, .f, ..., .workers=NULL, .scheduling=1, .seed=FALSE){
  
  .f <- purrr::as_mapper(.f)
  .dots <- list(...)
  .workers <- min(.workers, future::availableCores())
  
  if(!is.null(.workers)){
    .strategy_backup <- future::plan()
    if(.workers %% 1 != 0 |.workers < 0){
      stop(".workers needs to be an integer greater than ")
    }else if(.workers %in% 0:1){
      if(.workers == 0){
        future::plan(future::transparent, gc=TRUE)
      }else{
        future::plan(future::sequential, gc=TRUE)
      }
      .furrr_opts <- furrr::furrr_options(seed=.seed)
    }else if(.workers >= 2){
      if(rstudioapi::isAvailable()|grepl("(?i)windows", sessionInfo()$running)){
        future::plan(future::multisession, workers=.workers, gc=TRUE)
      }else{
        future::plan(future::multicore, workers=.workers, gc=TRUE)
      }
      .furrr_opts <- 
        furrr::furrr_options(scheduling=.scheduling, conditions=character(0L),
                             seed=.seed)
    }else{
      stop("unable to set future strategy", call.=FALSE)
    }
  }
  
  tryCatch({
    progressr::with_progress({
      .p <- progressr::progressor(steps=length(.x))
      .r <- furrr::future_map(.x, function(..x_i){
        ..r_i <- rlang::exec(.f, ..x_i, !!!.dots); .p(); return(..r_i)
      }, .options=.furrr_opts)
    })
    return(.r)
  }, finally={
    if(!is.null(.workers)){future::plan(.strategy_backup)}
  })
  
}

################################################################################

best_map2 <- function(.x, .y, .f, ..., .workers=1, .scheduling=1, .seed=FALSE){
  
  .f <- purrr::as_mapper(.f)
  .dots <- list(...)
  .workers <- min(.workers, future::availableCores())

  if(!is.null(.workers)){
    .strategy_backup <- future::plan()
    if(.workers %% 1 != 0 |.workers < 0){
      stop(".workers needs to be a natural number")
    }else if(.workers %in% 0:1){
      if(.workers == 0){
        future::plan(future::transparent, gc=TRUE)
      }else{
        future::plan(future::sequential, gc=TRUE)
      }
      .furrr_opts <- furrr::furrr_options()
    }else if(.workers >= 2){
      if(rstudioapi::isAvailable()|grepl("(?i)windows", sessionInfo()$running)){
        future::plan(future::multisession, workers=.workers, gc=TRUE)
      }else{
        future::plan(future::multicore, workers=.workers, gc=TRUE)
      }
      .furrr_opts <- 
        furrr::furrr_options(scheduling=.scheduling, conditions=character(0L),
                             seed=.seed)
    }else{
      stop("unable to set future strategy", call.=FALSE)
    }
  }
  
  tryCatch({
    progressr::with_progress({
      .p <- progressr::progressor(steps=length(.x))
      .r <- furrr::future_map2(.x, .y, function(..x_i, ..y_i){
        ..r_i <- rlang::exec(.f, ..x_i, ..y_i, !!!.dots); .p(); return(..r_i)
      }, .options=.furrr_opts)
    })
    return(.r)
  }, finally={
    if(!is.null(.workers)){future::plan(.strategy_backup)}
  })
  
}

