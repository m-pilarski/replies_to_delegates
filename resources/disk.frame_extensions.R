df_extensions_dep <- 
  c("glue", "magrittr", "disk.frame", "furrr", "progressr", "pbmcapply")

df_extensions_dep_miss <- 
  df_extensions_dep[!df_extensions_dep %in% installed.packages()]

if(length(df_extensions_dep_miss) > 0){
  install.packages(df_extensions_dep_miss)
}

rm(df_extensions_dep, df_extensions_dep_miss)

################################################################################

library(glue)
library(magrittr)
library(disk.frame)
library(furrr)
library(progressr)

################################################################################

progressr::handlers(handler_progress(
  format=":spin :current/:total [:bar] :percent in :elapsed ETA: :eta",
  width=60,
  complete="="
))

################################################################################

reset_workers_df <- function(
  .pass=NULL, .change_workers=NULL, .change_fst_threads=NULL
){
  .strategy <- future::plan()
  purrr::exec(future::plan, future::sequential, .cleanup=TRUE)
  gc()
  if(is.null(.change_workers)){
    purrr::exec(future::plan, strategy=.strategy)
  }else{
    purrr::exec(future::plan, strategy=future::multisession, 
                workers=.change_workers, gc=TRUE)
  }
  if(!is.null(.change_fst_threads)){
    fst::threads_fst(.change_fst_threads)
  }
  if(!is.null(.pass)){
    return(.pass)
  }
}

################################################################################

tmp_df <- function(
  .x=NULL, .dir_temp, .mode=c("get_new", "clear_old")
){
  
  if(missing(.dir_temp)){
    if(exists("dir_temp")){
      .dir_temp <- dir_temp
    }else{
      stop("specify .dir_temp or assign to dir_temp")
    }
  }
  
  if(!exists(".tmp_df_env", where=.GlobalEnv, mode="environment")){
    .tmp_df_env <<- 
      rlang::new_environment(data=list(tmp_df_list=character(0)), 
                             parent=.GlobalEnv)
  }
  
  if(.mode[1] == "get_new"){
    
    if(!is.null(.x)){
      .tmp_df_new <- glue("{.dir_temp}/{digest::digest(.x)}.df")
    }else{
      .tmp_df_new <- glue("{.dir_temp}/{digest::digest(rnorm(1))}.df")
      .tmp_df_env$tmp_df_list <- append(.tmp_df_env$tmp_df_list, .tmp_df_new)
    }
    
    return(.tmp_df_new)
    
  }else if(.mode[1] == "clear_old"){
    
    for(.tmp_df in .tmp_df_env$tmp_df_list){
      try({fs::dir_delete(.tmp_df)}, silent=TRUE)
    }
    
  }else{
    
    stop("mode must be in c(\"get_new\", \"clear_old\")")
    
  }
  
}

################################################################################

collect_cols.df <- function(
  .df, .cols_want = c(""), .trans_fun=NULL
){
  
  .chunk_paths <- fs::dir_ls(attr(.df, "path"), regexp="/\\d+\\.fst$")
  
  # .collected_df <- purrr::map_dfr(.chunk_paths, function(..chunk_path){
  .collected_df <- furrr::future_map_dfr(.chunk_paths, function(..chunk_path){
      
      
    ..cols_have <- colnames(fst::read.fst(..chunk_path, from=1, to=1))
    ..cols_get <- .cols_want[.cols_want %in% ..cols_have]
    
    ..chunk_loaded <- fst::read.fst(..chunk_path, columns=..cols_get)
    
    if(!is.null(.trans_fun)){
      ..chunk_loaded <- dplyr::mutate_all(..chunk_loaded, .trans_fun)
    }
    
    gc()
    
    return(..chunk_loaded)
      
  })
  
  gc()
  
  return(.collected_df)
  
}

################################################################################

# disk.frame("/home/rackelhahn69/Documents/uni/masterarbeit/analysis/data/01-tweets_raw/0035_b/JoeBiden/repl.df") %>%
#   rechunk_df(.outdir="/home/rackelhahn69/Documents/uni/masterarbeit/analysis/data/01-tweets_raw/0035/JoeBiden/repl.df")


rechunk_df <- function(
  .df, .nrow_chunks=1e5, .outdir=tmp_df()
){

  .df_new <- disk.frame(.outdir)
  
  .nchunk_df_new <- ceiling(nrow(.df)/.nrow_chunks)
  .nrow_chunk_df_new <- .nrow_chunks
  
  .load <- get_chunk(.df, 1)
  .nchunk_read <- 1
  .nchunk_written <- 0
  
  while(.nchunk_read < nchunks(.df)){
    if(.nrow_chunk_df_new < nrow(.load)){
      .nchunk_written <- .nchunk_written + 1
      disk.frame::add_chunk(.df_new, .load[1:.nrow_chunk_df_new, ])
      .load <- .load[-(1:.nrow_chunk_df_new), ]
    }else{
      .nchunk_read <- .nchunk_read + 1
      .load_add <- get_chunk(.df, .nchunk_read)
      .load <- data.table::rbindlist(list(.load, .load_add), fill=TRUE)
      rm(.load_add)
    }
    gc()
  }
  
  while(.nchunk_written < .nchunk_df_new & nrow(.load) > 0){
    .nchunk_written <- .nchunk_written + 1
    .rows_to_write <- min(.nrow_chunk_df_new, nrow(.load))
    disk.frame::add_chunk(.df_new, .load[1:.rows_to_write, ])
    .load <- .load[-(1:.rows_to_write), ]
  }
  
  gc()
  
  return(.df_new)
  
}

################################################################################

resuming_cmap <- function(
  .x, .f, ..., .workers=NULL, .scheduling=1, .outdir=tmp_df(list(.x, .f)), 
  .resume=FALSE, .overwrite=FALSE, .check=TRUE
){
  
  # progressr::handlers(global=TRUE)
  # progressr::handlers(handler_progress(
  #   format=":spin :current/:total [:bar] :percent in :elapsed ETA: :eta",
  #   width=60,
  #   complete="="
  # ))
  
  # str_remove_all(str_c(capture.output(print(function(.x){return(NULL)})) , collapse=""), "[[:space:]]+")
  
  .f <- purrr::as_mapper(.f)
  .dots <- list(...)
  
  if(!disk.frame:::is_ready(.x)){stop("disk.frame .x not ready", call.=FALSE)}
  
  fs::dir_create(.outdir)
  
  .df_path_i <- fs::path_real(attr(.x, "path"))
  .df_path_o <- fs::path_real(.outdir)
  
  if(.df_path_i==.df_path_o){
    stop("input and output must have different paths")
  }
  
  .ck_file_list_i <- 
    fs::path_file(fs::dir_ls(.df_path_i, regexp="/\\d+\\.fst$"))
    
  .ck_file_list_o <- 
    fs::path_file(fs::dir_ls(.df_path_o, regexp="/\\d+\\.fst$"))
  
  .ck_file_list <- .ck_file_list_i
  
  if(length(.ck_file_list_o) > 0){
    if(isTRUE(.resume)){
      .ck_file_list <- .ck_file_list_i[!.ck_file_list_i %in% .ck_file_list_o]
    }else if(isTRUE(.overwrite)){
      fs::dir_create(fs::dir_delete(.df_path_o))
    }else{
      stop(".outdir is not empty and .resume is not TRUE")
    }
  }
  
  .furrr_opts <- furrr::furrr_options()
  
  if(!is.null(.workers)){
    .strategy_backup <- future::plan()
    if(.workers %% 1 != 0 | .workers < 0){
      stop(".workers needs to be a natural number")
    }else if(.workers %in% 0:1){
      if(.workers == 0){
        future::plan(future::transparent, gc=TRUE)
      }else{
        future::plan(future::sequential, gc=TRUE)
      }
    }else if(.workers >= 2){
      .workers <- min(.workers, future::availableCores())
      future::plan(future::multisession, workers=.workers, gc=TRUE)
    }else{
      stop("unable to set future strategy", call.=FALSE)
    }
  }
  
  if(future::nbrOfWorkers() > 1){
    .furrr_opts <- 
      furrr::furrr_options(scheduling=.scheduling, conditions=character(0), 
                           stdout=FALSE, seed=TRUE)
  }

  tryCatch({
    progressr::with_progress({
      .prog <- progressr::progressor(along=.ck_file_list)
      furrr::future_walk(.ck_file_list, function(..ck_file){
        tryCatch({
          ..ck_data <- fst::read_fst(fs::path(.df_path_i, ..ck_file))
          ..ck_data_mod <- rlang::exec(.f, ..ck_data, !!!.dots)
          if(isTRUE(nrow(..ck_data_mod) > 0)){
            fst::write_fst(data.table::as.data.table(..ck_data_mod), 
                           fs::path(.df_path_o, ..ck_file))
          }
          rm(..ck_data, ..ck_data_mod); gc()
          .prog()
        }, error=function(...error){
          stop("Error in chunk ", ..ck_file, ":", ...error$message)
        })
      }, .options=.furrr_opts)
    })
  }, finally={
    if(!is.null(.workers)){future::plan(.strategy_backup)}
  })
  
  cmap(disk.frame(.outdir), function(.chunk){.chunk; return(NULL)})
  
  return(disk.frame(.outdir))
  
}

################################################################################

bind_rows_df <- function(
  .df_list, .outdir, .chunk_size=1e4, .overwrite=TRUE, .dir_temp=tempdir()
){
  
  stopifnot(all(purrr::map_lgl(.df_list, disk.frame::is_disk.frame)))
  
  .append <- `&`(
    isTRUE(path_abs(attr(.df_list[[1]], "path")) == path_abs(.outdir)),
    isTRUE(nrow(.df_list[[1]]) > 0)
  )
  
  if(.append){
    
    .disk_frame <- pluck(.df_list, 1)
    
    .append_chunk_path_all <- 
      dir_ls(attr(.disk_frame, "path"), regexp="\\d+\\.fst$")
    
    .append_chunk_path_last <- 
      .append_chunk_path_all %>% 
      `[[`(which.max(as.integer(str_extract({.}, "\\d+(?=\\.fst$)"))))
    
    .append_chunk_back_last <- file_copy(
      .append_chunk_path_last, file_temp(tmp_dir=.dir_temp, ext=".fst")
    )
    
    .chunk_hold <- fst::read.fst(.append_chunk_back_last)
    
    .disk_frame <- 
      remove_chunk(.disk_frame, .append_chunk_path_last, full.names=TRUE)
    
    .df_list <- .df_list[-1]
    
  }else{
    
    .disk_frame <- disk.frame::disk.frame(fs::dir_create(.outdir))
    .chunk_hold <- tibble::tibble()

  }
  
  .files <- 
    .df_list %>% 
    purrr::map(function(.df){
      fs::dir_ls(attr(.df, "path"), regexp="[[:digit:]]+\\.fst")
    }) %>% 
    tibble::as_tibble_col("path_in") %>%
    tibble::rowid_to_column("id_df") %>% 
    tidyr::unnest(path_in) %>% 
    dplyr::mutate(
      id_file = 
        path_in %>% 
        fs::path_file() %>% 
        stringr::str_extract("^[[:digit:]]+") %>% 
        as.integer()
    ) %>% 
    dplyr::arrange(id_df, id_file) %>% 
    purrr::pluck("path_in", .default=character(0L))
  
  tryCatch({
  
    for(.file_id in seq_along(.files)){
      
      .file_i_done <- FALSE
      .file_i_nrow_total <- purrr::pluck(
        fst::fst.metadata(.files[[.file_id]]), "nrOfRows", 
        .default=0L
      )
      .file_i_nrow_copied <- 0L
      
      while(isFALSE(.file_i_done)){
        
        .file_i_nrow_left <- .file_i_nrow_total - .file_i_nrow_copied
        
        .chunk_hold_nrow_free <- .chunk_size - nrow(.chunk_hold)
        
        if(.chunk_hold_nrow_free > 0){
          
          .chunk_temp <- fst::read.fst(
            path=.files[[.file_id]],
            from=`+`(.file_i_nrow_copied, 1L),
            to=`+`(.file_i_nrow_copied, 
                   min(.file_i_nrow_left, .chunk_hold_nrow_free))
          )
          
          .chunk_hold <- dplyr::bind_rows(.chunk_hold, .chunk_temp)
          
          .file_i_nrow_copied <- .file_i_nrow_copied + nrow(.chunk_temp)
          
        }
        
        if(nrow(.chunk_hold) == .chunk_size | .file_id == length(.files)){
          .disk_frame <- add_chunk(.disk_frame, .chunk_hold)
          .chunk_hold <- tibble()
        }
        
        if(.file_i_nrow_copied == .file_i_nrow_total){
          .file_i_done <- TRUE
        }
        
      }
      
    }
    
    invisible(cmap(
      .disk_frame, function(..chunk){invisible(..chunk); return(NULL)},
      lazy=FALSE
    )); gc()
    
  }, error=function(..error){
    if(.append){
      file_delete(
        discard(dir_ls(attr(.disk_frame, "path")), `%in%`, .append_chunk_path_all)
      )
      file_copy(
        .append_chunk_back_last, .append_chunk_path_last, overwrite=TRUE
      )
    }
    stop(..error)
  }, interrupt=function(..interrupt){
    if(.append){
      file_delete(
        discard(dir_ls(attr(.disk_frame, "path")), `%in%`, .append_chunk_path_all)
      )
      file_copy(
        .append_chunk_back_last, .append_chunk_path_last, overwrite=TRUE
      )
    }
    stop("Interrupted by the user")
  })
    
  gc()
    
  return(.disk_frame)
    
}
