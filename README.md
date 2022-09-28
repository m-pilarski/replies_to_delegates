
# get_replies_delegates

## Overview

Scripts that can be used to collect all Twitter coversations started by
members of the US congress. The replies are collected via the [standard
search (Version
1.1)](https://developer.twitter.com/en/docs/twitter-api/v1/tweets/search/api-reference/get-search-tweets)
API endpoint. As my access to historical data through this API enpoint
is limited to the past \~7 days, the script has to be executed
repeatedly.

## Repeated Data Collection

1.  [get_tweets_raw](https://github.com/m-pilarski/replies_to_delegates/blob/master/01.01-get_tweets_raw.R)
    -   replies are collected via the [standard search (Version
        1.1)](https://developer.twitter.com/en/docs/twitter-api/v1/tweets/search/api-reference/get-search-tweets)
        API endpoint
    -   the collected tweets are stored in a SQLite Database

## Exemplary results

``` r

library(tidyverse)
library(DBI)

dir_data <- "~/Documents/get_tweets/data"

tweets_db <- dbConnect(
  drv=RSQLite::SQLite(), 
  dbname=fs::path(dir_data, "tweets", ext="db"),
  extended_types=TRUE
)

dbListTables(tweets_db)
#> [1] "convers_info"        "tweets_clean"        "tweets_clean_pruned"
#> [4] "tweets_raw"
```

``` r

tbl(tweets_db, "tweets_raw")
#> # Source:   table<tweets_raw> [?? x 26]
#> # Database: sqlite 3.39.1 [/mnt/21f3fcbc-9669-4c84-afae-5205d7a138ad/Documents/get_tweets/data/tweets.db]
#>   tweet_id convers…¹ conve…² conve…³ user_id user_…⁴ tweet_date          tweet…⁵
#>    <int64>   <int64> <chr>     <int>   <int> <chr>   <dttm>              <chr>  
#> 1     1e18      1e18 AlexPa…  8.25e7  8.25e7 AlexPa… 2022-05-25 16:41:20 en     
#> 2     1e18      1e18 AlexPa…  8.25e7  8.25e7 AlexPa… 2022-05-24 19:54:37 en     
#> 3     1e18      1e18 AlexPa…  8.25e7  8.25e7 AlexPa… 2022-05-24 16:41:31 en     
#> 4     1e18      1e18 AlexPa…  8.25e7  8.25e7 AlexPa… 2022-05-24 01:05:02 en     
#> 5     1e18      1e18 AlexPa…  8.25e7  8.25e7 AlexPa… 2022-05-23 21:30:05 en     
#> # … with more rows, 18 more variables: tweet_text <chr>,
#> #   tweet_reply_tweet_id <int>, tweet_reply_user_id <int>,
#> #   tweet_reply_user_screen_name <chr>, tweet_retweet_tweet_id <int>,
#> #   tweet_retweet_tweet_date <dttm>, tweet_retweet_user_id <int>,
#> #   tweet_retweet_user_screen_name <chr>, tweet_quote_tweet_id <int>,
#> #   tweet_quote_tweet_date <dttm>, tweet_quote_user_id <int>,
#> #   tweet_quote_user_screen_name <chr>, tweet_quote_text <chr>, …

# Number of rows
pull(tally(tbl(tweets_db, "tweets_raw")), n)
#> [1] 29244142
```

``` r

tweets_count <- local({
  
  .tweets_count_chunk_size <- 1e6L
  
  .tweets_count_statement <- str_c(
    "SELECT `convers_user_screen_name`, `tweet_date` ",
    "FROM `tweets_raw`"
  )
  
  .tweets_count <- tibble()
  
  .tweets_count_query <- DBI::dbSendQuery(
    conn=tweets_db, statement=.tweets_count_statement
  )
  
  repeat{
    
    .chunk_raw <- dbFetch(res=.tweets_count_query, n=.tweets_count_chunk_size)
    
    if(nrow(.chunk_raw) == 0){break()}
    
    .tweets_count <- 
      .chunk_raw %>% 
      count(
        convers_user_screen_name,
        tweet_date_day = lubridate::as_date(tweet_date), 
        name="count"
      ) %>% 
      bind_rows(.tweets_count) %>% 
      group_by(across(-count)) %>% 
      summarize(across(count, sum), .groups="drop")
    
    gc()
    
  }
  
  dbClearResult(.tweets_count_query)
  
  .delegates_info <- 
    tbl(tweets_db, "convers_info") %>% 
    select(
      convers_user_screen_name, 
      convers_user_party = party,
      convers_user_role = role
    ) %>% 
    collect()
  
  .tweets_count <- left_join(
    .tweets_count, .delegates_info, by="convers_user_screen_name"
  )
  
  return(.tweets_count)
  
})

tweets_count %>% 
  group_by(convers_user_party, tweet_date_day) %>% 
  summarise(across(count, sum), .groups="drop") %>% 
  mutate(count = modify_if(
    as.double(count), convers_user_party == "republican", `*`, -1
  )) %>% 
  filter(tweet_date_day > lubridate::ymd("2022-06-01")) %>% 
  ggplot(
    aes(x=tweet_date_day, y=count, fill=convers_user_party)
  ) + 
  geom_col(width=1) +
  scale_y_continuous(
    labels=function(.b){scales::number(abs(.b))},
    limits=function(.l){max(abs(.l)) * c(-1, 1)}
  ) +
  rcartocolor::scale_fill_carto_d(palette="Safe") +
  theme(legend.position="bottom")
```

<img src="README_files/figure-gfm/unnamed-chunk-4-1.png" width="100%" />
