
# get_replies_delegates

## Overview

Scripts that can be used to collect all Twitter coversations started by
members of the US congress. The replies are collected via the [standard
search (Version
1.1)](https://developer.twitter.com/en/docs/twitter-api/v1/tweets/search/api-reference/get-search-tweets)
API endpoint. As my access to historical data through this API enpoint
is limited to the past \~7 days, the script has to be executed
repeatedly.

## …

``` r

library(tidyverse)

dir_data <- fs::dir_create("~/Documents/get_tweets/data")

tweets_db <- DBI::dbConnect(
  drv=RSQLite::SQLite(), 
  dbname=fs::path(dir_data, "tweets", ext="db"),
  extended_types=TRUE
)

DBI::dbListTables(tweets_db)
#> [1] "tweets_raw"
```

``` r

tbl(tweets_db, "tweets_raw")
#> # Source:   table<tweets_raw> [?? x 25]
#> # Database: sqlite 3.39.1 [/mnt/21f3fcbc-9669-4c84-afae-5205d7a138ad/Documents/get_tweets/data/tweets.db]
#>   tweet_st…¹ tweet…² tweet…³ tweet…⁴ tweet…⁵ tweet_created_at    tweet…⁶ tweet…⁷
#>      <int64> <int64> <chr>     <int> <chr>   <dttm>              <chr>   <chr>  
#> 1       1e18    1e18 AlexPa…  8.25e7 AlexPa… 2022-05-25 16:41:20 en      "Anoth…
#> 2       1e18    1e18 AlexPa…  8.25e7 AlexPa… 2022-05-24 19:54:37 en      "Title…
#> 3       1e18    1e18 AlexPa…  8.25e7 AlexPa… 2022-05-24 16:41:31 en      "Encou…
#> 4       1e18    1e18 AlexPa…  8.25e7 AlexPa… 2022-05-24 01:05:02 en      "Formu…
#> 5       1e18    1e18 AlexPa…  8.25e7 AlexPa… 2022-05-23 21:30:05 en      "A rem…
#> # … with more rows, 17 more variables: tweet_reply_to_status_id <int>,
#> #   tweet_reply_to_user_id <int>, tweet_reply_to_user_screen_name <chr>,
#> #   tweet_retweet_status_id <int>, tweet_retweet_created_at <dttm>,
#> #   tweet_retweet_user_id <int>, tweet_retweet_user_screen_name <chr>,
#> #   tweet_quote_status_id <int>, tweet_quote_created_at <dttm>,
#> #   tweet_quote_user_id <int>, tweet_quote_user_screen_name <chr>,
#> #   tweet_quote_text <chr>, tweet_url_json <chr>, tweet_media_int_json <chr>, …
#> # ℹ Use `print(n = ...)` to see more rows, and `colnames()` to see all variable names

pull(tally(tbl(tweets_db, "tweets_raw")), n)
#> [1] 19925888
```
