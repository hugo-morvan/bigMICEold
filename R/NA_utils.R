# Utils file for NA related spark functions

count_na <- function(data){
  data %>%
    summarise(across(everything(), ~ sum(as.integer(is.na(.))), .names = "{.col}")) %>% collect()
}

is.na.spark <- function(data) {
  sum(count_na(data)) > 0
}

na_locations <- function(data) {
  data %>%
    summarise(across(everything(), ~ is.na(.)))
}

