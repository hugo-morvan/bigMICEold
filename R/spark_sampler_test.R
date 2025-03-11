# testing the sampler.spark function

library(sparklyr)
library(dplyr)
library(ggplot2)

conf <- spark_config()
conf$`sparklyr.shell.driver-memory`<- "128G"
conf$spark.memory.fraction <- 0.6
conf$`sparklyr.cores.local` <- 24

sc = spark_connect(master = "local", config = conf)

path_DORS =     "/vault/hugmo418_amed/NDR-SESAR/Uttag Socialstyrelsen 2024/ut_r_par_ov_63851_2023.csv"
path_NDR =      "/vault/hugmo418_amed/NDR-SESAR/Uttag SCB+NDR+SESAR 2024/FI_Lev_NDR.csv"
path_SESAR_FU = "/vault/hugmo418_amed/NDR-SESAR/Uttag SCB+NDR+SESAR 2024/FI_Lev_SESAR_FU.csv"
path_SESAR_IV = "/vault/hugmo418_amed/NDR-SESAR/Uttag SCB+NDR+SESAR 2024/FI_Lev_SESAR_IV.csv"
path_SESAR_TS = "/vault/hugmo418_amed/NDR-SESAR/Uttag SCB+NDR+SESAR 2024/FI_Lev_SESAR_TS.csv"
path_small_SESAR_IV = "/vault/hugmo418_amed/subsets_thesis_hugo/small_IV.csv"

fromto <- c(1,3) # Number of iterations

data_small <- spark_read_csv(sc, name = "df",path = path_small_SESAR_IV,infer_schema = TRUE, null_value = 'NA')
#Filter out Date data type
features <- names(sdf_schema(data_small))
cols <- sdf_schema(data_small)
filtered_features<- features[sapply(cols[features], function(x) !x$type %in% c("StringType", "DateType", "TimestampType"))]
data <- data_small %>% select(all_of(filtered_features)) %>%
  select(-"IV_TherapyBilevelPAPX") %>%
  select(-"IV_TherapySplintX") %>%
  select(-"IV_TherapySurgeryX") %>%
  select(-"IV_TherapyWeightX") %>%
  select(-"IV_PositionTherapyX")
imp_init <- impute_with_random_samples(sc, data)

mice_imputed <- sampler.spark(sc, data, imp_init, fromto)



data_big <- spark_read_csv(sc, name = "bdf", path = path_SESAR_IV, infer_schema = TRUE, null_value = 'NA')
features_big <- names(sdf_schema(data_big))
cols_big <- sparklyr::sdf_schema(data_big)
filtered_big_features <- features_big[sapply(cols[features_big], function(x) !x$type %in% c("StringType", "DateType", "TimestampType"))]
big_data <- data_big %>% select(all_of(filtered_big_features))
impt_init_big <- impute_with_random_samples(sc, big_data)



#Initialize imputation

# Monitor runtime

code_to_be_monitored <- function(x) {

  mice_imputed <- sampler.spark(sc, data, imp_init, fromto)
}

# Monitor the function
results <- monitor_memory(
  code_to_be_monitored,    # The function to monitor
  x = NULL,    # Your function's parameters
  sampling_interval = 0.1,  # How often to sample (in seconds)
  pre_time = 1,           # How long to monitor before (in seconds)
  post_time = 1          # How long to monitor after (in seconds)
)


