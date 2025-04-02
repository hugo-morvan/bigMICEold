#install.packages("devtools")
#devtools::install_github("hugo-morvan/bigMice")
library(bigMice)
#install.packages("sparklyr")
library(sparklyr)
library(dplyr)


conf <- spark_config()
conf$`sparklyr.shell.driver-memory`<- "256G"
conf$spark.memory.fraction <- 0.8
conf$`sparklyr.cores.local` <- 32

sc = spark_connect(master = "local", config = conf)

path_DORS =     "/vault/hugmo418_amed/NDR-SESAR/Uttag Socialstyrelsen 2024/ut_r_par_ov_63851_2023.csv"
path_NDR =      "/vault/hugmo418_amed/NDR-SESAR/Uttag SCB+NDR+SESAR 2024/FI_Lev_NDR.csv"
path_SESAR_FU = "/vault/hugmo418_amed/NDR-SESAR/Uttag SCB+NDR+SESAR 2024/FI_Lev_SESAR_FU.csv"
path_SESAR_IV = "/vault/hugmo418_amed/NDR-SESAR/Uttag SCB+NDR+SESAR 2024/FI_Lev_SESAR_IV.csv"
path_SESAR_TS = "/vault/hugmo418_amed/NDR-SESAR/Uttag SCB+NDR+SESAR 2024/FI_Lev_SESAR_TS.csv"
path_small_SESAR_IV = "/vault/hugmo418_amed/subsets_thesis_hugo/small_IV.csv"

data_small <- spark_read_csv(sc, name="df", path=path_small_SESAR_IV, infer_schema=TRUE, null_value='NA')
#data_small <- spark_read_csv(sc, name = "df",path = path_SESAR_FU,infer_schema = TRUE, null_value = 'NA')


# In  lopNr and SenPNr contain missing values, so i remove those columns for simplicity
# I also convert the boolean binary variable into 0/1 binaries format.
# This allows for more flexibility in the model selection
data_small <- data_small %>%
  select(-c("LopNr","IV_SenPNr")) %>%
  select(sort(colnames(.))) %>% # Order alphabetically
  mutate(IV_AtrialFibrillation2 = as.numeric(IV_AtrialFibrillation2)) %>%
  mutate(IV_CerebrovascDisease2 = as.numeric(IV_CerebrovascDisease2)) %>%
  mutate(IV_CopdAsthma2 = as.numeric(IV_CopdAsthma2)) %>%
  mutate(IV_CoronaryHeartDisease2 = as.numeric(IV_CoronaryHeartDisease2)) %>%
  mutate(IV_Depression2 = as.numeric(IV_Depression2)) %>%
  mutate(IV_Diabetes2 = as.numeric(IV_Diabetes2)) %>%
  mutate(IV_HeartFailure2 = as.numeric(IV_HeartFailure2)) %>%
  mutate(IV_Hypertension2 = as.numeric(IV_Hypertension2)) %>%
  mutate(IV_TherapyCPAPX = as.numeric(IV_TherapyCPAPX)) %>%
  mutate(IV_TherapyBilevelPAPX = as.numeric(IV_TherapyBilevelPAPX)) %>%
  mutate(IV_TherapySplintX = as.numeric(IV_TherapySplintX)) %>%
  mutate(IV_TherapySurgeryX = as.numeric(IV_TherapySurgeryX))%>%
  mutate(IV_TherapyWeightX = as.numeric(IV_TherapyWeightX))%>%
  mutate(IV_PositionTherapyX = as.numeric(IV_PositionTherapyX))


dataset_info_IV = "Datasets info - Sesar_IV.csv"
data_info <- read.csv(dataset_info_IV)

get_named_vector <- function(csv_file) {
  # Helper function to extract the named datatypes of a dataset.
  data <- read.csv(csv_file, stringsAsFactors = FALSE)
  named_vector <- setNames(data$Variable.type, data$Variable.Name)
  names(named_vector)[names(named_vector) == "Age"] <- "IV_Age"
  ordered_vector <- named_vector[order(names(named_vector))] #Order alphabet.
  return(ordered_vector)
}

variable_types <- get_named_vector(dataset_info_IV)
print(variable_types)
variable_types <- variable_types[!(names(variable_types) %in% c("LopNr", "SenPNr"))]

colnames(data_small)


init_dict <- c("Binary" = "mode",
               "Nominal" = "mode",
               "Ordinal" = "mode",
               "Code (don't impute)" = "none", # LopNr, Unit_code etc...
               "Continuous_int" = "median",
               "Continuous_float" = "mean",
               "smalldatetime" = "none",
               "String" = "none", #TBD
               "Count" = "median", #TBD
               "Semi-continuous" = "none", #TBD
               "Else", "none")

impute_modes <- replace(variable_types, variable_types %in% names(init_dict), init_dict[variable_types])

#data_small %>% sdf_persist(name = "data")

imput_init <- bigMice::impute_with_MeMoMe(sc, data_small, impute_mode = impute_modes)

#imput_init %>% sdf_persist()

# Example analysis: CoronaryDisease2 ~ BMI + Age on full cases only
example_data <- data_small %>%
  select(c("IV_CoronaryHeartDisease2","IV_Age","IV_BMI_Calculated")) %>%
  na.omit()

print(sdf_nrow(example_data))

formula_obj <- as.formula("IV_CoronaryHeartDisease2 ~ IV_Age + IV_BMI_Calculated")
model_ex <- example_data %>%
  ml_logistic_regression(formula = formula_obj)

print(model_ex$coefficients)
#print(model_ex$response)
#print(model_ex$summary) # same as ml_summary(model_ex)
print(model_ex$summary$accuracy())
print(model_ex$summary$area_under_roc())
names(model_ex$coefficients)

# Example analysis: CoronaryDisease2 ~ BMI + Age on mean/meadian imputed values
example_data2 <- imput_init %>%
  select(c("IV_CoronaryHeartDisease2","IV_Age","IV_BMI_Calculated")) %>%
  na.omit()

print(sdf_nrow(example_data2))

formula_obj <- as.formula("IV_CoronaryHeartDisease2 ~ IV_Age + IV_BMI_Calculated")
model_ex <- example_data2 %>%
  ml_logistic_regression(formula = formula_obj)

print(model_ex$coefficients)
#print(model_ex$response)
#print(model_ex$summary) # same as ml_summary(model_ex)
print(model_ex$summary$accuracy())
print(model_ex$summary$area_under_roc())
names(model_ex$coefficients)



imputed_results = bigMice::mice.spark(data_small,
                                       sc,
                                       variable_types,
                                       analysis_formula = formula_obj,
                                       m = 2,
                                       maxit = 2)


imputed_results$imputation_stats
imputed_results$model_params
imputed_results$rubin_stats
model_ex$coefficients

#spark_session(sc) %>% invoke("catalog") %>% invoke("clearCache")
#spark_disconnect(sc)
