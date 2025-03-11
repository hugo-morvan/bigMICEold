# load SESAR_TS dummy data
library(sparklyr)
library(dplyr)

sc <- spark_connect(master = "local")

dummy_data <- spark_read_csv(sc, name = "df",
      path = "C:\\Users\\hugom\\Desktop\\CAMEO\\Code\\sesar_dummy_100.csv",
      infer_schema = TRUE)

holy_data <- spark_read_csv(sc, name = "df_holy",
                             path = "C:\\Users\\hugom\\Desktop\\CAMEO\\Code\\sesar_dummy_100_020MVR.csv",
                             infer_schema = TRUE)
r_holy_data <- read.csv("C:\\Users\\hugom\\Desktop\\CAMEO\\Code\\sesar_dummy_100_020MVR.csv")

res <- mice_half_spark(r_holy_data)

res <- mice(r_holy_data)

res$predictorMatrix
heatmap(res$predictorMatrix)

class(dummy_data)

cols <- sparklyr::sdf_schema(dummy_data)

#print unique column types
unique_types <- unique(sapply(cols, function(x) x$type))
# Column to impute
label_col <- "FU_SplintStill"
label_col <- "FU_CPAPStill" # Three classes (0, 1, 9)
# PROBLEM: Having 9 instead of 2 makes the logistic regression think there are 10 classes
# SOLUTION: Replace 9 with 2 ?

# Other binary columns: FU_SplintAdverse2, FU_SplintStill

# Column to use as features (all others)
features_col <- setdiff(names(cols), label_col)
# features_col <- features_col[sapply(cols[features_col],
#                                     function(x) x$type %in% c("IntegerType", "DoubleType"))]

# Drop String type, DateType, TimestampType columns
features_col <- features_col[sapply(cols[features_col],
                                    function(x) !x$type %in% c("StringType", "DateType", "TimestampType"))]

#Drop the features that are not IntegerType
# dummy_data <- dummy_data %>% select(any_of(c(features_col, label_col)))

#Convert the label column to a binary column
# dummy_data <- dummy_data %>% mutate(FU_SplintStill = ifelse(FU_SplintStill == 1, 1, 0))

# Assemble multiple features into a single vector column
dummy_data <- dummy_data %>%
  ft_vector_assembler(input_cols = features_col, output_col = "features")



weight_col <- NULL

# model_formula <- ft_r_formula(x = dummy_data, formula = FU_SplintStill ~ .)
# print(model_formula$lazy_query$x)
# print(model_formula$lazy_query$vars)
# as.character(model_formula)

# Cannot have missing values in the input columns => needs to pre-fill them, but remember which were pre-filled

model <- ml_logistic_regression(
  x = dummy_data,
  # formula = FU_SplintStill ~ .,
  features_col = "features", # Needs to be a vector column with all the features (created via ft_vector_assembler)
  label_col = label_col,
  family = "auto",
  # weight_col = weight_col, # weights = w[ry],
  max_iter = 100
)
# The object returned depends on the class of x.
# If it is a spark_connection, the function returns a ml_estimator object.
# If it is a ml_pipeline, it will return a pipeline with the predictor appended to it.
# If a tbl_spark, it will return a tbl_spark with the predictions added to it.
# TODO Run some test to understand of this all work. you can do it Hugo
fit <- ml_fit(model) #?
model_summary <- summary(model)
print(model_summary)
pred <- ml_predict(fit, test_data)


# %%%%%%%%%%%%%%%%% CART %%%%%%%%%%%%%%%%%%%

library(sparklyr)
library(dplyr)

sc <- spark_connect(master = "local")

dummy_data <- spark_read_csv(sc, name = "df",
                             path = "C:\\Users\\hugom\\Desktop\\CAMEO\\Code\\sesar_dummy_100.csv",
                             infer_schema = TRUE)

class(dummy_data)

cols <- sparklyr::sdf_schema(dummy_data)

label_col = "FU_SplintStill"

features_col <- setdiff(names(cols), label_col)

features_col <- features_col[sapply(cols[features_col],
                                    function(x) !x$type %in% c("StringType", "DateType", "TimestampType"))]
dummy_data <- dummy_data %>%
  ft_vector_assembler(input_cols = features_col, output_col = "features")



model <- ml_decision_tree(
  x = dummy_data,
  type = "auto",
  features_col = "features",
  label_col = label_col,
  max_depth = 5
)

minimal_features <- head(features_col, 2)  # Use only the first two features
dummy_data_minimal <- dummy_data %>%
  ft_vector_assembler(input_cols = minimal_features, output_col = "minimal_features")

model_minimal <- ml_decision_tree(
  x = dummy_data_minimal,
  type = "auto",
  features_col = "minimal_features",
  label_col = label_col,
  max_depth = 3
)

label_col %in% names(cols)

# %%%%%%%%%%%%%%%%% Linear Regression %%%%%%%%%%%%%%%%%%%

library(sparklyr)
library(dplyr)

sc <- spark_connect(master = "local")

dummy_data <- spark_read_csv(sc, name = "df",
                             path = "C:\\Users\\hugom\\Desktop\\CAMEO\\Code\\sesar_dummy_100.csv",
                             infer_schema = TRUE)

class(dummy_data)

cols <- sparklyr::sdf_schema(holy_data)

label_col = "IV_Weight"

features_col <- setdiff(names(cols), label_col)

features_col <- features_col[sapply(cols[features_col],
                                    function(x) !x$type %in% c("StringType", "DateType", "TimestampType"))]
dummy_data <- dummy_data %>%
  ft_vector_assembler(input_cols = features_col, output_col = "features")



model <- ml_linear_regression(
  x = dummy_data,
  features_col = "features",
  label_col = label_col,
)

coefficients <- model$coefficients
intercept <- model$intercept

model_summary <- model$summary
print(model_summary)
names(model_summary)
for(name in names(model_summary) ){
  print(name)
  print(model_summary[[name]])
}
model_summary$r2
model_summary$deviance_residuals()

model_summary$residuals()

imputations <- ml_predict(model, dummy_data)





library(sparklyr)
library(dplyr)


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

data_small <- spark_read_csv(sc, name = "df",path = path_small_SESAR_IV,infer_schema = TRUE, null_value = 'NA')
data_big <- spark_read_csv(sc, name="bigdf", path = path_SESAR_IV, infer_schema = TRUE, null_value = "NA" )
data_big

where <- make.where.spark(data, keyword = "missing")
where

imputed_sdf <- impute_with_random_samples(sc, data_big)
spark_disconnect(sc)

impute_with_random_samples <- function(sc, sdf, columns = NULL) {
  # Given a spark connection sc, a spark dataframe sdf and optional column names
  # return a spark dataframe where missing values are replaced with random samples
  # from the observed values.

  # If columns not specified, use all columns
  if (is.null(columns)) {
    columns <- colnames(sdf)
  }

  # Process each column
  for (col_name in columns) {
    cat(col_name, " - ")
    # Create a temporary view of the dataframe
    sdf %>% sparklyr::spark_dataframe() %>%
      invoke("createOrReplaceTempView", paste0("temp_", col_name))

    # Get the column data type
    col_type <- sdf %>%
      sparklyr::sdf_schema() %>%
      purrr::keep(~ .x$name == col_name) %>%
      purrr::pluck(1, "type")

    # SQL query to collect non-null values for sampling
    sample_values_query <- paste0(
      "SELECT ", col_name, " FROM temp_", col_name,
      " WHERE ", col_name, " IS NOT NULL"
    )

    # Get distinct values for sampling
    sample_values <- DBI::dbGetQuery(sc, sample_values_query)

    # If there are no non-null values, skip this column
    if (nrow(sample_values) == 0) {
      warning(paste0("Column '", col_name, "' has no non-null values. Skipping."))
      next
    }

    # Create a temporary table with the sample values
    sdf_sample <- sparklyr::copy_to(
      sc,
      sample_values,
      paste0("sample_", col_name),
      overwrite = TRUE
    )

    # SQL to replace nulls with random samples
    # We use the rand() function to randomly select values
    random_sample_sql <- paste0(
      "SELECT a.*, ",
      "CASE WHEN a.", col_name, " IS NULL ",
      "THEN b.", col_name, " ",
      "ELSE a.", col_name, " END AS ", col_name, "_imputed ",
      "FROM temp_", col_name, " a ",
      "CROSS JOIN (SELECT * FROM sample_", col_name,
      " ORDER BY rand() LIMIT 1) b"
    )

    # Apply the imputation
    sdf <- DBI::dbGetQuery(sc, random_sample_sql) %>%
      sparklyr::copy_to(sc, ., "temp_result", overwrite = TRUE) %>%
      dplyr::select(-!!rlang::sym(col_name)) %>%
      dplyr::rename(!!rlang::sym(col_name) := !!rlang::sym(paste0(col_name, "_imputed")))

    # Clean up temporary views
    DBI::dbExecute(sc, paste0("DROP VIEW IF EXISTS temp_", col_name))
    sparklyr::tbl_uncache(sc, paste0("sample_", col_name))
  }

  return(sdf)
}
