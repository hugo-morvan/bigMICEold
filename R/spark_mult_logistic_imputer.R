impute_with_mult_logistic_regression <- function(sc, sdf, target_col, feature_cols) {
  # Given a spark connection, a spark dataframe, a target column with missing values,
  # and feature columns without missing values, this function:
  # 1. Builds a multinomial logistic regression model using complete cases
  # 2. Uses that model to predict missing values
  # 3. Returns a dataframe with imputed values in the target column

  # Validate inputs
  if (!is.character(target_col) || length(target_col) != 1) {
    stop("target_col must be a single column name as a character string")
  }
  if (!is.character(feature_cols) || length(feature_cols) == 0) {
    stop("feature_cols must be a character vector of column names")
  }

  # Create a unique temporary table name to avoid conflicts with multiple calls
  temp_table_name <- paste0("temp_imputation_data_", floor(runif(1) * 1000000))
  complete_table_name <- paste0("complete_data_", floor(runif(1) * 1000000))
  incomplete_table_name <- paste0("incomplete_data_", floor(runif(1) * 1000000))

  # Keep track of tables we create so we can clean them up
  created_tables <- c()

  # Function to clean up temp tables (call this before any return)
  cleanup <- function() {
    for (table in created_tables) {
      tryCatch({
        DBI::dbExecute(sc, paste0("DROP VIEW IF EXISTS ", table))
        sparklyr::tbl_uncache(sc, table)
      }, error = function(e) {
        # Silently ignore errors during cleanup
      })
    }
  }

  # Register the input dataframe as a temp view
  tryCatch({
    sdf %>%
      sparklyr::sdf_register(temp_table_name)
    created_tables <- c(created_tables, temp_table_name)

    # Step 1: Split data into complete and incomplete rows
    # Get rows with non-null target values for training
    complete_data <- sdf %>%
      dplyr::filter(!is.na(!!rlang::sym(target_col))) %>%
      sparklyr::sdf_register(complete_table_name)
    created_tables <- c(created_tables, complete_table_name)

    # Get rows with null target values for prediction
    incomplete_data <- sdf %>%
      dplyr::filter(is.na(!!rlang::sym(target_col))) %>%
      sparklyr::sdf_register(incomplete_table_name)
    created_tables <- c(created_tables, incomplete_table_name)

    # Step 2: Check if we have enough data for training
    complete_count <- complete_data %>% dplyr::count() %>% dplyr::pull(n)

    if (complete_count < 10) {  # Minimum threshold for meaningful regression
      message("Imputation impossible, not enough data")
      cleanup()
      return(sdf)
    } else {
      # Step 3: Build regression formula
      formula_str <- paste0(target_col, " ~ ", paste(feature_cols, collapse = " + "))
      formula_obj <- as.formula(formula_str)

      # Step 4: Build multinomial logistic regression model on complete data
      model <- complete_data %>%
        ml_logistic_regression(formula = formula_obj)

      # Step 5: Predict missing values
      if (sparklyr::sdf_nrow(incomplete_data) > 0) {
        # Make predictions
        predictions <- ml_predict(model, incomplete_data)

        # Get all columns from the predictions dataframe
        pred_cols <- colnames(predictions)

        # Remove any conflicting columns that might cause the duplicate error
        cols_to_keep <- pred_cols[!pred_cols %in% c(target_col, "rawPrediction", "probability")]

        # Now do a direct update of the target column with the prediction
        incomplete_data_with_predictions <- predictions %>%
          dplyr::select(dplyr::all_of(cols_to_keep)) %>%
          dplyr::rename(!!rlang::sym(target_col) := prediction)

        # Step 6: Combine complete and imputed data
        result <- complete_data %>%
          dplyr::union_all(incomplete_data_with_predictions)
      } else {
        result <- complete_data
      }

      # Step 7: Clean up temp tables before returning
      cleanup()

      return(result)
    }
  }, error = function(e) {
    # Clean up on error
    cleanup()
    stop(paste("Error in imputation:", e$message))
  })
}
# TESTING

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

cols <- sparklyr::sdf_schema(data_small)

label_col = "IV_VeriByM" # 0, 1 or 9

features_col <- setdiff(names(cols), label_col)

imputed_sdf <- impute_with_random_samples(sc, data_small)

# replace random sample values in IV_height with the original missing values
df_missing_height <- imputed_sdf %>%select(-label_col) %>% cbind(data_small %>% select(all_of(label_col)))

df_missing_height
df_missing_height %>% select(label_col)

#Filter out Date data type
features_col <- features_col[sapply(cols[features_col],
                                    function(x) !x$type %in% c("StringType", "DateType", "TimestampType"))]

#initialize the feature column
#df_missing_height <- df_missing_height %>%
#  sparklyr::ft_vector_assembler(input_cols = features_col, output_col = "features")

#Call the imputer
imputed_missing_height <- impute_with_mult_logistic_regression(sc, df_missing_height, label_col, features_col)

imputed_missing_height %>% select(label_col)

#spark_disconnect(sc)
