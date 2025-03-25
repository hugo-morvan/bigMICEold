impute_with_mean_mode <- function(sc, sdf, column = NULL) {
  # Given a spark connection, a spark dataframe, and an optional column name:
  # 1. If column is specified, imputes missing values in that column only
  # 2. If column is NULL, imputes missing values in all columns
  # 3. Imputation is done using mean for numeric columns and mode for categorical columns
  # 4. Returns a dataframe with imputed values

  # Determine which columns to process
  cols_to_process <- if (!is.null(column)) {
    if (!is.character(column) || length(column) == 0) {
      stop("column must be a character string")
    }
    column
  } else {
    colnames(sdf)
  }

  # Start with the original dataframe
  result <- sdf

  for (col in cols_to_process) {
    cat("\n", col, "- ")

    # Skip columns that don't exist
    if (!(col %in% colnames(result))) {
      warning(paste("Column", col, "not found in dataframe. Skipping."))
      next
    }

    # Split into complete and incomplete data for this column
    complete_data <- result %>%
      dplyr::filter(!is.na(!!rlang::sym(col)))

    incomplete_data <- result %>%
      dplyr::filter(is.na(!!rlang::sym(col)))

    # If no missing values or no observed values, skip this column
    if (sdf_nrow(incomplete_data) == 0) {
      cat(" No missing values found. Skipping.\n")
      next
    }

    if (sdf_nrow(complete_data) == 0) {
      cat(" No observed values found. Skipping.\n")
      next
    }

    # Get column type information
    schema <- sdf_schema(sdf)
    col_type <- setNames(sapply(schema, `[[`, "type"), sapply(schema, `[[`, "name"))

    # Impute based on data type
    if (grepl("DoubleType|float|IntegerType|long|decimal", col_type, ignore.case = TRUE)[1]) {
      # Numeric column - impute with mean
      cat(" Imputing numeric data with mean\n")

      # Calculate mean of the column
      mean_value <- complete_data %>%
        dplyr::summarize(mean_val = mean(!!rlang::sym(col), na.rm = TRUE)) %>%
        dplyr::collect() %>%
        dplyr::pull(mean_val)

      cat(" Mean value:", mean_value, "\n")

      # Create a dataframe with the mean value for this column
      mean_df <- incomplete_data %>%
        dplyr::mutate(!!rlang::sym(col) := mean_value)

      # Union the incomplete data (now with imputed values) with the complete data
      result <- complete_data %>%
        dplyr::union_all(mean_df)

    } else {
      # Categorical column - impute with mode
      cat(" Imputing categorical data with mode\n")

      # Calculate mode (most frequent value) of the column
      mode_value <- complete_data %>%
        dplyr::group_by(!!rlang::sym(col)) %>%
        dplyr::summarize(count = n()) %>%
        dplyr::arrange(desc(count)) %>%
        dplyr::collect() %>%
        head(1) %>%
        dplyr::pull(!!rlang::sym(col))

      cat(" Mode value:", as.character(mode_value), "\n")

      # Create a dataframe with the mode value for this column
      mode_df <- incomplete_data %>%
        dplyr::mutate(!!rlang::sym(col) := mode_value)

      # Union the incomplete data (now with imputed values) with the complete data
      result <- complete_data %>%
        dplyr::union_all(mode_df)
    }
  }

  return(result)
}


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

imputed <- impute_with_mean_mode(sc, data_small)
