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
    print("col_type")
    print(col_type)
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

      # Calculate most frequent value (almost mode :/ )
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

impute_with_MeMoMe  <- function(sc, sdf, column = NULL, impute_mode) {
  # Validate impute_mode is provided
  if (missing(impute_mode)) {
    stop("impute_mode is a mandatory argument and must be specified for each column")
  }

  # Determine which columns to process
  cols_to_process <- if (!is.null(column)) {
    if (!is.character(column) || length(column) == 0) {
      stop("column must be a character string")
    }
    column
  } else {
    colnames(sdf)
  }

  # Validate impute_mode matches number of columns
  if (length(impute_mode) != length(cols_to_process)) {
    stop("Length of impute_mode must match number of columns being processed")
  }

  # Validate impute_mode values
  valid_modes <- c("mean", "mode", "median", "none")
  invalid_modes <- setdiff(impute_mode, valid_modes)
  if (length(invalid_modes) > 0) {
    stop(paste("Invalid imputation modes:", paste(invalid_modes, collapse=", "),
               "\nValid modes are:", paste(valid_modes, collapse=", ")))
  }

  # Add a sequential ID to preserve row order
  result <- sdf %>%
    sparklyr::sdf_with_sequential_id()

  # Process each column
  for (i in seq_along(cols_to_process)) {
    col <- cols_to_process[i]
    mode <- impute_mode[i]

    cat("\n", col, "- ")

    # Skip columns that don't exist
    if (!(col %in% colnames(result))) {
      warning(paste("Column", col, "not found in dataframe. Skipping."))
      next
    }

    # If mode is "none", skip to next column
    if (mode == "none") {
      cat(" Skipping imputation as specified.\n")
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

    # Calculate imputation value based on mode
    impute_value <- if (mode == "mean") {
      cat(" Imputing with mean\n")
      value <- complete_data %>%
        dplyr::summarize(impute_val = mean(!!rlang::sym(col), na.rm = TRUE)) %>%
        dplyr::collect() %>%
        dplyr::pull(impute_val)
      cat(" Mean value:", value, "\n")
      value
    } else if (mode == "median") {
      cat(" Imputing with median\n")
      value <- complete_data %>%
        dplyr::summarize(impute_val = median(!!rlang::sym(col), na.rm = TRUE)) %>%
        dplyr::collect() %>%
        dplyr::pull(impute_val)
      cat(" Median value:", value, "\n")
      value
    } else if (mode == "mode") {
      cat(" Imputing with mode\n")
      value <- complete_data %>%
        dplyr::group_by(!!rlang::sym(col)) %>%
        dplyr::summarize(count = n()) %>%
        dplyr::arrange(desc(count)) %>%
        dplyr::collect() %>%
        head(1) %>%
        dplyr::pull(!!rlang::sym(col))
      cat(" Mode value:", as.character(value), "\n")
      value
    }

    # Create imputed dataframe
    imputed_df <- result %>%
      dplyr::mutate(!!rlang::sym(col) := if_else(is.na(!!rlang::sym(col)), impute_value, !!rlang::sym(col)))

    # Update result with imputed dataframe
    result <- imputed_df
  }

  # Sort by the sequential ID and drop the ID column
  result <- result %>%
    dplyr::arrange(id) %>%
    dplyr::select(-id)

  return(result)
}
impute_modes <- setNames(rep("mode", length(colnames(data_small))), colnames(data_small))
impute_modes[c("LopNr","IV_SenPNr","IV_Height", "IV_Weight", "IV_BMI_Calculated","IV_BMI_UserSubmitted" )] <-
              c("none","none",  "median",    "median",    "mean",              "mean")
imputed <- impute_with_MeMoMe(sc, data_small, impute_mode = impute_modes)

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

# Note: if session fails to start/ fails to read and the error mentions hive,
# Kill jvm related running processes and delete metastore_db folder, then restart conenction

imputed <- impute_with_mean_mode(sc, data_small)
