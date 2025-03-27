impute_with_random_samples_old <- function(sc, sdf, column = NULL) {
  # Given a spark connection, a spark dataframe, and an optional column name:
  # 1. If column is specified, imputes missing values in that column only
  # 2. If column is NULL, imputes missing values in all columns
  # 3. Imputation is done by randomly sampling from observed values in each column
  # 4. Returns a dataframe with imputed values

  # TODO: Fix issues:
  # The number of sampled values does not match the number of missing values.
  # Is this due to rounding error when using the fraction parameter of sdf_sample?
  # Sometimes is is oversampling, sometimes undersampling

  # Also, for each variable, n_observed+n_missing != 1000 (when using data_small)
  # so there is something weird happening
  # need to add a progress bar to look at while it is initialising


  # Determine which columns to process
  cols_to_process <- if (!is.null(column)) {
    if (!is.character(column) || length(column) == 0) {
      stop("column must be a character string")
    }
    column
  } else {
    colnames(sdf)
  }

  # # Add a unique identifier to the original dataframe
  result <- sdf #%>%
  #   dplyr::mutate(temp_row_id = monotonically_increasing_id())
  num_cols = length(cols_to_process)
  i <- 0
  for (col in cols_to_process) {
    cat("\nvariable", i , "out of", num_cols)
    i <- i + 1
    cat("\n",col,"- ")
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
    if (sdf_nrow(incomplete_data) == 0 || sdf_nrow(complete_data) == 0) {
      next
    }

    # Extract values to sample from (non-NA values in the column)
    values_to_sample <- complete_data %>%
      dplyr::select(!!rlang::sym(col))



    # Limit the sampled values to match the number of missing values
    n_missing <- sdf_nrow(incomplete_data)
    cat(" n_missing ", n_missing)

    n_observed <- sdf_nrow(complete_data)
    cat(" n_observed", n_observed)

    sampled_df <- complete_data %>%
      dplyr::select(all_of(col)) %>%  # Only need the column to sample
      dplyr::sample_n(size = n_missing, replace = TRUE) %>%
      sdf_with_sequential_id(id = "id")

      n_sampled <- sdf_nrow(sampled_df)
      cat(" n_sampled", n_sampled)


    # Replace the NULL values with predictions
    incomplete_data <- sampled_df %>%
      dplyr::select(-!!rlang::sym(col))

    result <- complete_data %>%
      dplyr::union_all(incomplete_data)

  }

  return(result)
}

imputed_sdf <- impute_with_random_samples_old(sc, data_small)

impute_with_random_samples_v2 <- function(sc, sdf, column = NULL, plot_dist = FALSE) {
  # Works, but iterations are increasingly slow (exponentially ?)
  # This could be due to the increasingly complex query since spark is lazily loaded

  # Potential solution: Caching intermediate results, checkpointing, persisting.

  # Validate input
  if (!is.null(column) && (!is.character(column) || length(column) == 0)) {
    stop("column must be a non-empty character string")
  }

  # Determine columns to process
  cols_to_process <- if (!is.null(column)) column else colnames(sdf)

  # Add sequential ID to preserve original order
  sdf_with_id <- sdf %>%
    sdf_with_sequential_id(id = "temp_row_id")

  num_cols <- length(cols_to_process)
  i <- 0

  # Process each specified column
  for (col in cols_to_process) {
    cat("\nVariable", i, "out of", num_cols)
    i <- i + 1
    cat(":", col, "- ")

    # Skip if column doesn't exist
    if (!(col %in% colnames(sdf_with_id))) {
      warning(paste("Column", col, "not found in dataframe. Skipping."))
      next
    }

    # Separate observed and missing values while maintaining original order
    observed_data <- sdf_with_id %>%
      dplyr::select(all_of(c(col, "temp_row_id"))) %>%
      dplyr::filter(!is.na(!!rlang::sym(col)))

    missing_data <- sdf_with_id %>%
      dplyr::select(all_of(c(col, "temp_row_id"))) %>%
      dplyr::filter(is.na(!!rlang::sym(col)))

    # Calculate sampling fraction
    n_missing <- sdf_nrow(missing_data)
    n_observed <- sdf_nrow(observed_data)

    # Skip if no missing or no observed values
    if (n_missing == 0 || n_observed == 0) {
      cat("No missing values or no observed values to sample from")
      next
    }
    cat("Sampling", n_missing, "values")

    # Sample n_missing values from the observed values
    sampled_values <- observed_data %>%
      dplyr::select(all_of(col)) %>%  # Only need the column to sample
      dplyr::sample_n(size = n_missing, replace = TRUE) %>%
      sdf_with_sequential_id(id = "id")

    n_sampled <- sdf_nrow(sampled_values)
    cat(" n_sampled", n_sampled)

    # Add sequential ID to missing_data for joining
    missing_data_with_id <- missing_data %>%
      sdf_with_sequential_id(id = "id")

    # Replace NA values with sampled values
    imputed_data <- missing_data_with_id %>%
      left_join(sampled_values %>% rename(value_new = !!rlang::sym(col)), by = "id") %>%
      mutate(!!rlang::sym(col) := coalesce(value_new, !!rlang::sym(col))) %>%
      select(-id, -value_new)

    # Union with observed data and sort by temp_row_id
    new_col_data <- imputed_data %>%
      dplyr::union(observed_data) %>%
      dplyr::arrange(temp_row_id)

    # Update the column in sdf_with_id
    # Since new_col_data is a Spark DataFrame, we join and replace
    sdf_with_id <- sdf_with_id %>%
      select(-!!rlang::sym(col)) %>%  # Drop old column
      left_join(new_col_data %>% select(temp_row_id, !!rlang::sym(col)), by = "temp_row_id")

    if (plot_dist) {
      # Placeholder for plotting (implement as needed)
      cat("Plotting distributions (not implemented)\n")
    }

  }

  # Remove the temporary ID column and return
  sdf_with_id %>%
    dplyr::arrange(temp_row_id) %>%
    dplyr::select(-"temp_row_id")
}

imputed_sdf <- impute_with_random_samples_v2(sc, data_small)

impute_with_random_samples <- function(sc, sdf, column = NULL, plot_dist = FALSE) {
  # Works, but iterations are increasingly slow (exponentially ?)
  # This could be due to the increasingly complex query since spark is lazily loaded

  # Potential solution: Caching intermediate results, checkpointing, persisting reused data
  # checkpoint -> HDFS only
  # Cache -> memory then disk when memory runs out
  # persist -> memory and/or disk, choice possible


  # Validate input
  if (!is.null(column) && (!is.character(column) || length(column) == 0)) {
    stop("column must be a non-empty character string")
  }

  # Determine columns to process
  cols_to_process <- if (!is.null(column)) column else colnames(sdf)

  # Add sequential ID to preserve original order
  sdf_with_id <- sdf %>%
    sdf_with_sequential_id(id = "temp_row_id")

  num_cols <- length(cols_to_process)
  i <- 0

  # Process each specified column
  for (col in cols_to_process) {
    cat("\nVariable", i, "out of", num_cols)
    i <- i + 1
    cat(":", col, "- ")

    # Skip if column doesn't exist
    if (!(col %in% colnames(sdf_with_id))) {
      warning(paste("Column", col, "not found in dataframe. Skipping."))
      next
    }

    # Separate observed and missing values while maintaining original order
    observed_data <- sdf_with_id %>%
      dplyr::select(all_of(c(col, "temp_row_id"))) %>%
      dplyr::filter(!is.na(!!rlang::sym(col)))

    missing_data <- sdf_with_id %>%
      dplyr::select(all_of(c(col, "temp_row_id"))) %>%
      dplyr::filter(is.na(!!rlang::sym(col)))

    # Calculate sampling fraction
    n_missing <- sdf_nrow(missing_data)
    n_observed <- sdf_nrow(observed_data)

    # Skip if no missing or no observed values
    if (n_missing == 0 || n_observed == 0) {
      cat("No missing values or no observed values to sample from")
      next
    }
    cat("Sampling", n_missing, "values")

    # Sample n_missing values from the observed values
    sampled_values <- observed_data %>%
      dplyr::select(all_of(col)) %>%  # Only need the column to sample
      dplyr::sample_n(size = n_missing, replace = TRUE) %>%
      sdf_with_sequential_id(id = "id")

    # Add sequential ID to missing_data for joining
    missing_data_with_id <- missing_data %>%
      sdf_with_sequential_id(id = "id")

    # Replace NA values with sampled values
    imputed_data <- missing_data_with_id %>%
      left_join(sampled_values %>% rename(value_new = !!rlang::sym(col)), by = "id") %>%
      mutate(!!rlang::sym(col) := coalesce(value_new, !!rlang::sym(col))) %>%
      select(-id, -value_new)

    # Union with observed data and sort by temp_row_id
    new_col_data <- imputed_data %>%
      dplyr::union(observed_data) %>%
      dplyr::arrange(temp_row_id)

    # Update the column in sdf_with_id
    # Since new_col_data is a Spark DataFrame, we join and replace
    sdf_with_id <- sdf_with_id %>%
      select(-!!rlang::sym(col)) %>%  # Drop old column
      left_join(new_col_data %>% select(temp_row_id, !!rlang::sym(col)), by = "temp_row_id")

    if (plot_dist) {
      # Placeholder for plotting (implement as needed)
      cat("Plotting distributions (not implemented)\n")
    }

    sdf_with_id <- sdf_persist(sdf_with_id)
  }

  # Remove the temporary ID column and return
  sdf_with_id %>%
    dplyr::arrange(temp_row_id) %>%
    dplyr::select(-"temp_row_id")
}


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
    sparklyr::tbl_uncache(sc, paste0("sample_", tolower(col_name)))
  }

  return(sdf)
}

library(sparklyr)
library(dplyr)


conf <- spark_config()
conf$`sparklyr.shell.driver-memory`<- "128G"
conf$spark.memory.fraction <- 0.6
conf$`sparklyr.cores.local` <- 24

# Add checkpoint directory setting
checkpoint_dir <- "/home/hugmo418/Documents/sparkCheck"
conf[["spark.sql.checkpointLocation"]] <- checkpoint_dir

sc = spark_connect(master = "local", config = conf)
#spark_disconnect(sc)
path_DORS =     "/vault/hugmo418_amed/NDR-SESAR/Uttag Socialstyrelsen 2024/ut_r_par_ov_63851_2023.csv"
path_NDR =      "/vault/hugmo418_amed/NDR-SESAR/Uttag SCB+NDR+SESAR 2024/FI_Lev_NDR.csv"
path_SESAR_FU = "/vault/hugmo418_amed/NDR-SESAR/Uttag SCB+NDR+SESAR 2024/FI_Lev_SESAR_FU.csv"
path_SESAR_IV = "/vault/hugmo418_amed/NDR-SESAR/Uttag SCB+NDR+SESAR 2024/FI_Lev_SESAR_IV.csv"
path_SESAR_TS = "/vault/hugmo418_amed/NDR-SESAR/Uttag SCB+NDR+SESAR 2024/FI_Lev_SESAR_TS.csv"
path_small_SESAR_IV = "/vault/hugmo418_amed/subsets_thesis_hugo/small_IV.csv"

data_small <- spark_read_csv(sc, name = "df",path = path_small_SESAR_IV,infer_schema = TRUE, null_value = 'NA')

library(ggplot2)


imputed_sdf <- impute_with_random_samples(sc, data_small)
spark_session(sc) %>% invoke("catalog") %>% invoke("clearCache")

df1 <- data_small %>% sdf_collect() %>%
  select(all_of("IV_Weight")) %>%
  mutate(source = "Original") %>%
  filter(!is.na(IV_Weight))


df2 <- imputed_sdf %>% sdf_collect() %>%
  select(all_of("IV_Weight"))%>%
  mutate(source = "RandomInit")


df_combined <- bind_rows(df1, df2)
df_combined$source <- as.factor(df_combined$source)

ggplot(df_combined, aes(x = IV_Weight, fill = source)) +
  geom_histogram(alpha = 0.5, position = "identity", bins = 30) +  # Overlapping histograms
  scale_fill_manual(values = c("blue", "red")) +  # Set custom colors
  theme_minimal() +
  labs(title = "Comparison of IV_Weight Distributions",
       x = "IV Weight",
       y = "Count",
       fill = "Dataset")

ggplot(df_combined, aes(x = IV_Weight, fill = source)) +
  geom_density(alpha=0.5, size = 1.2) +  # Density plot with thicker lines
  scale_color_manual(values = c("Original" = "blue", "Initialisation" = "red")) +  # Match colors
  theme_minimal() +
  labs(title = "Comparison of IV_Weight Distributions",
       x = "IV Weight",
       y = "Density",
       fill = "Dataset")

data_r <- data_small %>% collect()
sum(is.na(data_r))
sapply(data_r, function(x) sum(is.na(x)))

imputed_df <- imputed_sdf %>% collect()
sum(is.na(imputed_df))
sapply(imputed_df, function(x) sum(is.na(x)))
