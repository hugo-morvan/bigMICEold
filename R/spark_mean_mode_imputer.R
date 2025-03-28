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

  num_cols <- length(cols_to_process)
  # Process each column
  for (i in seq_along(cols_to_process)) {
    col <- cols_to_process[i]
    mode <- impute_mode[i]

    cat("\n",i,"/",num_cols,":", col, "- ")

    # Skip columns that don't exist
    if (!(col %in% colnames(result))) {
      warning(paste("Column", col, "not found in dataframe. Skipping."))
      next
    }

    # If mode is "none", skip to next column
    if (mode == "none") {
      cat(" Skipping imputation as specified.")
      next
    }

    # Split into complete and incomplete data for this column
    complete_data <- result %>%
      dplyr::filter(!is.na(!!rlang::sym(col)))

    incomplete_data <- result %>%
      dplyr::filter(is.na(!!rlang::sym(col)))

    # If no missing values or no observed values, skip this column
    if (sdf_nrow(incomplete_data) == 0) {
      cat(" No missing values found. Skipping.")
      next
    }

    if (sdf_nrow(complete_data) == 0) {
      cat(" No observed values found. Skipping.")
      next
    }

    # Calculate imputation value based on mode
    impute_value <- if (mode == "mean") {
      cat("Imputing with mean")
      value <- complete_data %>%
        dplyr::summarize(impute_val = mean(!!rlang::sym(col), na.rm = TRUE)) %>%
        dplyr::collect() %>%
        dplyr::pull(impute_val)
      cat(" - Mean value:", value)
      value
    } else if (mode == "median") {
      cat("Imputing with median")
      value <- complete_data %>%
        dplyr::summarize(impute_val = median(!!rlang::sym(col), na.rm = TRUE)) %>%
        dplyr::collect() %>%
        dplyr::pull(impute_val)
      cat(" - Median value:", value)
      value
    } else if (mode == "mode") {
      cat("Imputing with mode")
      value <- complete_data %>%
        dplyr::group_by(!!rlang::sym(col)) %>%
        dplyr::summarize(count = n()) %>%
        dplyr::arrange(desc(count)) %>%
        dplyr::collect() %>%
        head(1) %>%
        dplyr::pull(!!rlang::sym(col))
      cat(" - Mode value:", as.character(value))
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

imputed_df <- impute_with_MeMoMe(sc, data_small, impute_mode = impute_modes)

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

sdf_nrow(imputed_df)

data_r <- data_small %>% collect()
sum(is.na(data_r))
sapply(data_r, function(x) sum(is.na(x)))

imputed_collected <- imputed_df %>% collect()
sum(is.na(imputed_collected))
sapply(imputed_collected, function(x) sum(is.na(x)))

df1 <- data_small %>% sdf_collect() %>%
  select(all_of("IV_Weight")) %>%
  mutate(source = "Original") %>%
  filter(!is.na(IV_Weight))


df2 <- imputed_df %>% sdf_collect() %>%
  select(all_of("IV_Weight"))%>%
  mutate(source = "MeMoMe")


df_combined <- bind_rows(df1, df2)
df_combined$source <- as.factor(df_combined$source)

library(ggplot2)

ggplot(df_combined, aes(x = IV_Weight, fill = source)) +
  geom_histogram(alpha = 0.5, position = "identity", bins = 30) +
  scale_fill_manual(values = c("blue", "red")) +
  theme_minimal() +
  labs(title = "Comparison of IV_Weight Distributions",
       x = "IV Weight",
       y = "Count",
       fill = "Dataset")

ggplot(df_combined, aes(x = IV_Weight, fill = source)) +
  geom_density(alpha=0.5, size = 1.2) +  # Density plot with thicker lines
  theme_minimal() +
  labs(title = "Comparison of IV_Weight Distributions",
       x = "IV Weight",
       y = "Density",
       fill = "Dataset")

plot_variable_distributions <- function(df1, df2) {
  df1$source <- "Dataset 1"
  df2$source <- "Dataset 2"

  df_combined <- rbind(df1, df2)

  for (var in names(df1)) {
    if (var == "source") next  # Skip the added source column

    df_var <- df_combined[!is.na(df_combined[[var]]), ]  # Filter out NAs for each variable

    if (is.numeric(df_var[[var]])) {
      # Histogram with overlapping datasets
      p1 <- ggplot(df_var, aes(x = .data[[var]], fill = source)) +
        geom_histogram(alpha = 0.5, position = "identity", bins = 30) +
        scale_fill_manual(values = c("blue", "red")) +
        theme_minimal() +
        labs(title = paste("Comparison of", var, "Distributions"),
             x = var, y = "Count", fill = "Dataset")
      print(p1)

      # Density plot with overlapping datasets
      p2 <- ggplot(df_var, aes(x = .data[[var]], fill = source)) +
        geom_density(alpha = 0.5, size = 1.2) +
        scale_fill_manual(values = c("blue", "red")) +
        theme_minimal() +
        labs(title = paste("Comparison of", var, "Density"),
             x = var, y = "Density", fill = "Dataset")
      print(p2)
    } else {
      # Bar plot with overlapping datasets
      p3 <- ggplot(df_var, aes(x = .data[[var]], fill = source)) +
        geom_bar(position = "dodge", alpha = 0.7) +
        scale_fill_manual(values = c("blue", "red")) +
        theme_minimal() +
        labs(title = paste("Comparison of", var, "Counts"),
             x = var, y = "Count", fill = "Dataset")
      print(p3)
    }
  }
}
plot_variable_distributions(data_small %>% sdf_collect(),
                            imputed_df %>% sdf_collect())
