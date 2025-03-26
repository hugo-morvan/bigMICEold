impute_with_logistic_regression_V2 <- function(sc, sdf, target_col, feature_cols) {
    # FOR BOOLEAN VARIABLES ONLY (result of prediction is cast to logical)
    # For 0/1 variables or categorical variables, use impute_with_mult_logistic_regression
    # Given a spark connection, a spark dataframe, a target column with missing values,
    # and feature columns without missing values, this function:
    # 1. Builds a logistic regression model using complete cases
    # 2. Uses that model to predict missing values
    # 3. Returns a dataframe with imputed values in the target column

    # Step 0; Validate inputs
    if (!is.character(target_col) || length(target_col) != 1) {
        stop("target_col must be a single column name as a character string")
    }
    if (!is.character(feature_cols) || length(feature_cols) == 0) {
        stop("feature_cols must be a character vector of column names")
    }
    #Step 1: add temporary id
    sdf <- sdf %>% sparklyr::sdf_with_sequential_id()

    # Step 2: Split the data into complete and incomplete rows
    # Reminder: all non target columns will have been initialized
    complete_data <- sdf %>%
        dplyr::filter(!is.na(!!rlang::sym(target_col)))

    incomplete_data <- sdf %>%
        dplyr::filter(is.na(!!rlang::sym(target_col)))

    # Step 3: Build regression formula
    formula_str <- paste0(target_col, " ~ ", paste(feature_cols, collapse = " + "))
    formula_obj <- as.formula(formula_str)

    # Step 4: Build logistic regression model on complete data
    model <- complete_data %>%
        ml_logistic_regression(formula = formula_obj)

    # Step 5: Predict missing values
    predictions <- ml_predict(model, incomplete_data)

    print(predictions %>% select(prediction))

    # Replace the NULL values with predictions
    incomplete_data <- predictions %>%
        dplyr::select(-!!rlang::sym(target_col)) %>%  # Remove the original NULL column
        dplyr::mutate(prediction = as.logical(prediction)) %>%
        dplyr::rename(!!rlang::sym(target_col) := prediction)  # Rename prediction to target_col

    # Step 6: Combine complete and imputed data
    result <- complete_data %>%
      dplyr::union_all(incomplete_data)

    result <- result %>%
      dplyr::arrange(id) %>%
      dplyr::select(-id)

    return(result)
}
# TESTING
imputed_missing <- impute_with_logistic_regression_V2(sc, df_missing, label_col, features_col)

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

label_col = "IV_TherapySurgeryX" # Boolean in data
# label_col = "IV_Depression" # 0/1 in data

features_col <- setdiff(names(cols), label_col)

impute_modes <- setNames(rep("mode", length(colnames(data_small))), colnames(data_small))
impute_modes[c("LopNr","IV_SenPNr","IV_Height", "IV_Weight", "IV_BMI_Calculated","IV_BMI_UserSubmitted" )] <-
  c("none","none",  "median",    "median",    "mean",              "mean")

imputed_sdf <- impute_with_MeMoMe(sc, data_small, impute_mode = impute_modes)

# replace initialized values in label_col with the original missing values
df_missing <- imputed_sdf %>%
  select(-label_col) %>%
  cbind(data_small %>% select(all_of(label_col)))

df_missing
df_missing %>% select(label_col)

#Filter out Date data type
features_col <- features_col[sapply(cols[features_col],
                                    function(x) !x$type %in% c("StringType", "DateType", "TimestampType"))]

#Call the imputer

imputed_missing <- impute_with_logistic_regression_V2(sc, df_missing, label_col, features_col)

imputed_missing %>% select(label_col)

# Inspection

df_missing %>% pull(label_col)
# Imputed labels (Notice that all the non-missing labels appears first, then the imputed ones)

imputed_missing %>% select(all_of(label_col))
temp <- sdf_with_unique_id(imputed_missing, id = "id") %>% arrange(-id)
sdf_schema(temp)

#temp %>% select(any_of("IV_Weight")) #Doesnt work, not sure why
imputed_weight <- temp %>% sdf_collect() %>% select(all_of(label_col))

library(dbplot)
library(ggplot2)

df1 <- data_small %>% sdf_collect() %>%
  select(all_of(label_col)) %>%
  mutate(source = "Original") %>%
  filter(!is.na(!!sym(label_col)))

df2 <- temp %>% sdf_collect() %>%
  select(all_of(label_col))%>%
  mutate(source = "Original+Imputed")

df3 <- imputed_sdf %>% sdf_collect() %>%
  select(all_of(label_col))%>%
  mutate(source = "Init")


df_combined <- bind_rows(df1, df2)
df_combined <- bind_rows(df_combined, df3)
df_combined$source <- as.factor(df_combined$source)

ggplot(df_combined, aes(x = !!rlang::sym(label_col), fill = source)) +
  geom_histogram(alpha = 0.5, position = "identity", bins = 30) +  # Overlapping histograms
  scale_fill_manual(values = c("blue", "red", "green")) +  # Set custom colors
  theme_minimal() +
  labs(title = "Comparison of Distributions",
       x = label_col,
       y = "Count",
       fill = "Dataset")

ggplot(df_combined, aes(x = !!rlang::sym(label_col), fill = source)) +
  geom_density(alpha=0.5, size = 1.2) +  # Density plot with thicker lines
  scale_color_manual(values = c("Original" = "blue", "Original+Imputed" = "red", "Init" = "green")) +
  theme_minimal() +
  labs(title = "Comparison of Distributions",
       x = label_col,
       y = "Density",
       fill = "Dataset")

ggplot(df_combined, aes(x = !!rlang::sym(label_col), fill = source)) +
  geom_bar(alpha=0.5, size = 1.2, position = "dodge") +
  scale_color_manual(values = c("Original" = "blue", "Original+Imputed" = "red", "Init" = "green")) +
  theme_minimal() +
  labs(title = "Comparison of Distributions",
       x = label_col,
       y = "Density",
       fill = "Dataset")
#spark_disconnect(sc)
