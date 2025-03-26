impute_with_mult_logistic_regression <- function(sc, sdf, target_col, feature_cols) {
  # General template for adding an imputer using sparklyr ML models.
  # See https://rdrr.io/cran/sparklyr/man/ for a list of available models (ml_XXX class functions)

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
  stop("Template function used, it is not supposed to work") # REMOVE THIS
  model <- complete_data %>%
    ml_INSERT_MODEL_NAME_HERE(formula = formula_obj)

  # Step 5: Predict missing values
  predictions <- ml_predict(model, incomplete_data)

  print(predictions %>% select(prediction))

  # Replace the NULL values with predictions
  incomplete_data <- predictions %>%
    dplyr::select(-!!rlang::sym(target_col)) %>%  # Remove the original NULL column
    #dplyr::mutate(prediction = as.logical(prediction)) %>% #MIGHT WANT TO UNCOMMENT THIS IF MODEL US IS FOR BOOLEANS
    dplyr::rename(!!rlang::sym(target_col) := prediction)  # Rename prediction to target_col

  # Step 6: Combine complete and imputed data
  result <- complete_data %>%
    dplyr::union_all(incomplete_data)

  # Sort the rows back to the original order
  result <- result %>%
    dplyr::arrange(id) %>%
    dplyr::select(-id)

  return(result)
}
