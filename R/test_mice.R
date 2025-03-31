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

  # Sort by the sequential ID and drop the ID column to restore original row order
  result <- result %>%
    dplyr::arrange(id) %>%
    dplyr::select(-id)

  return(result)
}

impute_with_linear_regression <- function(sc, sdf, target_col, feature_cols, elastic_net_param = 0) {
  # Given a spark connection, a spark dataframe, a target column with missing values,
  # and feature columns without missing values, this function:
  # 1. Builds a linear regression model using complete cases
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

  # Step 4: Build linear regression model on complete data
  lm_model <- complete_data %>%
    ml_linear_regression(formula = formula_obj,
                         elastic_net_param = elastic_net_param)

  # Step 5: Predict missing values
  predictions <- ml_predict(lm_model, incomplete_data)

  # Replace the NULL values with predictions
  incomplete_data <- predictions %>%
    dplyr::select(-!!rlang::sym(target_col)) %>%  # Remove the original NULL column
    dplyr::rename(!!rlang::sym(target_col) := prediction)  # Rename prediction to target_col

  # Re join the observed and imputed rows
  result <- complete_data %>%
    dplyr::union_all(incomplete_data)

  # Restore original row order and return
  result <- result %>%
    dplyr::arrange(id) %>%
    dplyr::select(-id)

  return(result)
}
impute_with_logistic_regression <- function(sc, sdf, target_col, feature_cols) {
  # FOR BOOLEAN VARIABLES ONLY (0/1)
  # For categorical variables, use impute_with_mult_logistic_regression
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

  # removing unused created columns (only need prediction)
  pre_pred_cols <- c(colnames(incomplete_data),"prediction")
  post_pred_cols <- colnames(predictions)
  extra_cols <- setdiff(post_pred_cols, pre_pred_cols)
  predictions <- predictions %>% select(-all_of(extra_cols))

  # Replace the NULL values with predictions
  incomplete_data <- predictions %>%
    dplyr::select(-!!rlang::sym(target_col)) %>%  # Remove the original NULL column
    # dplyr::mutate(prediction = as.logical(prediction)) %>%
    dplyr::rename(!!rlang::sym(target_col) := prediction)  # Rename prediction to target_col

  # Step 6: Combine complete and imputed data
  result <- complete_data %>%
    dplyr::union_all(incomplete_data)

  result <- result %>%
    dplyr::arrange(id) %>%
    dplyr::select(-id)

  return(result)
}

impute_with_mult_logistic_regression <- function(sc, sdf, target_col, feature_cols) {
  # Given a spark connection, a spark dataframe, a target column with missing values,
  # and feature columns without missing values, this function:
  # 1. Builds a multinomial logistic regression model using complete cases
  # 2. Uses that model to predict missing values
  # 3. Returns a dataframe with imputed values in the target column

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
  #print(colnames(predictions))

  # removing unused created columns (only need prediction)
  pre_pred_cols <- c(colnames(incomplete_data),"prediction")
  post_pred_cols <- colnames(predictions)
  extra_cols <- setdiff(post_pred_cols, pre_pred_cols)
  predictions <- predictions %>% select(-all_of(extra_cols))

  # Replace the NULL values with predictions
  incomplete_data <- predictions %>%
    dplyr::select(-!!rlang::sym(target_col)) %>%  # Remove the original NULL column
    #dplyr::mutate(prediction = as.logical(prediction)) %>%
    dplyr::rename(!!rlang::sym(target_col) := prediction)  # Rename prediction to target_col

  # Step 6: Combine complete and imputed data
  result <- complete_data %>%
    dplyr::union_all(incomplete_data)

  result <- result %>%
    dplyr::arrange(id) %>%
    dplyr::select(-id)

  return(result)
}

impute_with_random_forest_regressor <- function(sc, sdf, target_col, feature_cols) {
  # Random forest regressor using sparklyr ml_random_forest Good for continuous values
  # Doc: https://rdrr.io/cran/sparklyr/man/ml_random_forest.html

  #TODO: Added more flexibility for the user to use hyperparameters of the model (see doc)
  # Maybe add that as a ... param to the function
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
    ml_random_forest_regressor(formula = formula_obj)

  # Step 5: Predict missing values
  predictions <- ml_predict(model, incomplete_data)

  #print(predictions %>% select(prediction))

  # Replace the NULL values with predictions
  incomplete_data <- predictions %>%
    dplyr::select(-!!rlang::sym(target_col)) %>%  # Remove the original NULL column
    #dplyr::mutate(prediction = as.logical(prediction)) %>%
    dplyr::rename(!!rlang::sym(target_col) := prediction)  # Rename prediction to target_col

  # Step 6: Combine complete and imputed data
  result <- complete_data %>%
    dplyr::union_all(incomplete_data)

  result <- result %>%
    dplyr::arrange(id) %>%
    dplyr::select(-id)

  return(result)
}

impute_with_random_forest_classifier <- function(sc, sdf, target_col, feature_cols) {
  # Random forest imputer using sparklyr ml_random_forest Good for categorical values
  # Doc: https://rdrr.io/cran/sparklyr/man/ml_random_forest.html

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
    ml_random_forest_classifier(formula = formula_obj)

  # Step 5: Predict missing values
  predictions <- ml_predict(model, incomplete_data)

  #print(predictions %>% select(prediction))

  # Replace the NULL values with predictions
  incomplete_data <- predictions %>%
    dplyr::select(-!!rlang::sym(target_col)) %>%  # Remove the original NULL column
    #dplyr::mutate(prediction = as.logical(prediction)) %>%
    dplyr::rename(!!rlang::sym(target_col) := prediction)  # Rename prediction to target_col

  # Step 6: Combine complete and imputed data
  result <- complete_data %>%
    dplyr::union_all(incomplete_data)

  result <- result %>%
    dplyr::arrange(id) %>%
    dplyr::select(-id)

  return(result)
}

mice.spark <- function(data,
                       sc,
                       variable_types, # Used for initialization and method selection
                       m = 5,
                       method = NULL,
                       predictorMatrix = NULL,
                       ignore = NULL,
                       where = NULL,
                       blocks = NULL,
                       visitSequence = NULL,
                       formulas,
                       modeltype = NULL,
                       blots = NULL,
                       post = NULL,
                       defaultMethod = c("pmm", "logreg", "polyreg", "polr"),
                       maxit = 5,
                       printFlag = TRUE,
                       seed = NA,
                       imp_init = NULL,
                       ...) {


  if (!is.na(seed)) set.seed(seed)

  # check form of data and m
  #data <- check.spark.dataform(data)
  cols <- names(variable_types)
  #m <- check.m(m)


  from <- 1
  to <- from + maxit - 1

  # INITIALISE THE IMPUTATION USING Mean/Mode/Median SAMPLING

  # Do this inside or outside the m loop ?
  # Do I want each imputation to start from the same sample or have more variation in initial condition ?

  #TODO : add support for column parameter in initialisation

  # Dictionnary to infer initialization method based on variable type
  # Should be one of (mean, mode, median, none), and be used as input of MeMoMe function
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

  init_modes <- replace(variable_types, variable_types %in% names(init_dict), init_dict[variable_types])
  names(init_modes) <- cols
  # print("**DEBUG**: init_modes:")
  # print(init_modes)

  cat("\nStarting initialisation\n")
  # print(" ")

  # print(length(init_modes))
  # print(sdf_ncol(data))
  #
  init_start_time <- proc.time()
  if(is.null(imp_init)){
    imp_init <- impute_with_MeMoMe(sc = sc,
                                   sdf = data,
                                   column = NULL, #TODO: add support for this
                                   impute_mode = init_modes)
  }else{
    print("Using initial imputation provided manually, I hope it is correct")
    imp_init <- imp_init # User provided initiale imputation
  }

  init_end_time <- proc.time()
  init_elapsed <- (init_end_time-init_start_time)['elapsed']
  cat("Initalisation time:", init_elapsed)
  # TODO : Add elapse time to the result dataframe (and create result dataframe)

  # initialize return object (?) :
  imputation_results = c()

  # FOR EACH IMPUTATION SET i = 1, ..., m
  for (i in 1:m) {
    cat("Iteration: ", i, "\n")

    # Run the imputation algorithm
    cat("Starting imputation")

    imp_start_time <- proc.time()

    imp <- sampler.spark(sc = sc,
                         data = data,
                         imp_init = imp_init,
                         fromto = c(from, to),
                         var_types = variable_types,
                         printFlag = printFlag)

    imp_end_time <- proc.time()
    imp_elapsed <- (imp_end_time-imp_start_time)['elapsed']
    cat("Imputation time:", imp_elapsed)

    # Save imputation to dataframe ?
    # imputation_results = rbind(imputation_results, imp)

    # Calculate Rubin Rules statistics
    # TODO
  }

  return(imputation_results)
}

sampler.spark <- function(sc,
                          data,
                          imp_init,
                          fromto,
                          var_types,
                          printFlag){

  # This function takes a sparc connection sc, a spark dataframe data, an initial imputation (by random sampling),
  # and a range of iteration fromto (0-maxit) (done like this to follow mice() format)

  #TODO; add support for functionalities present in the mice() function (where, ignore, blocks, predictorMatrix, formula, ...)

  # For iteration k in fromto
  from = fromto[1]
  to = fromto[2]

  var_names <- names(sparklyr::sdf_schema(data))

  # Method dictionary for imputation. Can change as desired
  # TODO: implement; keep this as default, or use user-provided dict ?
  method_dict <- c("Binary" = "Logistic",
                   "Nominal" = "Mult_Logistic",
                   "Ordinal" = "RandomForestClassifier",
                   "Code (don't impute)" = "none", # LopNr, Unit_code etc...
                   "Continuous_int" = "Linear",
                   "Continuous_float" = "Linear",
                   "smalldatetime" = "none",  #TBD
                   "String" = "none", #TBD
                   "Count" = "RandomForestClassifier", #TBD
                   "Semi-continuous" = "none", #TBD
                   "Else" = "none")

  imp_methods <- replace(var_types, var_types %in% names(method_dict), method_dict[var_types])
  names(imp_methods) <- var_names
  print(imp_methods)
  num_vars <- length(var_names)

  for (k in from:to){
    cat("\n iteration: ", k)
    # For each variable j in the data
    j <- 0
    for (var_j in var_names){
      j <- j+1
      cat("\n",j,"/",num_vars,"Imputing variable", var_j," using method ")
      # Obtain the variables use to predict the missing values of variable j and create feature column
      label_col <- var_j

      feature_cols <- setdiff(var_names, label_col)
      #print(feature_cols)
      #Filter out Date data type
      feature_cols <- feature_cols[sapply(var_types[feature_cols],
                                          function(x) !(x %in% c("String", "smalldatetime")))]

      # Replace initialized values in label column with the original missing values
      j_df <- imp_init %>%
        sparklyr::select(-label_col) %>%
        cbind(data %>% sparklyr::select(all_of(label_col)))


      method <- imp_methods[[var_j]]
      cat(method)

      result <- switch(method,
                       "Logistic" = impute_with_logistic_regression(sc, j_df, label_col, feature_cols),
                       "Mult_Logistic" = impute_with_mult_logistic_regression(sc, j_df, label_col, feature_cols),
                       "Linear" = impute_with_linear_regression(sc, j_df, label_col, feature_cols),
                       "RandomForestClassifier" = impute_with_random_forest_classifier(sc, j_df, label_col, feature_cols),
                       "none" = j_df, # don't impute this variable
                       "Invalid method"  # Default case
      )
      #Use the result to do something to the original dataset
      #print(result)
      # To avoid stackoverflow error, I try to break/collect? the lineage after each imputation
      # Might not be necessary at every iteration. only run into error after ~25 imputation
      result %>% sdf_persist()
      # But this does not seems to work. Still run into stack_overflow error
    } #end of var_j loop (each variable)

  } #end of k loop (iterations)
  # The sampler has finish his iterative work, can now return the imputed dataset ?
  return(result)
}


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


# In FU, lopNr and SenPNr contain missing values, so i remove those columns for simplicity
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

data_small %>% sdf_persist(name = "data")

imput_init <- impute_with_MeMoMe(sc, data_small, impute_mode = impute_modes)

imput_init %>% sdf_persist()

imputed_results = mice.spark(data_small,
                             sc,
                             variable_types,
                             m = 2,
                             imp_init = imput_init)

#spark_disconnect(sc)
