
mice.spark <- function(data,
                       sc,
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
                       data.init = NULL,
                       ...) {
  call <- match.call()
  #check.deprecated(...)

  if (!is.na(seed)) set.seed(seed)

  # check form of data and m
  data <- check.spark.dataform(data)
  cols <- sparklyr::sdf_schema(data)
  m <- check.m(m)

  # mp <- missing(predictorMatrix)
  #
  # predictorMatrix <- make.spark.predictorMatrix(data, blocks)
  # print("**DEBUG** where:")
  # print(where)
  # where <- check.where.spark(where, data, blocks)
  # print("**DEBUG** where after check.where:")
  # print(where)

  from <- 1
  to <- from + maxit - 1
  # Calculate imputation mask ...
  #where <- make.where.spark(data, keyword = "missing")

  #observed <- make.where.spark(data, keyword = "observed")

  # and initialize it with random sampling from where the data is not missing
  #imp_init <- data %>% sparklyr::sdf_sample(fraction = 0.1)

  # INITIALISE THE IMPUTATION USING RANDOM SAMPLING
  #Do this inside or outside the m loop ?
  #Do I want each imputation to start from the same sample or have more variation in initial condition ?
  imp_init_random <- impute_with_random_samples(sc = sc, sdf = data)

  # initialise return object:
  imputation_results = c()
  # FOR EACH IMPUTATION SET i = 1, ..., m
  for (i in 1:m) {
    cat("Iteration: ", i, "\n")
    # Load data, imputation mask and imputation set
    # is the copy needed ?
    data <- sparklyr::sdf_copy_to(sc, data)
    imp_init <- sparklyr::sdf_copy_to(sc, imp_init_random)

    # Run the imputation algorithm
    imp <- sampler.spark(data, imp_init, c(from, to))

    # Save imputation to dataframe ?

    imputation_results = rbind(imputation_results, imp)

  }

  return(imputation_results)
}


sampler.spark <- function(sc, data, imp_init, fromto){
  # This function takes a sparc connection sc, a spark dataframe data, an initial imputation (by random sampling),
  # and a range of iteration fromto (0-maxit) (done like this to follow mice() format)

  #TODO; add support for functionalities present in the mice() function (where, ignore, blocks, predictorMatrix, formula, ...)

  # For iteration k in fromto
  from = fromto[1]
  to = fromto[2]
  # get the variable type of each variable in the data and store that into an array
  var_names <- names(sparklyr::sdf_schema(data))
  cols <- sparklyr::sdf_schema(data)
  var_types = get_var_types(data, var_names)


  for (k in from:to){
    cat("\n iteration: ", k)
    # For each variable j in the data
    for (var_j in var_names){
      cat("\nImputing variable", var_j,". ")
      # Obtain the variables use to predict the missing values of variable j and create feature column
      label_col <- var_j

      feature_cols <- setdiff(var_names, label_col)
      #print(feature_cols)
      #Filter out Date data type
      feature_cols <- feature_cols[sapply(cols[feature_cols],
                                          function(x) !x$type %in% c("StringType", "DateType", "TimestampType"))]

      # replace random sample values in label column with the original missing values
      j_df <- imp_init %>%
        sparklyr::select(-label_col) %>%
        cbind(data %>% sparklyr::select(all_of(label_col)))

      # Get the variable type then choose the appropriate imputation method
      # Create the model
      if (var_types[[var_j]] == "Numerical"){
        cat("Numerical type detected, imputing using linear regression. ")
        imp_init <- impute_with_linear_regression(sc, j_df, label_col, feature_cols)

      } else if (var_types[[var_j]] == "Categorical"){
        cat("Categorical type detected, imputing using multinomial logistic regression. ")
        imp_init <- impute_with_mult_logistic_regression(sc, j_df, label_col, feature_cols)

      } else if (var_types[[var_j]] == "Binary"){
        cat("Binary type detected, imputing using logistic regression. ")
        imp_init <- impute_with_logistic_regression(sc, j_df, label_col, feature_cols)

      } else if(var_types[[var_j]] == "Unsupported"){
        cat("Unsupported type detected, skipping imputation for this variable.")
        # Do nothing, pass to the next for loop iteration
        next
      } else{
        cat("Nothing was detected, woopsies")
      }
    } #end of var_j loop

  } #end of k loop
  # The sampler has finish his iterative work, can now return the imputed dataset ?
  return(imp_init)
}


# Spark version of make.where
make.where.spark <- function(data, keyword = c("missing", "all", "none", "observed")) {
  keyword <- match.arg(keyword)

  data <- check.spark.dataform(data)
  where <- switch(keyword,
                  missing = na_locations(data),
                  all = data %>% mutate(across(everything(), ~ TRUE)),
                  none = data %>% mutate(across(everything(), ~ FALSE)),
                  observed = data %>% mutate(across(everything(), ~ !is.na(.)))
  )
  where
}



# Spark version of check.where
check.where.spark <- function(where, data) {
  if (is.null(where)) {
    # print("**DEBUG** where is null")
    where <- make.where.spark(data, keyword = "missing")
  }

  if (!inherits(where, "tbl_spark")) {
    if (is.character(where)) {
      return(make.where.spark(data, keyword = where))
    } else {
      stop("Argument `where` not a Spark DataFrame", call. = FALSE)
    }
  }
  # Num rows of a spark data frame is unknown until pulled, so dim(X) returns (NA, n_cols)
  # Thus n_rows needs to be calculated separately
  n_rows_where = where %>% dplyr::count() %>% dplyr::pull()
  # print("**DEBUG** n_rows_where:")
  # print(n_rows_where)
  n_rows_data = data %>% dplyr::count() %>% dplyr::pull()
  # print("**DEBUG** n_rows_data:")
  # print(n_rows_data)
  if ((dim(data)[2] != dim(where)[2]) || (n_rows_where != n_rows_data)) {
    stop("Arguments `data` and `where` not of same size", call. = FALSE)
  }

  #where <- as.logical(as.matrix(where)) #Not needed ?
  if (is.na.spark(where)) { #from NA_utils.R
    stop("Argument `where` contains missing values", call. = FALSE)
  }

  where
}


get_var_types <- function(data, var_names) {
  # Initialize an empty vector to store variable types
  # TODO: Make this function better. right now it make bad guesses
  types <- character(length(var_names))
  names(types) <- var_names

  schema <- sdf_schema(data)
  schema_types <- setNames(sapply(schema, `[[`, "type"), sapply(schema, `[[`, "name"))
  # Loop through each variable in the schema
  for (var_name in var_names) {

    # Extract the type information
    var_type <- schema_types[[var_name]]

    # Categorize based on Spark types
    if (grepl("BooleanType", var_type)) {
      types[var_name] <- "Binary"

    } else if (grepl("ByteType|ShortType|IntegerType|LongType", var_type)) {
      types[var_name] <- "Numerical"

    } else if (grepl("FloatType|DoubleType|DecimalType", var_type)) {
      types[var_name] <- "Numerical"

    } else if (grepl("StringType|CharType|VarcharType", var_type)) {
      types[var_name] <- "Categorical"

    } else  {
      types[var_name] <- "Unsupported"
    }

    if (grepl("Type", var_name)) {
      #if "Type" in the var name, set type to categorical (for sesar datasets)
      types[var_name] <- "Categorical"
    }
  }

  return(types)
}
# tests for get_var_types
# var_names <- sparklyr::sdf_schema(dummy_data)
# var_types <- get_var_types(dummy_data, var_names)
# print(var_types)



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

