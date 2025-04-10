check.data <- function(data, method) {
  check.dataform(data)
}


check.dataform <- function(data) {
  # Check data type
  if (!(is.matrix(data) || is.data.frame(data))) {
    stop("Data should be a matrix or data frame", call. = FALSE)
  }
  # Check more than 1 column
  if (ncol(data) < 2) {
    stop("Data should contain at least two columns", call. = FALSE)
  }
  data <- as.data.frame(data)
  # Checks for nested matrix/data.frame
  mat <- sapply(data, is.matrix)
  df <- sapply(data, is.data.frame)
  if (any(mat)) {
    stop(
      "Cannot handle columns with class matrix: ",
      colnames(data)[mat]
    )
  }
  if (any(df)) {
    stop(
      "Cannot handle columns with class data.frame: ",
      colnames(data)[df]
    )
  }
  # Checks for duplicates col names
  dup <- duplicated(colnames(data))
  if (any(dup)) {
    stop(
      "Duplicate names found: ",
      paste(colnames(data)[dup], collapse = ", ")
    )
  }
  data
}

# Spark version of check.dataform function

check.spark.dataform <- function(data) {
  # Check spark dataframe data type
  if (!inherits(data, "tbl_spark")) {
    stop("Data should be a Spark DataFrame (tbl_spark)", call. = FALSE)
  }
  cols <- sparklyr::sdf_schema(data)
  # print("**DEBUG** in check_spark_dataframe, cols: ") # DEBUG STATEMENT, remove later
  # print(cols) # DEBUG STATEMENT, remove later
  # Check more than 1 column
  if (length(cols) < 2) {
    stop("Data should contain at least two columns", call. = FALSE)
  }
  # Checks for duplicates col names
  col_names <- names(cols)
  # print("**DEBUG** in check_spark_dataframe, col_names: ") # DEBUG STATEMENT, remove later
  # print(col_names)
  dup <- duplicated(col_names)
  if (any(dup)) {
    stop(
      "Duplicate names found: ",
      paste(colnames(data)[dup], collapse = ", ")
    )
  }
  # Checks for nested matrix/data.frame

  for (col in cols) {
    if (grepl("array|struct", col$type, ignore.case = TRUE)) {
      # print("**DEBUG** in check_spark_dataframe, col$type: ") # DEBUG STATEMENT, remove later
      # print(col$type)
      stop(
        "Cannot handle columns with nested types (arrays or structs): ",
        col$name,
        call. = FALSE
      )
    }
  }
  data
}


check.m <- function(m) {
  m <- m[1L]
  if (!is.numeric(m)) {
    stop("Argument m not numeric", call. = FALSE)
  }
  m <- floor(m)
  if (m < 1L) {
    stop("Number of imputations (m) lower than 1.", call. = FALSE)
  }
  m
}


