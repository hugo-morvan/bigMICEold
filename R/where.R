#' Creates a \code{where} argument
#'
#' This helper function creates a valid \code{where} matrix. The
#' \code{where} matrix is an argument to the \code{mice} function.
#' It has the same size as \code{data} and specifies which values
#' are to be imputed (\code{TRUE}) or nor (\code{FALSE}).
#' @param data A \code{data.frame} with the source data
#' @param keyword An optional keyword, one of \code{"missing"} (missing
#' values are imputed), \code{"observed"} (observed values are imputed),
#' \code{"all"} and \code{"none"}. The default
#' is \code{keyword = "missing"}
#' @return A matrix with logical
#' @seealso \code{\link{make.blocks}}, \code{\link{make.predictorMatrix}}
#' @examples
#' head(make.where(nhanes), 3)
#'
#' # create & analyse synthetic data
#' where <- make.where(nhanes2, "all")
#' imp <- mice(nhanes2,
#'   m = 10, where = where,
#'   print = FALSE, seed = 123
#' )
#' fit <- with(imp, lm(chl ~ bmi + age + hyp))
#' summary(pool.syn(fit))
#' @export
make.where <- function(data,
                       keyword = c("missing", "all", "none", "observed")) {
  keyword <- match.arg(keyword)

  data <- check.dataform(data)
  n_rows <- nrow(data)

  where <- switch(keyword,
    missing = is.na(data),
    all = matrix(TRUE, nrow = nrow(data), ncol = ncol(data)),
    none = matrix(FALSE, nrow = nrow(data), ncol = ncol(data)),
    observed = !is.na(data)
  )

  dimnames(where) <- dimnames(data)
  where
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


check.where <- function(where, data, blocks) {
  if (is.null(where)) {
    where <- make.where(data, keyword = "missing")
  }

  if (!(is.matrix(where) || is.data.frame(where))) {
    if (is.character(where)) {
      return(make.where(data, keyword = where))
    } else {
      stop("Argument `where` not a matrix or data frame", call. = FALSE)
    }
  }
  if (!all(dim(data) == dim(where))) {
    stop("Arguments `data` and `where` not of same size", call. = FALSE)
  }

  where <- as.logical(as.matrix(where))
  if (anyNA(where)) {
    stop("Argument `where` contains missing values", call. = FALSE)
  }

  where <- matrix(where, nrow = nrow(data), ncol = ncol(data))
  dimnames(where) <- dimnames(data)
  where[, !colnames(where) %in% unlist(blocks)] <- FALSE
  where
}

# Spark version of check.where
check.where.spark <- function(where, data, blocks) {
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

  # where <- matrix(where, nrow = nrow(data), ncol = ncol(data))
  # dimnames(where) <- dimnames(data)
  blocks_list <- unlist(blocks)
  cols_not_in_blocks <- colnames(where)[!colnames(where) %in% blocks_list]

  # Apply the transformation to set columns not in blocks to FALSE
  for (col in cols_not_in_blocks) {
    where <- where %>%
      mutate(across(all_of(cols_not_in_blocks), ~ FALSE))
  }
  where
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


