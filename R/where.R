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

