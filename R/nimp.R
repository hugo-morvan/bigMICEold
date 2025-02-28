#' Number of imputations per block
#'
#' Calculates the number of cells within a block for which imputation
#' is requested.
#' @inheritParams mice
#' @return  A numeric vector of length \code{length(blocks)} containing
#' the number of cells that need to be imputed within a block.
#' @seealso \code{\link{mice}}
#' @export
#' @examples
#' where <- is.na(nhanes)
#'
#' # standard FCS
#' nimp(where)
#'
#' # user-defined blocks
#' nimp(where, blocks = name.blocks(list(c("bmi", "hyp"), "age", "chl")))
nimp <- function(where, blocks = make.blocks(where)) {
  nwhere <- apply(where, 2, sum)
  nimp <- vector("integer", length = length(blocks))
  names(nimp) <- names(blocks)
  for (i in seq_along(blocks)) nimp[i] <- sum(nwhere[blocks[[i]]])
  nimp
}


# Spark version of nimp. Sould handle spark dataframes (where)
nimp.spark <- function(where, blocks = make.blocks(where)) {
  # Compute the column sum of the where (logical) matrix
  # Convert bool to numeric
  print("**DEBUG** where")
  nwhere <- sapply(colnames(where), function(col) {
    where %>%
      summarize(total = sum(as.numeric(!!sym(col)), na.rm = TRUE)) %>%
      collect() %>% pull(total)
  })
  print("**DEBUG** nwhere")
  # Initialize the nimp vector, same length as blocks
  nimp <- vector("integer", length = length(blocks))
  # Assign names to the nimp vector
  colnames(nimp) <- colnames(blocks)
  # Loop through the blocks and compute the number of cells that need to be imputed
  for (i in seq_along(blocks)) nimp[i] <- sum(nwhere[blocks[[i]]])
  nimp
}

