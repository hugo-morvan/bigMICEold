#' Imputation by logistic regression
#'
#' Imputes univariate missing data using logistic regression.


mice.impute.spark.logreg <- function(y, ry, x, wy = NULL, augment_data = FALSE, ...) {
  # If the where to impute argument is not specified (wy),
  # impute on everything that is not used to fit the model (ry) :
  if (is.null(wy)) wy <- !ry

  # augment data in order to evade perfect prediction
  if (augment_data) {
    aug <- augment(y, ry, x, wy)
    x <- aug$x    # augmented features
    y <- aug$y    # augmented labels ?
    ry <- aug$ry  # augmented training data indicator
    wy <- aug$wy  # augmented where to impute indicator
    w <- aug$w    # weights
  }

  # fit model in mice.impute.logreg
  # x <- cbind(1, as.matrix(x))
  # expr <- expression(glm.fit(
  #   x = x[ry, , drop = FALSE],
  #   y = y[ry],
  #   family = quasibinomial(link = logit),
  #   weights = w[ry]
  # ))
  # fit <- eval(expr)
  # fit.sum <- summary.glm(fit)
  # beta <- coef(fit)
  # rv <- t(chol(sym(fit.sum$cov.unscaled)))
  # beta.star <- beta + rv %*% rnorm(ncol(rv))

  # fit model using Spark
  # Get the features names
  cols <- sparklyr::sdf_schema(x)
  features_col <- setdiff(names(cols), label_col)

  # Drop String type, DateType, TimestampType columns as not supported by logistic regression
  features_col <- features_col[sapply(cols[features_col],
            function(x) !x$type %in% c("StringType", "DateType", "TimestampType"))]

  # Create the features vector column
  x <- x %>% ft_vector_assembler(input_cols = features_col, output_col = "features")

  model <- ml_logistic_regression(
    x = x, # TODO filter for imputation mask ?
    features_col = "features",
    label_col = y,
    family = "auto", # Good choice ?
    #weight_col = "weight_col", # weights = w[ry], # TODO Account for weights ?
    max_iter = 100
  )
  # The object returned depends on the class of x.
  # If it is a spark_connection, the function returns a ml_estimator object.
  # If it is a ml_pipeline, it will return a pipeline with the predictor appended to it.
  # If a tbl_spark, it will return a tbl_spark with the predictions added to it.

  # beta <- coef(fit)
  # beta <- model$coefficients
  # rv <- t(chol(sym(fit.sum$cov.unscaled)))
  # Big problem: Spark ml_logistic_regression does not return the covariance matrix...
  # beta.star <- beta + rv %*% rnorm(ncol(rv))
  # Solution ? : use the bootstrap code instead ?
  beta.star <- model$coefficients
  # print(class(beta.star))
  # draw imputations
  # p <- 1 / (1 + exp(-(x[wy, , drop = FALSE] %*% beta.star)))

  # p is a vector of probabilities
  # p <- 1 / (1 + exp(-(x[wy, ] %*% beta.star)))
  # TODO: Test this code
  p <- x %>%
    ft_vector_assembler(input_cols = colnames(x), output_col = "features_vector") %>%
    mutate(linear_pred = ml_dot_product(features_vector, lit(beta_star))) %>%
    mutate(p = 1/(1 + exp(-linear_pred)))
  # If p is greater than a random number between 0 and 1, set the value to 1
  # vec <- (runif(nrow(p)) <= p)
  vec <- p %>%
    mutate(random_val = rand()) %>%
    mutate(vec = when(random_val <= p, 1, 0))

  #vec[vec] <- 1
  vec <- vec %>% mutate(vec = as.numeric(vec))
  # If y is a factor, convert vec to a factor
  # factor are specific to R, so no need for that ?
  # if (is.factor(y)) {
  #   vec <- factor(vec, c(0, 1), levels(y))
  # }
  vec
}

# TODO: modify this function to use Spark

augment <- function(y, ry, x, wy, maxcat = 50) {
  # define augmented data for stabilizing logreg and polyreg
  # by the ad hoc procedure of White, Daniel & Royston, CSDA, 2010
  # This function will prevent augmented data beyond the min and
  # the max of the data
  # Input:
  # x: numeric data.frame (n rows)
  # y: factor or numeric vector (lengt n)
  # ry: logical vector (length n)
  # Output:
  # return a list with elements y, ry, x, and w with length n+2*(ncol(x))*length(levels(y))
  # SvB May 2009
  icod <- sort(unique(unclass(y)))
  k <- length(icod)
  if (k > maxcat) {
    stop("Maximum number of categories (", maxcat, ") exceeded")
  }
  p <- ncol(x)

  # skip augmentation if there are no predictors
  if (p == 0) {
    return(list(y = y, ry = ry, x = x, wy = wy, w = rep(1, length(y))))
  }

  ## skip augmentation if there is only 1 missing value 12jul2012
  ## this need to be fixed 12jul2011
  if (sum(!ry) == 1) {
    return(list(y = y, ry = ry, x = x, wy = wy, w = rep(1, length(y))))
  }

  # calculate values to augment
  mean <- apply(x, 2, mean, na.rm = TRUE)
  sd <- sqrt(apply(x, 2, var, na.rm = TRUE))
  minx <- apply(x, 2, min, na.rm = TRUE)
  maxx <- apply(x, 2, max, na.rm = TRUE)
  nr <- 2 * p * k
  a <- matrix(mean, nrow = nr, ncol = p, byrow = TRUE)
  b <- matrix(rep(c(rep.int(c(0.5, -0.5), k), rep.int(0, nr)), length = nr * p), nrow = nr, ncol = p, byrow = FALSE)
  c <- matrix(sd, nrow = nr, ncol = p, byrow = TRUE)
  d <- a + b * c
  d <- pmax(matrix(minx, nrow = nr, ncol = p, byrow = TRUE), d, na.rm = TRUE)
  d <- pmin(matrix(maxx, nrow = nr, ncol = p, byrow = TRUE), d, na.rm = TRUE)
  e <- rep(rep(icod, each = 2), p)

  dimnames(d) <- list(paste0("AUG", seq_len(nrow(d))), dimnames(x)[[2]])
  xa <- rbind.data.frame(x, d)

  # beware, concatenation of factors
  ya <- if (is.factor(y)) as.factor(levels(y)[c(y, e)]) else c(y, e)
  rya <- c(ry, rep.int(TRUE, nr))
  wya <- c(wy, rep.int(FALSE, nr))
  wa <- c(rep.int(1, length(y)), rep.int((p + 1) / nr, nr))

  list(y = ya, ry = rya, x = xa, w = wa, wy = wya)
}

