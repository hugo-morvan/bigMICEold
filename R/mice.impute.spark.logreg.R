#' Imputation by logistic regression
#'
#' Imputes univariate missing data using logistic regression.
#'
#' @aliases mice.impute.logreg
#' @inheritParams mice.impute.pmm
#' @param ... Other named arguments.
#' @return Vector with imputed data, same type as \code{y}, and of length
#' \code{sum(wy)}
#' @author Stef van Buuren, Karin Groothuis-Oudshoorn
#' @details
#' Imputation for binary response variables by the Bayesian logistic regression
#' model (Rubin 1987, p. 169-170).  The
#' Bayesian method consists of the following steps:
#' \enumerate{
#' \item Fit a logit, and find (bhat, V(bhat))
#' \item Draw BETA from N(bhat, V(bhat))
#' \item Compute predicted scores for m.d., i.e. logit-1(X BETA)
#' \item Compare the score to a random (0,1) deviate, and impute.
#' }
#' The method relies on the
#' standard \code{glm.fit} function. Warnings from \code{glm.fit} are
#' suppressed. Perfect prediction is handled by the data augmentation
#' method.
#'
#' @seealso \code{\link{mice}}, \code{\link{glm}}, \code{\link{glm.fit}}
#' @references Van Buuren, S., Groothuis-Oudshoorn, K. (2011). \code{mice}:
#' Multivariate Imputation by Chained Equations in \code{R}. \emph{Journal of
#' Statistical Software}, \bold{45}(3), 1-67.
#' \doi{10.18637/jss.v045.i03}
#'
#' Brand, J.P.L. (1999). Development, Implementation and Evaluation of Multiple
#' Imputation Strategies for the Statistical Analysis of Incomplete Data Sets.
#' Ph.D. Thesis, TNO Prevention and Health/Erasmus University Rotterdam. ISBN
#' 90-74479-08-1.
#'
#' Venables, W.N. & Ripley, B.D. (1997). Modern applied statistics with S-Plus
#' (2nd ed). Springer, Berlin.
#'
#' White, I., Daniel, R. and Royston, P (2010). Avoiding bias due to perfect
#' prediction in multiple imputation of incomplete categorical variables.
#' Computational Statistics and Data Analysis, 54:22672275.
#' @family univariate imputation functions
#' @keywords datagen
#' @export

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
  # model <- training_data %>% ml_logistic_regression(label ~ features)

  model <- ml_logistic_regression(
    x = x[ry, , drop = FALSE],
    features_col = "features", #TODO: replace this
    label_col = "label",       #TODO: replace this #TODO: replace this
    family = "auto", # Good choice ?
    weight_col = "weight_col", # weights = w[ry],
    max_iter = 100
  )
  # The object returned depends on the class of x.
  # If it is a spark_connection, the function returns a ml_estimator object.
  # If it is a ml_pipeline, it will return a pipeline with the predictor appended to it.
  # If a tbl_spark, it will return a tbl_spark with the predictions added to it.
  # TODO Run some test to understand of this all work. you can do it Hugo
  fit <- ml_fit(model) #?
  model_summary <- summary(model)
  print(model_summary)
  pred <- ml_predict(fit, test_data)

  # draw imputations
  p <- 1 / (1 + exp(-(x[wy, , drop = FALSE] %*% beta.star)))
  vec <- (runif(nrow(p)) <= p)
  vec[vec] <- 1
  if (is.factor(y)) {
    vec <- factor(vec, c(0, 1), levels(y))
  }
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

