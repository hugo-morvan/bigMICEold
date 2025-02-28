#' Imputation by linear regression, bootstrap method
#'
#' Imputes univariate missing data using linear regression with bootstrap

mice.impute.norm.boot <- function(y, ry, x, wy = NULL, ...) {
  if (is.null(wy)) wy <- !ry
  x <- cbind(1, as.matrix(x))
  n1 <- sum(ry)
  s <- sample(n1, n1, replace = TRUE)
  ss <- s
  dotxobs <- x[ry, , drop = FALSE][s, ]
  dotyobs <- y[ry][s]
  p <- estimice(dotxobs, dotyobs, ...)
  sigma <- sqrt((sum(p$r^2)) / (n1 - ncol(x) - 1))
  x[wy, ] %*% p$c + rnorm(sum(wy)) * sigma
}
