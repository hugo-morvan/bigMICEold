# Modification of mice.impute.cart.R

mice.impute.spark.cart <- function(y, ry, x, wy = NULL, minbucket = 5, cp = 1e-04,
                             ...) {
  #TODO: - convert R dataframe to spark dataframe (createDataFrame)?
  #      or, automatically infer to use spark or not based on type of x and y (spark df or not)
  #     - check the fields of the predict object of the spark decision tree
  #     - pass in the spark session object ? 
  install.on.demand("rpart")
  install.on.demand("sparklyr")
  
  if (is.null(wy)) {
    wy <- !ry
  }
  minbucket <- max(1, minbucket)
  if (dim(x)[2] == 0) {
    x <- cbind(x, 1)
    dimnames(x) <- list(NULL, "int")
  }
  
  xobs <- data.frame(x[ry, , drop = FALSE]) # xobs is the observed data
  xmis <- data.frame(x[wy, , drop = FALSE]) # xmis is the missing data
  yobs <- y[ry] 
  
  # if (!is.factor(yobs)) { # regression
  #   fit <- rpart::rpart(yobs ~ .,
  #                       data = cbind(yobs, xobs), method = "anova",
  #                       control = rpart::rpart.control(minbucket = minbucket, cp = cp, ...)
  #   )
  #   leafnr <- floor(as.numeric(row.names(fit$frame[fit$where, ])))
  #   fit$frame$yval <- as.numeric(row.names(fit$frame))
  #   nodes <- predict(object = fit, newdata = xmis)
  #   donor <- lapply(nodes, function(s) yobs[leafnr == s])
  #   impute <- vapply(seq_along(donor), function(s) sample(donor[[s]], 1), numeric(1))
  # } else { # classification
  #   # escape with same impute if the dependent does not vary
  #   cat.has.all.obs <- table(yobs) == sum(ry)
  #   if (any(cat.has.all.obs)) {
  #     return(rep(levels(yobs)[cat.has.all.obs], sum(wy)))
  #   }
  #   
  #   xy <- cbind(yobs, xobs)
  #   xy <- droplevels(xy)
  #   # FIXME: rpart fails to runs on empty categories in yobs,
  #   # droplevels() removes empty levels, and this is
  #   # likely to present problems further down the road
  #   # potential problem case: table(yobs): 0 10 15, then
  #   # droplevels may forget about category 1
  #   fit <- rpart::rpart(yobs ~ .,
  #                       data = xy, method = "class",
  #                       control = rpart::rpart.control(minbucket = minbucket, cp = cp, ...)
  #   )
  #   nodes <- predict(object = fit, newdata = xmis)
  #   impute <- apply(nodes,
  #                   MARGIN = 1,
  #                   FUN = function(s) {
  #                     sample(colnames(nodes),
  #                            size = 1, prob = s
  #                     )
  #                   }
  #   )
  # }
  
  # %%%%%%%%%  Sparklyr adaptation: %%%%%%%%%
  if (!is.factor(yobs)) { 
    # --- Regression ---
    # use spark decision tree in regression mode with same control parameters
    spark_data = ...
    fit <- ml_decision_tree_regressor(x = spark_data, 
                                      formula = yobs ~ . ,
                                      min_instances_per_node = minbucket,
                                      min_info_gain = cp
                                      )
    
    leafnr <- floor(as.numeric(row.names(fit$frame[fit$where, ])))
    fit$frame$yval <- as.numeric(row.names(fit$frame))
    # Predict:
    nodes <- predict(object = fit, newdata = xmis)
    donor <- lapply(nodes, function(s) yobs[leafnr == s])
    impute <- vapply(seq_along(donor), function(s) sample(donor[[s]], 1), numeric(1))
  } else { 
    
    # --- Classification ---
    # escape with same impute if the dependent does not vary
    cat.has.all.obs <- table(yobs) == sum(ry)
    if (any(cat.has.all.obs)) {
      return(rep(levels(yobs)[cat.has.all.obs], sum(wy)))
    }
    
    xy <- cbind(yobs, xobs)
    xy <- droplevels(xy)
    spark_data = ...
    # use spark decision tree in classification mode with same control parameters
    fit <- ml_decision_tree_classifier(x = spark_data,
                                       formula = yobs ~ .,
                                       min_instances_per_node = minbucket,
                                       min_info_gain = cp
                                       )
    # Predict:
    nodes <- predict(object = fit, newdata = xmis)
    
    impute <- apply(nodes,
                    MARGIN = 1,
                    FUN = function(s) {
                      sample(colnames(nodes),
                             size = 1, prob = s
                      )
                    }
    )
  }
  
  
  
  impute
}
