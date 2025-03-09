# IDEA: Keep R dataframes up until sampling steps.
# use package bigmemory ?

mice_half_spark <- function(data,
                 m = 5,
                 method = NULL,
                 predictorMatrix,
                 ignore = NULL,
                 where = NULL,
                 blocks,
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
  check.deprecated(...)

  if (!is.na(seed)) set.seed(seed)

  # check form of data and m
  data <- check.dataform(data)
  m <- check.m(m)

  # determine input combination: predictorMatrix, blocks, formulas
  mp <- missing(predictorMatrix)
  mb <- missing(blocks)
  mf <- missing(formulas)

  # case A
  if (mp & mb & mf) {
    # blocks lead
    blocks <- make.blocks(colnames(data))
    predictorMatrix <- make.predictorMatrix(data, blocks)
    formulas <- make.formulas(data, blocks)
    modeltype <- make.modeltype(modeltype, predictorMatrix, formulas, "pred")
  }
  # case B
  if (!mp & mb & mf) {
    # predictorMatrix leads
    predictorMatrix <- check.predictorMatrix(predictorMatrix, data)
    blocks <- make.blocks(colnames(predictorMatrix), partition = "scatter")
    formulas <- make.formulas(data, blocks, predictorMatrix = predictorMatrix)
    modeltype <- make.modeltype(modeltype, predictorMatrix, formulas, "pred")
  }

  # case C
  if (mp & !mb & mf) {
    # blocks leads
    blocks <- check.blocks(blocks, data)
    predictorMatrix <- make.predictorMatrix(data, blocks)
    formulas <- make.formulas(data, blocks)
    modeltype <- make.modeltype(modeltype, predictorMatrix, formulas, "pred")
  }

  # case D
  if (mp & mb & !mf) {
    # formulas leads
    formulas <- check.formulas(formulas, data)
    blocks <- construct.blocks(formulas)
    predictorMatrix <- make.predictorMatrix(data, blocks)
    modeltype <- make.modeltype(modeltype, predictorMatrix, formulas, "formula")
  }

  # case E
  if (!mp & !mb & mf) {
    # predictor leads
    blocks <- check.blocks(blocks, data)
    z <- check.predictorMatrix(predictorMatrix, data, blocks)
    predictorMatrix <- z$predictorMatrix
    blocks <- z$blocks
    formulas <- make.formulas(data, blocks, predictorMatrix = predictorMatrix)
    modeltype <- make.modeltype(modeltype, predictorMatrix, formulas, "pred")
  }

  # case F
  if (!mp & mb & !mf) {
    # formulas lead
    formulas <- check.formulas(formulas, data)
    predictorMatrix <- check.predictorMatrix(predictorMatrix, data)
    blocks <- construct.blocks(formulas, predictorMatrix)
    predictorMatrix <- make.predictorMatrix(data, blocks, predictorMatrix)
    modeltype <- make.modeltype(modeltype, predictorMatrix, formulas, "formula")
  }

  # case G
  if (mp & !mb & !mf) {
    # blocks lead
    blocks <- check.blocks(blocks, data)
    formulas <- check.formulas(formulas, blocks)
    predictorMatrix <- make.predictorMatrix(data, blocks)
    modeltype <- make.modeltype(modeltype, predictorMatrix, formulas, "formula")
  }

  # case H
  if (!mp & !mb & !mf) {
    # blocks lead
    blocks <- check.blocks(blocks, data)
    formulas <- check.formulas(formulas, data)
    predictorMatrix <- check.predictorMatrix(predictorMatrix, data, blocks)
    modeltype <- make.modeltype(modeltype, predictorMatrix, formulas, "formula")
  }

  chk <- check.cluster(data, predictorMatrix)
  where <- check.where(where, data, blocks)

  # check visitSequence, edit predictorMatrix for monotone
  user.visitSequence <- visitSequence
  visitSequence <- check.visitSequence(visitSequence,
    data = data, where = where, blocks = blocks
  )
  predictorMatrix <- mice.edit.predictorMatrix(
    predictorMatrix = predictorMatrix,
    visitSequence = visitSequence,
    user.visitSequence = user.visitSequence,
    maxit = maxit
  )
  method <- check.method(
    method = method, data = data, where = where,
    blocks = blocks, defaultMethod = defaultMethod
  )
  post <- check.post(post, data)
  blots <- check.blots(blots, data, blocks)
  ignore <- check.ignore(ignore, data)

  # data frame for storing the event log
  state <- list(it = 0, im = 0, dep = "", meth = "", log = FALSE)
  loggedEvents <- data.frame(it = 0, im = 0, dep = "", meth = "", out = "")

  # edit imputation setup
  setup <- list(
    method = method,
    predictorMatrix = predictorMatrix,
    visitSequence = visitSequence,
    post = post
  )
  setup <- mice.edit.setup(data, setup, ...)
  method <- setup$method
  predictorMatrix <- setup$predictorMatrix
  visitSequence <- setup$visitSequence
  post <- setup$post

  # initialize imputations
  nmis <- apply(is.na(data), 2, sum)
  imp <- initialize.imp(
    data, m, ignore, where, blocks, visitSequence,
    method, nmis, data.init
  )

  # and iterate...
  from <- 1
  to <- from + maxit - 1
  q <- spark.sampler(
    data, m, ignore, where, imp, blocks, method,
    visitSequence, predictorMatrix, formulas,
    modeltype, blots,
    post, c(from, to), printFlag, ...
  ) # q contains the result from

  if (!state$log) loggedEvents <- NULL
  if (state$log) row.names(loggedEvents) <- seq_len(nrow(loggedEvents))

  ## save, and return
  midsobj <- mids(
    data = data,
    imp = q$imp,
    m = m,
    where = where,
    blocks = blocks,
    call = call,
    nmis = nmis,
    method = method,
    predictorMatrix = predictorMatrix,
    visitSequence = visitSequence,
    formulas = formulas,
    modeltype = modeltype,
    post = post,
    blots = blots,
    ignore = ignore,
    seed = seed,
    iteration = q$iteration,
    lastSeedValue = get(".Random.seed",
      envir = globalenv(), mode = "integer",
      inherits = FALSE),
    chainMean = q$chainMean,
    chainVar = q$chainVar,
    loggedEvents = loggedEvents)

  if (!is.null(midsobj$loggedEvents)) {
    warning("Number of logged events: ", nrow(midsobj$loggedEvents),
      call. = FALSE
    )
  }
  return(midsobj)
}

