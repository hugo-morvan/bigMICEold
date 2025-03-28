mice.spark <- function(data,
                       sc,
                       variable_types, # Used for initialization and method selection
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
                       imp_init = NULL,
                       ...) {


  if (!is.na(seed)) set.seed(seed)

  # check form of data and m
  #data <- check.spark.dataform(data)
  cols <- names(variable_types)
  #m <- check.m(m)


  from <- 1
  to <- from + maxit - 1

  # INITIALISE THE IMPUTATION USING Mean/Mode/Median SAMPLING

  # Do this inside or outside the m loop ?
  # Do I want each imputation to start from the same sample or have more variation in initial condition ?

  #TODO : add support for column parameter in initialisation

  # Dictionnary to infer initialization method based on variable type
  # Should be one of (mean, mode, median, none), and be used as input of MeMoMe function
  init_dict <- c("Binary" = "mode",
                 "Nominal" = "mode",
                 "Ordinal" = "mode",
                 "Code (don't impute)" = "none", # LopNr, Unit_code etc...
                 "Continuous_int" = "median",
                 "Continuous_float" = "mean",
                 "smalldatetime" = "none",
                 "String" = "none", #TBD
                 "Count" = "median", #TBD
                 "Semi-continuous" = "none", #TBD
                 "Else", "none")

  init_modes <- replace(variable_types, variable_types %in% names(init_dict), init_dict[variable_types])
  names(init_modes) <- cols
  print("**DEBUG**: init_modes:")
  print(init_modes)

  cat("\nStarting initialisation\n")
  print(" ")

  print(length(init_modes))
  print(sdf_ncol(data))

  init_start_time <- proc.time()
  if(is.null(imp_init)){
    imp_init <- impute_with_MeMoMe(sc = sc,
                                  sdf = data,
                                  column = NULL, #TODO: add support for this
                                  impute_mode = init_modes)
  }else{
    print("initial imputation provided manually, I hope it is correct")
    imp_init <- imp_init # User provided initiale imputation
  }

  init_end_time <- proc.time()
  init_elapsed <- (init_end_time-init_start_time)['elapsed']
  cat("Initalisation time:", init_elapsed)
  # TODO : Add elapse time to the result dataframe (and create result dataframe)

  # initialize return object (?) :
  imputation_results = c()

  # FOR EACH IMPUTATION SET i = 1, ..., m
  for (i in 1:m) {
    cat("Iteration: ", i, "\n")

    # Run the imputation algorithm
    cat("Starting imputation")

    imp_start_time <- proc.time()

    imp <- sampler.spark(sc = sc,
                         data = data,
                         imp_init = imp_init,
                         fromto = c(from, to),
                         var_types = variable_types,
                         printFlag = printFlag)

    imp_end_time <- proc.time()
    imp_elapsed <- (imp_end_time-imp_start_time)['elapsed']
    cat("Imputation time:", imp_elapsed)

    # Save imputation to dataframe ?
    imputation_results = rbind(imputation_results, imp)

    # Calculate Rubin Rules statistics
    # TODO
  }

  return(imputation_results)
}

sampler.spark <- function(sc,
                          data,
                          imp_init,
                          fromto,
                          var_types,
                          printFlag){

  # This function takes a sparc connection sc, a spark dataframe data, an initial imputation (by random sampling),
  # and a range of iteration fromto (0-maxit) (done like this to follow mice() format)

  #TODO; add support for functionalities present in the mice() function (where, ignore, blocks, predictorMatrix, formula, ...)

  # For iteration k in fromto
  from = fromto[1]
  to = fromto[2]

  var_names <- names(sparklyr::sdf_schema(data))

  # Method dictionary for imputation. Can change as desired
  # TODO: implement; keep this as default, or use user-provided dict ?
  method_dict <- c("Binary" = "Logistic",
                   "Nominal" = "Mult_Logistic",
                   "Ordinal" = "RandomForestClassifier",
                   "Code (don't impute)" = "none", # LopNr, Unit_code etc...
                   "Continuous_int" = "Linear",
                   "Continuous_float" = "Linear",
                   "smalldatetime" = "none",
                   "String" = "none", #TBD
                   "Count" = "RandomForestClassifier", #TBD
                   "Semi-continuous" = "none", #TBD
                   "Else" = "none")

  imp_methods <- replace(var_types, var_types %in% names(method_dict), method_dict[var_types])
  names(imp_methods) <- var_names
  print(imp_methods)
  result <- imp_init # Is this a valid copy ?
  num_vars <- length(var_names)

  for (k in from:to){
    cat("\n iteration: ", k)
    # For each variable j in the data
    j <- 0
    for (var_j in var_names){
      j <- j+1
      cat("\n",j,"/",num_vars,"Imputing variable", var_j," using method ")
      # Obtain the variables use to predict the missing values of variable j and create feature column
      label_col <- var_j

      feature_cols <- setdiff(var_names, label_col)
      #print(feature_cols)
      #Filter out Date data type
        feature_cols <- feature_cols[sapply(var_types[feature_cols],
                                            function(x) !(x %in% c("String", "smalldatetime")))]

      # Replace random sample values in label column with the original missing values
      j_df <- result %>%
        sparklyr::select(-label_col) %>%
        cbind(data %>% sparklyr::select(all_of(label_col)))


      method <- imp_methods[[var_j]]
      cat(method)

      result <- switch(method,
         "Logistic" = impute_with_logistic_regression(sc, j_df, label_col, feature_cols),
         "Mult_Logistic" = impute_with_mult_logistic_regression(sc, j_df, label_col, feature_cols),
         "Linear" = impute_with_linear_regression(sc, j_df, label_col, feature_cols),
         "RandomForestClassifier" = impute_with_random_forest_classifier(sc, j_df, label_col, feature_cols),
         "none" = j_df, # don't impute this variable
         "Invalid method"  # Default case
      )
      #Use the result to do something to the original dataset
      #print(result)
      # To avoid stacjoverflow error, break the lineage after each imputation
      # Might not be necessary at every iteration. only run into error after ~25 imputation
      result %>% sdf_persist()
    } #end of var_j loop

  } #end of k loop
  # The sampler has finish his iterative work, can now return the imputed dataset ?
  return(result)
}


library(sparklyr)
library(dplyr)


conf <- spark_config()
conf$`sparklyr.shell.driver-memory`<- "128G"
conf$spark.memory.fraction <- 0.6
conf$`sparklyr.cores.local` <- 24

sc = spark_connect(master = "local", config = conf)

path_DORS =     "/vault/hugmo418_amed/NDR-SESAR/Uttag Socialstyrelsen 2024/ut_r_par_ov_63851_2023.csv"
path_NDR =      "/vault/hugmo418_amed/NDR-SESAR/Uttag SCB+NDR+SESAR 2024/FI_Lev_NDR.csv"
path_SESAR_FU = "/vault/hugmo418_amed/NDR-SESAR/Uttag SCB+NDR+SESAR 2024/FI_Lev_SESAR_FU.csv"
path_SESAR_IV = "/vault/hugmo418_amed/NDR-SESAR/Uttag SCB+NDR+SESAR 2024/FI_Lev_SESAR_IV.csv"
path_SESAR_TS = "/vault/hugmo418_amed/NDR-SESAR/Uttag SCB+NDR+SESAR 2024/FI_Lev_SESAR_TS.csv"
path_small_SESAR_IV = "/vault/hugmo418_amed/subsets_thesis_hugo/small_IV.csv"

data_small <- spark_read_csv(sc, name="df", path=path_small_SESAR_IV, infer_schema=TRUE, null_value='NA')
#data_small <- spark_read_csv(sc, name = "df",path = path_SESAR_FU,infer_schema = TRUE, null_value = 'NA')


# In FU, lopNr and SenPNr contain missing values, so i remove those columns for simplicity
data_small <- data_small %>%
  select(-c("LopNr","IV_SenPNr")) %>%
  select(sort(colnames(.))) %>% # Order alphabetically
  mutate(IV_AtrialFibrillation2 = as.numeric(IV_AtrialFibrillation2)) %>%
  mutate(IV_CerebrovascDisease2 = as.numeric(IV_CerebrovascDisease2)) %>%
  mutate(IV_CopdAsthma2 = as.numeric(IV_CopdAsthma2)) %>%
  mutate(IV_CoronaryHeartDisease2 = as.numeric(IV_CoronaryHeartDisease2)) %>%
  mutate(IV_Depression2 = as.numeric(IV_Depression2)) %>%
  mutate(IV_Diabetes2 = as.numeric(IV_Diabetes2)) %>%
  mutate(IV_HeartFailure2 = as.numeric(IV_HeartFailure2)) %>%
  mutate(IV_Hypertension2 = as.numeric(IV_Hypertension2)) %>%
  mutate(IV_TherapyCPAPX = as.numeric(IV_TherapyCPAPX)) %>%
  mutate(IV_TherapyBilevelPAPX = as.numeric(IV_TherapyBilevelPAPX)) %>%
  mutate(IV_TherapySplintX = as.numeric(IV_TherapySplintX)) %>%
  mutate(IV_TherapySurgeryX = as.numeric(IV_TherapySurgeryX))%>%
  mutate(IV_TherapyWeightX = as.numeric(IV_TherapyWeightX))%>%
  mutate(IV_PositionTherapyX = as.numeric(IV_PositionTherapyX))


dataset_info_IV = "Datasets info - Sesar_IV.csv"
data_info <- read.csv(dataset_info_IV)

get_named_vector <- function(csv_file) {
  data <- read.csv(csv_file, stringsAsFactors = FALSE)
  named_vector <- setNames(data$Variable.type, data$Variable.Name)
  names(named_vector)[names(named_vector) == "Age"] <- "IV_Age"
  ordered_vector <- named_vector[order(names(named_vector))] #Order alphabet.
  return(ordered_vector)
}

variable_types <- get_named_vector(dataset_info_IV)
print(variable_types)
variable_types <- variable_types[!(names(variable_types) %in% c("LopNr", "SenPNr"))]

colnames(data_small)


init_dict <- c("Binary" = "mode",
               "Nominal" = "mode",
               "Ordinal" = "mode",
               "Code (don't impute)" = "none", # LopNr, Unit_code etc...
               "Continuous_int" = "median",
               "Continuous_float" = "mean",
               "smalldatetime" = "none",
               "String" = "none", #TBD
               "Count" = "median", #TBD
               "Semi-continuous" = "none", #TBD
               "Else", "none")

impute_modes <- replace(variable_types, variable_types %in% names(init_dict), init_dict[variable_types])

data_small %>% sdf_persist(name = "data")

imput_init <- impute_with_MeMoMe(sc, data_small, impute_mode = impute_modes)

imput_init %>% sdf_persist()

imputed_results = mice.spark(data_small,
                             sc,
                             variable_types,
                             m = 2,
                             imp_init = imput_init)

#spark_disconnect(sc)
