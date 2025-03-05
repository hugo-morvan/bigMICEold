# load SESAR_TS dummy data
library(sparklyr)
library(dplyr)

sc <- spark_connect(master = "local")

dummy_data <- spark_read_csv(sc, name = "df",
      path = "C:\\Users\\hugom\\Desktop\\CAMEO\\Code\\sesar_dummy_100.csv",
      infer_schema = TRUE)

holy_data <- spark_read_csv(sc, name = "df_holy",
                             path = "C:\\Users\\hugom\\Desktop\\CAMEO\\Code\\sesar_dummy_100_020MVR.csv",
                             infer_schema = TRUE)
r_holy_data <- read.csv("C:\\Users\\hugom\\Desktop\\CAMEO\\Code\\sesar_dummy_100_020MVR.csv")

res <- mice_half_spark(r_holy_data)

res <- mice(r_holy_data)

res$predictorMatrix
heatmap(res$predictorMatrix)

class(dummy_data)

cols <- sparklyr::sdf_schema(dummy_data)

#print unique column types
unique_types <- unique(sapply(cols, function(x) x$type))
# Column to impute
label_col <- "FU_SplintStill"
label_col <- "FU_CPAPStill" # Three classes (0, 1, 9)
# PROBLEM: Having 9 instead of 2 makes the logistic regression think there are 10 classes
# SOLUTION: Replace 9 with 2 ?

# Other binary columns: FU_SplintAdverse2, FU_SplintStill

# Column to use as features (all others)
features_col <- setdiff(names(cols), label_col)
# features_col <- features_col[sapply(cols[features_col],
#                                     function(x) x$type %in% c("IntegerType", "DoubleType"))]

# Drop String type, DateType, TimestampType columns
features_col <- features_col[sapply(cols[features_col],
                                    function(x) !x$type %in% c("StringType", "DateType", "TimestampType"))]

#Drop the features that are not IntegerType
# dummy_data <- dummy_data %>% select(any_of(c(features_col, label_col)))

#Convert the label column to a binary column
# dummy_data <- dummy_data %>% mutate(FU_SplintStill = ifelse(FU_SplintStill == 1, 1, 0))

# Assemble multiple features into a single vector column
dummy_data <- dummy_data %>%
  ft_vector_assembler(input_cols = features_col, output_col = "features")



weight_col <- NULL

# model_formula <- ft_r_formula(x = dummy_data, formula = FU_SplintStill ~ .)
# print(model_formula$lazy_query$x)
# print(model_formula$lazy_query$vars)
# as.character(model_formula)

# Cannot have missing values in the input columns => needs to pre-fill them, but remember which were pre-filled

model <- ml_logistic_regression(
  x = dummy_data,
  # formula = FU_SplintStill ~ .,
  features_col = "features", # Needs to be a vector column with all the features (created via ft_vector_assembler)
  label_col = label_col,
  family = "auto",
  # weight_col = weight_col, # weights = w[ry],
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


# %%%%%%%%%%%%%%%%% CART %%%%%%%%%%%%%%%%%%%

library(sparklyr)
library(dplyr)

sc <- spark_connect(master = "local")

dummy_data <- spark_read_csv(sc, name = "df",
                             path = "C:\\Users\\hugom\\Desktop\\CAMEO\\Code\\sesar_dummy_100.csv",
                             infer_schema = TRUE)

class(dummy_data)

cols <- sparklyr::sdf_schema(dummy_data)

label_col = "FU_SplintStill"

features_col <- setdiff(names(cols), label_col)

features_col <- features_col[sapply(cols[features_col],
                                    function(x) !x$type %in% c("StringType", "DateType", "TimestampType"))]
dummy_data <- dummy_data %>%
  ft_vector_assembler(input_cols = features_col, output_col = "features")



model <- ml_decision_tree(
  x = dummy_data,
  type = "auto",
  features_col = "features",
  label_col = label_col,
  max_depth = 5
)

minimal_features <- head(features_col, 2)  # Use only the first two features
dummy_data_minimal <- dummy_data %>%
  ft_vector_assembler(input_cols = minimal_features, output_col = "minimal_features")

model_minimal <- ml_decision_tree(
  x = dummy_data_minimal,
  type = "auto",
  features_col = "minimal_features",
  label_col = label_col,
  max_depth = 3
)

label_col %in% names(cols)

# %%%%%%%%%%%%%%%%% Linear Regression %%%%%%%%%%%%%%%%%%%

library(sparklyr)
library(dplyr)

sc <- spark_connect(master = "local")

dummy_data <- spark_read_csv(sc, name = "df",
                             path = "C:\\Users\\hugom\\Desktop\\CAMEO\\Code\\sesar_dummy_100.csv",
                             infer_schema = TRUE)

class(dummy_data)

cols <- sparklyr::sdf_schema(dummy_data)

label_col = "IV_Weight"

features_col <- setdiff(names(cols), label_col)

features_col <- features_col[sapply(cols[features_col],
                                    function(x) !x$type %in% c("StringType", "DateType", "TimestampType"))]
dummy_data <- dummy_data %>%
  ft_vector_assembler(input_cols = features_col, output_col = "features")



model <- ml_linear_regression(
  x = dummy_data,
  features_col = "features",
  label_col = label_col,
)

coefficients <- model$coefficients
intercept <- model$intercept

model_summary <- model$summary
print(model_summary)
names(model_summary)
for(name in names(model_summary) ){
  print(name)
  print(model_summary[[name]])
}
model_summary$r2
model_summary$deviance_residuals()

model_summary$residuals()

imputations <- ml_predict(model, dummy_data)
