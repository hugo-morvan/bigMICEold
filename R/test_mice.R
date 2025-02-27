# load SESAR_TS dummy data
library(sparklyr)
library(dplyr)

sc <- spark_connect(master = "local")

dummy_data <- spark_read_csv(sc, name = "df", path = "C:\\Users\\hugom\\Desktop\\CAMEO\\Code\\sesar_dummy_1000_020MVR.csv")

# do default multiple imputation on a numeric matrix
imp <- mice.spark(dummy_data)
imp

# list the actual imputations for BMI
imp$imp$bmi

# first completed data matrix
complete(imp)

# imputation on mixed data with a different method per column
mice(nhanes2, meth = c("sample", "pmm", "logreg", "norm"))

## Not run:
# example where we fit the imputation model on the train data
# and apply the model to impute the test data
set.seed(123)
ignore <- sample(c(TRUE, FALSE), size = 25, replace = TRUE, prob = c(0.3, 0.7))

# scenario 1: train and test in the same dataset
imp <- mice(nhanes2, m = 2, ignore = ignore, print = FALSE, seed = 22112)
imp.test1 <- filter(imp, ignore)
imp.test1$data
complete(imp.test1, 1)
complete(imp.test1, 2)

# scenario 2: train and test in separate datasets
traindata <- nhanes2[!ignore, ]
testdata <- nhanes2[ignore, ]
imp.train <- mice(traindata, m = 2, print = FALSE, seed = 22112)
imp.test2 <- mice.mids(imp.train, newdata = testdata)
complete(imp.test2, 1)
complete(imp.test2, 2)
