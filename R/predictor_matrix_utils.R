create_predictor_matrix <- function(df, save=FALSE){
  # Utility function to create a predictor matrix for bigMICE
  # takes a dataframe and saves a excel file with the default predictor matrix
  # optional argument save to also save the matrix to an csv file to be modified (easier)

  # Extract variable names
  variables <- colnames(df)

  # Create a TRUE matrix with FALSE on the diagonal
  predictor_matrix <- matrix(TRUE, nrow = length(variables), ncol = length(variables),
                             dimnames = list(variables, variables))
  diag(predictor_matrix) <- FALSE  # Set diagonal to FALSE

  # save the matrix to an excel file
  if (save){
    write.csv(as.data.frame(predictor_matrix), "predictor_matrix.csv")
  }
  return(predictor_matrix)
}

import_predictor_matrix <- function(file){
  # Utility function to import a predictor matrix from an csv file
  # takes a file path and returns the matrix as a matrix object
  data <- read.csv(file)
  return(as.matrix(data))
}

# Example usage
path = "C:\\Users\\hugom\\Desktop\\CAMEO\\Code\\sesar_dummy_100.csv"
df <- read.csv(path)
predictor_matrix <- create_predictor_matrix(df, save=TRUE)
numeric_matrix <- ifelse(predictor_matrix, 1, 0)
heatmap(numeric_matrix, Rowv=NA, Colv=NA, col = c("white", "darkgreen"))

# Modify the predictor matrix in the csv file using excel or sheets
# i use google sheets where the TRUE/FALSE value can be changed using a quick drop down menu.
# It is however a tidious process to decide for each variable if it should be a predictor or not.
# And requires some domain knowledge.
path_modified = "C:\\Users\\hugom\\Downloads\\predictor_matrix_modified - predictor_matrix.csv"
predictor_matrix <- import_predictor_matrix(path_modified)
numeric_matrix <- ifelse(predictor_matrix, 1, 0)
heatmap(numeric_matrix, Rowv=NA, Colv=NA, col = c("white", "darkgreen"))
