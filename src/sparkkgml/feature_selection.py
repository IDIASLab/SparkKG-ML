from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
import numpy as np

def correlation_feature_selection(df, threshold, features_arr):
    
    """
        Perform correlation-based feature selection on a DataFrame.

        Args:
            df (pyspark.sql.DataFrame): The input DataFrame.
            threshold (float): The correlation threshold.
            features_arr (list[str]): A list of feature column names.

        Returns:
            pyspark.sql.DataFrame: The resulting DataFrame with selected non-correlated features.

        Notes:
            This function performs feature selection based on correlation. It follows these steps:

            1. Assemble the specified feature columns into a vector column.

            2. Compute the correlation matrix of the assembled features.

            3. Identify features that have low absolute correlation with all other features.

            4. Select the identified non-correlated features.

            5. Return the input DataFrame with only the selected non-correlated feature columns.
    """
    
    
    assembler = VectorAssembler(inputCols=features_arr, outputCol="features")
    df_assembled = assembler.transform(df).select("features")
    
    corr_mat = (Correlation.corr(df_assembled, 'features').collect()[0][0]).toArray()
    new_columns=[]
    
    for i in range(len(features_arr)):
        flag=False
        for j in range(i+1, len(features_arr)):
            #check if it has high corr with any other column
            if np.abs(corr_mat[i][j]) > threshold:
                flag=True
        #add to new column list if it has no high corr with any other column        
        if flag==False:
            new_columns.append(features_arr[i])
    
    return df.select(new_columns)


def sequential_feature_selection(df, ml_model, cross_validator, evaluator, label, feature_cols, threshold=0.1, descending=True):
    """
    Performs sequential feature selection based on cross-validation accuracy.

    Args:
        df (pyspark.sql.DataFrame): The input DataFrame containing the features and label.
        ml_model: The machine learning model to be used for training.
        cross_validator: The cross-validator for hyperparameter tuning.
        evaluator: The evaluator for model performance assessment.
        label (str): The name of the label column.
        feature_cols (list): List of feature column names.
        threshold (float, optional): The threshold for improvement in accuracy to add a feature. Default is 0.1.
        descending (bool, optional): If True, features are sorted in descending order of accuracy. Default is True.

    Returns:
        list: A list of selected feature column names.

     Explanation:
        The features are initially sorted based on their accuracy, with the option to sort in descending or ascending order. 
        The selection process starts by choosing the feature with the highest accuracy as the initial feature. 
        Subsequently, the function sequentially adds features to the selection based on the sorted list. 
        For each added feature, it evaluates the cross-validation performance and checks if the inclusion of the feature improves the performance by a specified threshold. If the performance improves, the feature is included in the selected set; otherwise, it is excluded.
        The final output is a list of selected feature column names representing the optimal subset that maximizes the cross-validation performance.
    """

    #Prepocess to choose initial column 
    accuracy_list=[]
    
    for i in range(len(feature_cols)):
        # Create a vector assembler to assemble all the feature column into a single vector column
        assembler = VectorAssembler(inputCols=[feature_cols[i]], outputCol="features")
        initialFeaturesDf = assembler.transform(df)

        # train the model
        model = ml_model.fit(initialFeaturesDf)
        predictions = model.transform(initialFeaturesDf)
        accuracy = evaluator.evaluate(predictions)

        accuracy_list.append(accuracy)
        
    # sort the columns according to their accuracy along with their indices in original df
    sorted_numbers_with_indices = sorted(enumerate(accuracy_list), key=lambda x: x[1], reverse=descending)
    #print(sorted_numbers_with_indices)
    # have list of sorted columns according to original df indices (sequential selection will start according to these indice orders)
    indices = [x[0] for x in sorted_numbers_with_indices]

    # Create a list to store the selected features
    selected_features = [feature_cols[indices[0]]]    
        
    # Create a vector assembler to assemble all the feature columns(just first one) into a single vector column
    assembler = VectorAssembler(inputCols=selected_features, outputCol="features")
    featuresDf = assembler.transform(df)    

    # Define the initial performance as the cross-validation performance with only the first feature
    cv_model = cross_validator.fit(featuresDf)
    best_score = evaluator.evaluate(cv_model.transform(featuresDf))

    
    # Loop over the remaining feature columns in ascending order of sum of erros 
    # add the next best feature only if it improves the cross-validation performance
    for k in indices[1:]:
        # Add the current feature to the list of selected features
        selected_features.append(feature_cols[k])
      
        # combine new set as input features
        assembler = VectorAssembler(inputCols=selected_features, outputCol="features")
        featuresDf2 = assembler.transform(df)

        # Fit the cross-validator on the features and get the AUC score
        cv_model = cross_validator.fit(featuresDf2)
        accuracy = evaluator.evaluate(cv_model.transform(featuresDf2))

        # Check if the performance has improved compared to the previous best performance
        if accuracy - best_score > threshold:
            #print('improvement: ',auc_score - best_score)
            #print('added ', k)
            best_score = accuracy
        else:
            #print('difference: ',auc_score - best_score)
            #print('dropped ', k)
            # If the performance has not improved, remove the last added feature from the list of selected features
            selected_features.remove(feature_cols[k])
    
    return selected_features