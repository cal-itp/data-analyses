from sklearn.cluster import AgglomerativeClustering 
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from scipy.cluster.hierarchy import linkage
import pandas as pd
    
def make_hierarchal_clustering(
    data: pd.DataFrame,
    num_cols: list,
    cat_cols: list,
    cluster_num = int
):
    """
    uses sklearns hierarchal/agglomerative clustering algo with ward's linkage.
    Returns dataframe with "cluster_name" column and feature array to be used for dendrogram.
    """
    
    
    preprocessor = ColumnTransformer(
    [
        ("numerical", StandardScaler(), num_cols),
        ("categorical", OneHotEncoder(drop="first", sparse_output=False), cat_cols)
    ]
)
    pipeline = Pipeline(
    [
        ("preprocessing", preprocessor),
        ("clustering", AgglomerativeClustering(n_clusters=cluster_num, linkage="ward"))
    ]
)
    data_fit = data.copy() # why do i need to copy/clone?

    data_fit["cluster_name"] = pipeline.fit_predict(data_fit)
    
    return data_fit

def make_dendrogram_data(
    data: pd.DataFrame,
    num_cols: list,
    cat_cols: list,
):
    """
    Creates feature array data for dendrogram.
    returns linkage
    """
    
    preprocessor = ColumnTransformer(
    [
        ("numerical", StandardScaler(), num_cols),
        ("categorical", OneHotEncoder(drop="first", sparse_output=False), cat_cols)
    ]
)
    feature_array = preprocessor.fit_transform(data)
    feature_names = preprocessor.get_feature_names_out()
    
    feature_df = pd.DataFrame(feature_array, columns = feature_names)
    
    z = linkage(feature_df, method="ward")
    
    return z