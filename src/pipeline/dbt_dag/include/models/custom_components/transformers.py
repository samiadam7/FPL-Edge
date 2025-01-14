import pandas as pd
import numpy as np
from scipy import stats
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.preprocessing import FunctionTransformer, PowerTransformer
from sklearn.ensemble import IsolationForest

class ColumnDropper(BaseEstimator, TransformerMixin):
    def __init__(self, columns_to_drop):
        self.columns_to_drop = columns_to_drop
        
    def fit(self, X, y= None):
        return self
    
    def transform(self, X):
        X = X.copy()
        return X.drop(columns= self.columns_to_drop, errors= "ignore")
    
    def get_feature_names_out(self, names= None):
        return
            
class SkewNormalizationTransformer(BaseEstimator, TransformerMixin):
    def __init__(self, skew_threshold=1.0, method='auto', verbose=False):
        self.skew_threshold = skew_threshold
        self.method = method
        self.verbose = verbose
        self.transformations_ = {}
    
    def _determine_transformation(self, series):
        skew = stats.skew(series.dropna())
        
        if self.method == 'log':
            return 'log'
        elif self.method == 'exp':
            return 'exp'
        elif self.method == 'yeo-johnson':
            return 'yeo-johnson'
        
        if abs(skew) > self.skew_threshold:
            if skew > 0 and (series > 0).all():
                return 'log'
            elif skew > 0:
                return 'yeo-johnson'
            else:
                return 'exp'
        return None
    
    def _create_transformer(self, transformation):
        if transformation == 'log':
            return FunctionTransformer(np.log1p)
        elif transformation == 'exp':
            return FunctionTransformer(np.expm1)
        elif transformation == 'yeo-johnson':
            return PowerTransformer(method='yeo-johnson')
        return None
    
    def fit(self, X, y=None):
        X = pd.DataFrame(X)
        
        self.transformations_ = {}
        for col in X.columns:
            if not np.issubdtype(X[col].dtype, np.number):
                continue
            
            transformation = self._determine_transformation(X[col])
            
            # if transformation:
            transformer = self._create_transformer(transformation)
            
            if self.verbose:
                print(f"Column {col}: Skew = {stats.skew(X[col].dropna()):.2f}, "
                        f"Transformation = {transformation}")
            
            self.transformations_[col] = transformer
        
        return self
    
    def transform(self, X):
        X = pd.DataFrame(X).copy()
        
        for col, transformer in self.transformations_.items():
            if transformer is not None:
                X[col] = transformer.fit_transform(X[[col]])
        
        return X
    
    def get_feature_names_out(self, input_features=None):
        return list(self.transformations_.keys())

class IsolationForestTransformer(BaseEstimator, TransformerMixin):
    def __init__(self, random_state=42):
        self.random_state = random_state
        self.outlier_detector = IsolationForest(random_state=self.random_state)
    
    def fit(self, X, y=None):
        self.outlier_detector.fit(X)
        return self
    
    def transform(self, X):
        outlier_pred = self.outlier_detector.predict(X)
        return X[outlier_pred == 1].reset_index(drop=True)
