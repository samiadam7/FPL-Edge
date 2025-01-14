import pandas as pd
import numpy as np
from typing import Optional
from sklearn.model_selection import BaseCrossValidator

class CustomTimeSeriesCV(BaseCrossValidator):
    def __init__(self, unique_fixtures, folds=4):
        if not isinstance(unique_fixtures, list):
            raise TypeError("unique_fixtures must be a list")
        
        if len(unique_fixtures) == 0:
            raise ValueError("unique_fixtures cannot be empty")
        
        if folds <= 0:
            raise ValueError("Number of folds must be positive")
        
        try:
            self.fix_per_fold = max(1, len(unique_fixtures) // (folds + 1))
        except ZeroDivisionError:
            raise ValueError("Not enough unique fixtures for the specified number of folds")
        
        self.unique_fixtures = unique_fixtures
        self.folds = folds

    def __repr__(self):
        return (f"CustomTimeSeriesCV(n_fixtures={len(self.unique_fixtures)}, "
                f"folds={self.folds}, "
                f"fixtures_per_fold={self.fix_per_fold})")

    def __str__(self):
        return (f"Time Series Cross-Validator\n"
                f"Total Fixtures: {len(self.unique_fixtures)}\n"
                f"Number of Folds: {self.folds}\n"
                f"Fixtures per Fold: {self.fix_per_fold}")

    def get_n_splits(self, X=None, y=None, groups=None):
        return self.folds

    def split(self, X, y=None, groups=None):
        fixture_id_col = 'remainder__fix_id'
        unique_fixt = self.unique_fixtures

        for fold in range(1, self.folds + 1):
            train_end = fold * self.fix_per_fold
            test_start = train_end
            test_end = test_start + self.fix_per_fold

            if test_end > len(unique_fixt):
                test_end = len(unique_fixt)
            
            train_fixtures = unique_fixt[:train_end]
            test_fixtures = unique_fixt[test_start:test_end]

            train_idx = X[X[fixture_id_col].isin(train_fixtures)].index.values
            test_idx = X[X[fixture_id_col].isin(test_fixtures)].index.values

            yield (train_idx, test_idx)
            
def fix_train_test_split(X:pd.DataFrame, y:pd.DataFrame, train_ratio:Optional[float]= 0.9):
    denom = 1 / (1- train_ratio)
    test_count = len(X["remainder__fix_id"].unique()) // int(denom)
    test_fixtures = (X["remainder__fix_id"].tolist()[-test_count:])

    train_idx = X[~X["remainder__fix_id"].isin(test_fixtures)].index.values
    test_idx = X[X["remainder__fix_id"].isin(test_fixtures)].index.values

    X_train = X.iloc[train_idx]
    y_train = y.iloc[train_idx]

    X_test = X.iloc[test_idx]
    y_test = y.iloc[test_idx]
    
    return X_train, X_test, y_train, y_test