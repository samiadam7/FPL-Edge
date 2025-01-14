import pandas as pd
import numpy as np
from sklearn.metrics import root_mean_squared_error, mean_absolute_error

def output1_rmse(y_true, y_pred):
    y_true = y_true[:, 0] if isinstance(y_true, np.ndarray) else y_true.iloc[:, 0].to_numpy()
    y_pred = y_pred[:, 0] if isinstance(y_pred, np.ndarray) else y_pred.iloc[:, 0].to_numpy()
    return root_mean_squared_error(y_true, y_pred)

def output2_rmse(y_true, y_pred):
    y_true = y_true[:, 1] if isinstance(y_true, np.ndarray) else y_true.iloc[:, 1].to_numpy()
    y_pred = y_pred[:, 1] if isinstance(y_pred, np.ndarray) else y_pred.iloc[:, 1].to_numpy()
    return root_mean_squared_error(y_true, y_pred)

def output1_mae(y_true, y_pred):
    y_true = y_true[:, 0] if isinstance(y_true, np.ndarray) else y_true.iloc[:, 0].to_numpy()
    y_pred = y_pred[:, 0] if isinstance(y_pred, np.ndarray) else y_pred.iloc[:, 0].to_numpy()
    return mean_absolute_error(y_true, y_pred)

def output2_mae(y_true, y_pred):
    y_true = y_true[:, 1] if isinstance(y_true, np.ndarray) else y_true.iloc[:, 1].to_numpy()
    y_pred = y_pred[:, 1] if isinstance(y_pred, np.ndarray) else y_pred.iloc[:, 1].to_numpy()
    return mean_absolute_error(y_true, y_pred)

def combined_metric(y_true, y_pred, goal_weight=0.7, metric_weight=0.5):
    rmse_goals = output1_rmse(y_true, y_pred)
    rmse_assists = output2_rmse(y_true, y_pred)
    mae_goals = output1_mae(y_true, y_pred)
    mae_assists = output2_mae(y_true, y_pred)
    
    rmse_goals_norm = rmse_goals / (rmse_goals + rmse_assists)
    rmse_assists_norm = rmse_assists / (rmse_goals + rmse_assists)
    mae_goals_norm = mae_goals / (mae_goals + mae_assists)
    mae_assists_norm = mae_assists / (mae_goals + mae_assists)
    
    rmse_combined = goal_weight * rmse_goals_norm + (1 - goal_weight) * rmse_assists_norm
    mae_combined = goal_weight * mae_goals_norm + (1 - goal_weight) * mae_assists_norm
    
    combined = metric_weight * rmse_combined + (1 - metric_weight) * mae_combined
    return combined