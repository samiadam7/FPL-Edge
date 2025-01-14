import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.pyplot import subplots
from scipy import stats
import joblib
from heapq import nsmallest
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
try:
    from include.data.collect_data.runners.run_all import get_current_season
except ImportError:
    print(ImportError)
    
from typing import Tuple, Dict, Optional, List
from sklearn.preprocessing import OneHotEncoder, StandardScaler, FunctionTransformer, PowerTransformer, PolynomialFeatures
from sklearn.impute import SimpleImputer
from sklearn.compose import ColumnTransformer, make_column_selector
from sklearn.pipeline import Pipeline, make_pipeline
from sklearn.ensemble import IsolationForest
from sklearn.cluster import KMeans
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.metrics.pairwise import rbf_kernel
from sklearn.model_selection import GridSearchCV, RandomizedSearchCV, BaseCrossValidator
from scipy.stats import randint, uniform
from sklearn.metrics import mean_squared_error, mean_absolute_error, root_mean_squared_error, make_scorer

from sklearn.multioutput import MultiOutputRegressor
from sklearn.ensemble import RandomForestRegressor, VotingRegressor, StackingRegressor, BaggingRegressor
from sklearn.linear_model import LinearRegression, Ridge, Lasso, ElasticNet
from sklearn.tree import DecisionTreeRegressor
from sklearn.svm import SVR
from xgboost import XGBRegressor

from include.models.custom_components.transformers import ColumnDropper, SkewNormalizationTransformer, IsolationForestTransformer
from include.models.custom_components.metrics import output1_rmse, output1_mae, output2_rmse, output2_mae, combined_metric
from include.models.custom_components.utils import CustomTimeSeriesCV, fix_train_test_split

# Preprocessing

def fetch_training_data_from_airflow(query, conn_id="snowflake_connection"):
    hook = SnowflakeHook(snowflake_conn_id=conn_id)
    conn = hook.get_conn()
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        data = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        return pd.DataFrame(data, columns=column_names)
    finally:
        conn.close()

def load_data(monitor=False) -> Tuple[pd.DataFrame, pd.DataFrame]:
    
    query = """
    SELECT *
    FROM fct_modeling_player
    WHERE rolling_ub_minutes >= 90
    """
    
    og_df = fetch_training_data_from_airflow(query)
    og_df.columns = [col.lower() for col in og_df.columns]
    og_df.sort_values("fix_id", inplace= True)
    og_df.reset_index(drop= True, inplace= True)
    
    if monitor:
        season = get_current_season()
        max_gw = og_df[og_df["season"] == season]["game_week"].max()
        
        og_df = og_df[(og_df["season"] == season) & (og_df["game_week"] == max_gw)]
    
    df = og_df.drop(columns= ["label_goals", "label_assists"]).copy()
    labels = og_df[["label_goals", "label_assists"]].copy()
    
    return df, labels


def get_drop_nulls_cols(df:pd.DataFrame, threshold:float) -> List:
    num_df = df.select_dtypes(include= np.number)
    nulls = num_df.isna().sum() / num_df.shape[0]
    return nulls[nulls > threshold].index.tolist()

def get_preprocessor(df: pd.DataFrame, drop_categoricals: bool = True):
    cols_to_drop = ['player_id', 'game_week', 'team_id']
    
    if drop_categoricals:
        cat_df = df.select_dtypes(include="object")
        cols_to_drop.extend(list(cat_df.columns))
    
    num_transformer = Pipeline([
        ("imputer", SimpleImputer(strategy="constant", fill_value=0)),
        ("skew", SkewNormalizationTransformer()),
        ("scaler", StandardScaler())
    ])
    
    cat_transformer = Pipeline([
        ("imputer", SimpleImputer(strategy="most_frequent")),
        ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=False))
    ])
    
    column_transformer = ColumnTransformer([
        ("num", num_transformer, [col for col in df.select_dtypes(include=np.number) if col != 'fix_id' and col not in cols_to_drop]),
        ("cat", cat_transformer, make_column_selector(dtype_include="object"))
    ], remainder="passthrough")
    
    preprocessor = Pipeline([
        ("dropper", ColumnDropper(cols_to_drop)),
        ("column_transform", column_transformer),
    ])
    
    return preprocessor

def preprocess_data(df: pd.DataFrame, labels: pd.DataFrame, preprocessor=None, drop_categoricals: bool = True) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if preprocessor is None:
        preprocessor = get_preprocessor(df, drop_categoricals=drop_categoricals)
    
    X_preprocessed = pd.DataFrame(preprocessor.fit_transform(df), columns=preprocessor.get_feature_names_out())
    
    outlier_detection = IsolationForest(random_state=42)
    outlier_pred = outlier_detection.fit_predict(X_preprocessed)
    
    X_preprocessed = X_preprocessed[outlier_pred == 1].reset_index(drop=True)
    labels = labels[outlier_pred == 1].reset_index(drop=True)
    
    return X_preprocessed, labels, preprocessor

# Modeling
def get_base_model_grids(variable_model_num:Optional[int]= 10) -> Tuple[Dict, Dict]:
    model_num = variable_model_num
    model_pipelines = {
        "Linear Regression": Pipeline([
            # ("poly", PolynomialFeatures()),
            ("lr", MultiOutputRegressor(LinearRegression(), n_jobs= -1))
        ]),
        
        "Ridge": Pipeline([
            ("poly", PolynomialFeatures()),
            ("ridge", Ridge(random_state= 42))
        ]),
        
        "Lasso": Pipeline([
            ("poly", PolynomialFeatures()),
            ("lasso", Lasso(random_state= 42))
        ]),

        "Elastic Net": Pipeline([
            ("poly", PolynomialFeatures()),
            ("e_net", ElasticNet(random_state= 42))
        ]),
        
        # "SVR": Pipeline([
        #     ("poly", PolynomialFeatures()),
        #     ("SVC", SVR())
        # ]),

        **{f"Decision Tree {i}": Pipeline(
            [("dt", DecisionTreeRegressor(random_state=42, max_features= "sqrt"))]) for i in range(1, model_num + 1)}
    }
    
    param_grids = {
        "Linear Regression": {
            # "poly__degree": randint(1, 2),
        },

        "Ridge": {
            "poly__degree": randint(1, 2),
            "ridge__alpha": uniform(0.01, 10),
        },

        "Lasso": {
            "poly__degree": randint(1, 2),
            "lasso__alpha": uniform(0.01, 1),
        },

        "Elastic Net": {
            "poly__degree": randint(1, 2),
            "e_net__alpha": uniform(0.01, 1),
            "e_net__l1_ratio": uniform(0.01, 1),
        },

        "SVR": {
            "poly__degree": randint(1, 2),
            "SVC__C": uniform(0.1, 10),
            "SVC__epsilon": uniform(0.01, 1),
            "SVC__kernel": ["linear", "poly", "rbf"],
            "SVC__degree": randint(2, 5),
            "SVC__gamma": uniform(0.01, 1),
        },

        **{f"Decision Tree {i}": {
                "dt__max_depth": randint(1, 10 * i),
                "dt__min_samples_split": randint(2 * i, 10 * i),
                "dt__min_samples_leaf": randint(1, i+1),
            }
            for i in range(1, model_num + 1)
        },
    }

    return model_pipelines, param_grids

def get_ensemble_model_grids(variable_model_num:Optional[int]= 5,
                             base_model_grids= None
                             ) -> Tuple[Dict, Dict]:

    ensemble_model_num = variable_model_num
    
    if base_model_grids != None:
        base_estimators = [(name, model.best_estimator_) for name, model in base_model_grids.items()]
    
    ensemble_pipelines = {
        # "VotingClassifier": Pipeline([
        #     ("voting", MultiOutputRegressor(VotingRegressor(estimators=base_estimators, n_jobs=-1)))
        # ]),
        
        # "StackingClassifier": Pipeline([
        #     ("stacking", StackingRegressor(estimators=base_estimators, n_jobs=-1, final_estimator= ElasticNet()))
        # ]),
        
        **{f"Bagging_DecisionTree_{i}": Pipeline([
            ("bc", BaggingRegressor(DecisionTreeRegressor(random_state=42), random_state=42))
            ])
        for i in range(1, ensemble_model_num + 1)},

        **{f"XGBoost_{i}": XGBRegressor(random_state=42)
        for i in range(1, ensemble_model_num + 1)},
        
        **{f"RandomForest_{i}": Pipeline([
            ("rf", RandomForestRegressor(random_state=42, max_features= "sqrt"))
            ]) 
        for i in range(1, ensemble_model_num + 1)}
    }

    growth_rate = (1 - 0.1) / (ensemble_model_num + 1)
    ensemble_param_grids = {
        # "VotingClassifier": {
        #     "voting__estimator__weights": [[1] * len(base_estimators), 
        #                         [2] * len(base_estimators), 
        #                         [i for i in range(1, len(base_estimators) + 1)]],
        # },

        # "StackingClassifier": {
        #     "stacking__final_estimator__alpha": uniform(0.1, 20),
        #     # "stacking__final_estimator__poly__degree": randint(1, 2),
        #     "stacking__final_estimator__l1_ratio": uniform(0.01, 1),
        # },

        **{
            f"Bagging_DecisionTree_{i}": {
                "bc__n_estimators": randint(10, 100 * i),
                "bc__max_samples": uniform(0.1, growth_rate * i),
                "bc__max_features": uniform(0.1, growth_rate * i),
                "bc__bootstrap": [True, False],
                "bc__bootstrap_features": [True, False]
            }
            for i in range(1, ensemble_model_num + 1)
        },

        **{
            f"XGBoost_{i}": {
                "max_depth": randint(3 , 10 * i),
                "learning_rate": uniform(0.1, 0.1 + growth_rate * i),
                "n_estimators": randint(50, 200 * i),
                "subsample": uniform(0.1, 0.1 + growth_rate * i),
                "colsample_bytree": uniform(0.1, 0.1 + growth_rate * i),
            }
            for i in range(1, ensemble_model_num + 1)
        },

        **{
            f"RandomForest_{i}": {
                "rf__n_estimators": randint(100, 300 * i), 
                "rf__max_depth": randint(5 * i, 20 * i),
                "rf__min_samples_split": randint(2 * i, 10 * i),
                "rf__min_samples_leaf": randint(1, 5 * i),
                "rf__bootstrap": [True, False],
            }
            for i in range(1, ensemble_model_num + 1)
        }
    }

    
    return ensemble_pipelines, ensemble_param_grids

def train_models(model_pipelines, param_grids, X_train, y_train, n_iter, folds=10, n_best=3, refit="combined_metric", n_jobs=-1):
    if n_best > len(model_pipelines.keys()):
        raise ValueError(f"top_models must be smaller than number of models ({len(model_pipelines.keys())})")
    
    scorers = {
        'combined_metric': make_scorer(combined_metric, greater_is_better=False),
        'output1_rmse': make_scorer(output1_rmse, greater_is_better=False),
        'output1_mae': make_scorer(output1_mae, greater_is_better=False),
        'output2_rmse': make_scorer(output2_rmse, greater_is_better=False),
        'output2_mae': make_scorer(output2_mae, greater_is_better=False),
    }
    
    unique_fixtures = X_train["remainder__fix_id"].unique().tolist()
    cv = CustomTimeSeriesCV(unique_fixtures, folds=folds)
    grids = {}
    
    model_scores = []
                       
    for model_name, pipeline in model_pipelines.items():
        print(f"Training and tuning {model_name}")
        
        grids[model_name] = RandomizedSearchCV(
            estimator=pipeline,
            param_distributions=param_grids[model_name],
            n_iter=n_iter,
            cv=cv,
            scoring=scorers,
            verbose=3,
            n_jobs=n_jobs,
            random_state=42,
            refit=refit,
        )
        
        grids[model_name].fit(X_train, y_train)
        
        best_score = np.abs(grids[model_name].best_score_)
        model_scores.append((model_name, best_score, grids[model_name].best_estimator_))
        
        if isinstance(scorers, dict) or isinstance(scorers, list):
            print(f"Best parameters for {model_name} based on '{refit}': {grids[model_name].best_params_}")
            print(f"Best Score for {model_name} based on '{refit}': {grids[model_name].best_score_}\n")
        else:
            print(f"Best parameters for {model_name}: {grids[model_name].best_params_}")
            print(f"Best Score for {model_name}: {grids[model_name].best_score_}\n")
    
    top_models = nsmallest(n_best, model_scores, key=lambda x: x[1])
    
    print("The top 3 models are:")
    for rank, (model_name, score, _) in enumerate(top_models, start=1):
        print(f"{rank}. Model: {model_name}, Absolute Score: {score}")
        
    print("\n")
    return grids, top_models

def combine_grid_results(grids, sort_column=None) -> pd.DataFrame:
    dfs = []
    for name, grid in grids.items():
        df = pd.DataFrame(grid.cv_results_)
        df["model"] = name
        dfs.append(df)
    
    if sort_column != None:
        combined_df = pd.concat(dfs).sort_values(sort_column, ascending=False).reset_index(drop=True)
    else:
        combined_df = pd.concat(dfs).reset_index(drop= True)
    return combined_df

def select_best_model(list_of_models: List[Tuple[str, float, BaseEstimator]], X_test, y_test, scoring=combined_metric, greater_is_better=False):
    best_model = None
    best_model_name = None
    best_score = float('-inf') if greater_is_better else float('inf')
    
    for model_name, _, model in list_of_models:
        predictions = model.predict(X_test)
        score = scoring(y_test, predictions)
        
        if (greater_is_better and score > best_score) or (not greater_is_better and score < best_score):
            best_score = score
            best_model = model
            best_model_name = model_name
    
    print(f"Best model: {best_model_name} \n Test Score: {round(best_score, 4)} \n")
    return best_model
    
def train_best_model(X:pd.DataFrame, y:pd.DataFrame, model_iter:Optional[int]=10, variable_models:Optional[int]= 5, cv:Optional[int]=5):
    X_train, X_test, y_train, y_test = fix_train_test_split(X, y)
    
    model_pipelines, param_grids = get_base_model_grids(variable_models)
    base_model_grids, base_best_models = train_models(model_pipelines, param_grids, X_train, y_train, model_iter, cv)
    base_results = combine_grid_results(base_model_grids)
    
    ensemble_pipelines, ensemble_param_grids = get_ensemble_model_grids(variable_models)
    ensemble_model_grids, ensemble_best_models = train_models(ensemble_pipelines, ensemble_param_grids, X_train, y_train, model_iter, cv)
    ensemble_results = combine_grid_results(ensemble_model_grids)
    
    all_results = pd.concat([base_results, ensemble_results]).sort_values("mean_test_combined_metric")
    all_results["overall_ranking"] = range(1, len(all_results) + 1)
    all_results.to_csv("modeling_results.csv", index= False)
    
    models = base_best_models + ensemble_best_models
    return select_best_model(models, X_test, y_test)
    

def retrain_model(model_filename, test=False):
    X, y = load_data()
    X_preprocessed, labels, preprocessor = preprocess_data(X, y)
    
    if test:
        model = train_best_model(X_preprocessed, labels, 1, 1, 2)
        
    else:
        model = train_best_model(X_preprocessed, labels)

    final_model = Pipeline([
    ("preprocessor", preprocessor),
    ("model", model)
    ])

    final_model.fit(X, y)
    joblib.dump(final_model, model_filename)
    
    print(f"Final Model: {final_model.steps}")

def monitor_model_performance(model_filename, threshold:float=0.55):
    model = joblib.load(model_filename)
    
    X, y = load_data(monitor= True)
        
    y_preds = model.predict(X)
    score = combined_metric(y, y_preds)
    print(f"Threshold: {threshold}")
    print(f"Model Score: {round(score, 5)}")
    if score > threshold:
        print("Model needs to retrain...")
        return True
    print("Model doesn't need retraining!")
    return False
  
def test_xgb_compatibility():
    X, y = load_data()
    X_preprocessed, labels = preprocess_data(X, y)
    
    model = XGBRegressor()
    params = {"max_depth": randint(3 , 10),
                "learning_rate": uniform(0.1, 0.9),
                "n_estimators": randint(50, 200),
                "subsample": uniform(0.1, 0.9),
                "colsample_bytree": uniform(0.1, 0.9),}
    
    rscv = RandomizedSearchCV(model, params, verbose= 3)
    rscv.fit(X_preprocessed, labels)
  
def main():
    df, labels = load_data(monitor=True)
    print(df.shape)
    
if __name__ == "__main__":
    retrain_model("fpl_player_performance_model.pkl", test= True)
    # test_xgb_compatibility()
    # monitor_model_performance("fpl_player_performance_model.pkl", 0.5)
    # main()