import numpy as np
import pandas as pd
import joblib
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from typing import Tuple, Dict, Optional, List
from snowflake.connector.pandas_tools import write_pandas

from include.models.custom_components.transformers import ColumnDropper, SkewNormalizationTransformer, IsolationForestTransformer
from include.models.custom_components.metrics import output1_rmse, output1_mae, output2_rmse, output2_mae, combined_metric
from include.models.custom_components.utils import CustomTimeSeriesCV, fix_train_test_split

def test_snowflake_connection(conn_id="snowflake_connection"):
    try:
        hook = SnowflakeHook(snowflake_conn_id=conn_id)
        
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute("SELECT CURRENT_USER(), CURRENT_DATABASE(), CURRENT_SCHEMA();")
        result = cursor.fetchall()
        
        print("Connection Test Successful!")
        print("Result:", result)
        
        return True
    except Exception as e:
        print("Connection Test Failed!")
        print("Error:", str(e))
        return False
    finally:
        conn.close()

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
        
def load_data(all_fixtures= False) -> pd.DataFrame:
    
    if all_fixtures:
        query = """
        SELECT *
        FROM fct_modeling_player
        """
    
    if not all_fixtures:
        query = """
        SELECT *
        FROM fct_predicting_player
        """
    
    og_df = fetch_training_data_from_airflow(query)
    og_df.columns = [col.lower() for col in og_df.columns]
    
    if all_fixtures:
        og_df = og_df.iloc[:, 2:]
    
    print("Dataset Head:")
    print(og_df.head())
    
    if len(og_df) == 0:
        raise ValueError("Loaded dataframe is empty.")
    return og_df.copy()

def save_predictions_to_snowflake(predictions, conn_id="snowflake_connection", table_name='fct_new_gw_predictions'):
    hook = SnowflakeHook(snowflake_conn_id=conn_id)
    conn = hook.get_conn()
    table_name = table_name.upper()
    try:
        cursor = conn.cursor()
        
        print("\nChecking connection...")
        
        cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
        print("\nTable accessible. Row count:", cursor.fetchone()[0])
        
        print("\nUploading to Snowflake...")
        
        predictions.columns = [col.upper() for col in predictions.columns]
        success, n_chunks, num_rows, _ = write_pandas(conn, predictions, table_name)
        
        if success:
            print(f"Successfully inserted {num_rows} rows into {table_name}")
        else:
            raise RuntimeError(f"Failed to insert rows into {table_name}: {_}")
    finally:
        conn.close()

def load_model(model_filename):
    try:
        model = joblib.load(model_filename)
        return model
    except FileNotFoundError as e:
        raise FileNotFoundError(f"Model not found: {model_filename}") from e

def predict_values(model_filename="/usr/local/airflow/include/models/fpl_player_performance_model.pkl", return_preds= False):
    model = load_model(model_filename)
    
    old_fix_df = load_data(all_fixtures= True)
    old_preds = model.predict(old_fix_df)
    old_predictions_df = pd.DataFrame({
        "player_id": old_fix_df["player_id"],
        "fixture_id": old_fix_df["fix_id"],
        "predicted_goals": old_preds[:,0],
        "predicted_assists": old_preds[:,1]
    })
    
    print("Previous GW Predictions Head:")
    print(old_predictions_df)
    
    
    
    old_table = 'fct_prev_gw_predictions'
    save_predictions_to_snowflake(old_predictions_df, table_name= old_table)
    
    new_fix_df = load_data(all_fixtures= True)
    new_preds = model.predict(new_fix_df)
    new_predictions_df = pd.DataFrame({
        "player_id": new_fix_df["player_id"],
        "fixture_id": new_fix_df["fix_id"],
        "predicted_goals": new_preds[:,0],
        "predicted_assists": new_preds[:,1]
    })
    
    print("New GW Predictions Head:")
    print(new_predictions_df.head())
    
    new_table = 'fct_new_gw_predictions'
    save_predictions_to_snowflake(new_predictions_df, table_name= new_table)
    
    if return_preds:
        return old_predictions_df, new_predictions_df
    
if __name__ == "__main__":
    predict_values()
    # test_snowflake_connection()