import snowflake.connector
import configparser

def load_snowflake_connection():
    parser = configparser.ConfigParser()
    parser.read("/usr/local/airflow/include/data/utils/pipeline.conf")
    # parser.read("./pipeline.conf")

    username = parser.get("snowflake_creds", "username").strip()
    password = parser.get("snowflake_creds", "password").strip()
    account_name = parser.get("snowflake_creds", "account_name").strip()
    
    return username, password, account_name

def upload_new_data():
    username, password, account_name = load_snowflake_connection()
    
    sql_statements = [
        "USE ROLE dbt_role;",
        "USE DATABASE fpl_database;",
        "USE SCHEMA dbt_schema;",

        """-- TEMP Tables
        CREATE OR REPLACE TABLE raw_team_data (
            code INT,
            draw INT,
            form FLOAT,
            id INT,
            loss INT,
            name VARCHAR(100),
            played INT,
            points INT,
            position INT,
            short_name VARCHAR(10),
            strength INT,
            team_division INT,
            unavailable BOOLEAN,
            win INT,
            strength_overall_home INT,
            strength_overall_away INT,
            strength_attack_home INT,
            strength_attack_away INT,
            strength_defence_home INT,
            strength_defence_away INT,
            pulse_id INT,
            season VARCHAR(10),
            fbref_id VARCHAR(20)
        );"""
        ,

        """COPY INTO raw_team_data
        FROM @my_s3_stage
        FILE_FORMAT = (FORMAT_NAME = 'my_csv_format')
        PATTERN = '.*teams\\.csv$';"""
        ,

        """CREATE OR REPLACE TABLE raw_fixtures (
            code INT,
            event INT,
            finished BOOLEAN,
            finished_provisional BOOLEAN,
            id INT,
            kickoff_time TIMESTAMP_LTZ,
            minutes INT,
            provisional_start_time BOOLEAN,
            started BOOLEAN,
            team_a INT,
            team_a_score FLOAT,
            team_h INT,
            team_h_score FLOAT,
            stats STRING,
            team_h_difficulty INT,
            team_a_difficulty INT,
            pulse_id INT,
            season STRING
        );
        """
        ,

        """COPY INTO raw_fixtures
        FROM @my_s3_stage
        FILE_FORMAT = (FORMAT_NAME = 'my_csv_format')
        PATTERN = '.*fixtures\\.csv$';
        """
        ,

        """CREATE OR REPLACE TABLE raw_players_clean (
            first_name VARCHAR(45),
            second_name VARCHAR(45),
            goals_scored INT,
            assists INT,
            total_points INT,
            minutes INT,
            goals_conceded INT,
            creativity FLOAT,
            influence FLOAT,
            threat FLOAT,
            bonus INT,
            bps INT,
            ict_index FLOAT,
            clean_sheets INT,
            red_cards INT,
            yellow_cards INT,
            selected_by_percent FLOAT,
            now_cost INT,
            element_type VARCHAR(10),
            team INT,
            season VARCHAR(10)
        );
        """
        ,

        """COPY INTO raw_players_clean
        FROM @my_s3_stage
        FILE_FORMAT = (FORMAT_NAME = 'my_csv_format')
        PATTERN = '.*players_clean\\.csv$';
        """
        ,

        """CREATE OR REPLACE TABLE raw_fpl_performance(
            name VARCHAR(100),
            position VARCHAR(10),
            team VARCHAR(50),
            xP FLOAT,
            assists INT,
            bonus INT,
            bps INT,
            clean_sheets INT,
            creativity FLOAT,
            element INT,
            expected_assists FLOAT,
            expected_goal_involvements FLOAT,
            expected_goals FLOAT,
            expected_goals_conceded FLOAT,
            fixture INT,
            goals_conceded INT,
            goals_scored INT,
            ict_index FLOAT,
            influence FLOAT,
            kickoff_time TIMESTAMP,
            minutes INT,
            opponent_team VARCHAR(50),
            own_goals INT,
            penalties_missed INT,
            penalties_saved INT,
            red_cards INT,
            round INT,
            saves INT,
            selected INT,
            starts INT,
            team_a_score INT,
            team_h_score INT,
            threat FLOAT,
            total_points INT,
            transfers_balance INT,
            transfers_in INT,
            transfers_out INT,
            value FLOAT,
            was_home BOOLEAN,
            yellow_cards INT,
            GW INT,
            season STRING
        );
        """
        ,

        """COPY INTO raw_fpl_performance
        FROM @my_s3_stage
        FILE_FORMAT = (FORMAT_NAME = 'my_csv_format')
        PATTERN = '.*merged_gw\\.csv$';
        """
        ,

        """CREATE OR REPLACE TABLE raw_real_perf (
            Date DATE,
            Day VARCHAR(10),
            Comp VARCHAR(50),
            Round VARCHAR(50),
            Venue VARCHAR(10),
            Result VARCHAR(10),
            Squad VARCHAR(50),
            Opponent VARCHAR(50),
            Starter INTEGER,
            Pos VARCHAR(50),
            Min INT NULL,
            Performance_Gls INT NULL,
            Performance_Ast INT NULL,
            Performance_PK INT NULL,
            Performance_PKatt INT NULL,
            Performance_Sh INT NULL,
            Performance_SoT INT NULL,
            Performance_CrdY INT NULL,
            Performance_CrdR INT NULL,
            Performance_Touches INT NULL,
            Performance_Tkl INT NULL,
            Performance_Int INT NULL,
            Performance_Blocks INT NULL,
            Expected_xG FLOAT NULL,
            Expected_npxG FLOAT NULL,
            Expected_xAG FLOAT NULL,
            SCA_SCA INT NULL,
            SCA_GCA INT NULL,
            Passes_Cmp INT NULL,
            Passes_Att INT NULL,
            Passes_Cmp_percent FLOAT NULL,
            Passes_PrgP INT NULL,
            Carries_Carries INT NULL,
            Carries_PrgC INT NULL,
            Take_Ons_Att INT NULL,
            Take_Ons_Succ INT NULL,
            Match_Report VARCHAR(255),
            Name VARCHAR(50),
            Performance_SoTA INT NULL,
            Performance_GA INT NULL,
            Performance_Saves INT NULL,
            Performance_Save_percent FLOAT NULL,
            Performance_CS INT NULL,
            Performance_PSxG FLOAT NULL,
            Penalty_Kicks_PKatt INT NULL,
            Penalty_Kicks_PKA INT NULL,
            Penalty_Kicks_PKsv INT NULL,
            Penalty_Kicks_PKm INT NULL,
            Launched_Cmp INT NULL,
            Launched_Att INT NULL,
            Launched_Cmp_percent FLOAT NULL,
            Passes_Att_GK INT NULL,
            Passes_Thr INT NULL,
            Passes_Launch_percent FLOAT NULL,
            Passes_AvgLen FLOAT NULL,
            Goal_Kicks_Att INT NULL,
            Goal_Kicks_Launch_percent FLOAT NULL,
            Goal_Kicks_AvgLen FLOAT NULL,
            Crosses_Opp INT NULL,
            Crosses_Stp INT NULL,
            Crosses_Stp_percent FLOAT NULL,
            Sweeper_OPA INT NULL,
            Sweeper_AvgDist FLOAT NULL,
            Performance_Fls INT NULL,
            Performance_Fld INT NULL,
            Performance_Off INT NULL,
            Performance_Crs INT NULL,
            Performance_TklW INT NULL,
            Performance_OG INT NULL,
            Performance_PKwon INT NULL,
            Performance_PKcon INT NULL,
            Season STRING,
            Captain CHAR(1)
        );
        """
        ,
        
        """
        CREATE OR REPLACE TABLE fct_new_gw_predictions (
            prediction_id INTEGER AUTOINCREMENT PRIMARY KEY,
            player_id INTEGER NOT NULL,
            fixture_id INTEGER NOT NULL,
            predicted_goals FLOAT NOT NULL,
            predicted_assists FLOAT NOT NULL,
            prediction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        ,
        
        """
        CREATE OR REPLACE TABLE fct_prev_gw_predictions (
            prediction_id INTEGER AUTOINCREMENT PRIMARY KEY,
            player_id INTEGER NOT NULL,
            fixture_id INTEGER NOT NULL,
            predicted_goals FLOAT NOT NULL,
            predicted_assists FLOAT NOT NULL,
            prediction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        ,

        """COPY INTO raw_real_perf
        FROM @my_s3_stage
        FILE_FORMAT = (FORMAT_NAME = 'my_csv_format')
        PATTERN = '.*fbref_merged_gw_data\\.csv$';
        """
        ,

        """CREATE OR REPLACE TABLE raw_compiled_ids(
            fpl_f_name VARCHAR(50),
            fpl_l_name VARCHAR(50),
            id_fpl INT,
            fbref_name VARCHAR(100),
            id_fbref VARCHAR(10)
        );
        """
        ,

        """COPY INTO raw_compiled_ids
        FROM @my_s3_stage
        FILE_FORMAT = (FORMAT_NAME = 'my_csv_format')
        PATTERN = '.*player_compiled_ids\\.csv$';
        """
        
    ]

    try:
        snow_conn = snowflake.connector.connect(
        user= username,
        password= password,
        account= account_name)
        
        cursor = snow_conn.cursor()
        
        for sql in sql_statements:
            cursor.execute(sql)
            print(f"Executed: {sql[:50]}...")
            

    finally:
        snow_conn.close()
        print("Snowflake connection closed.")

if __name__ == "__main__":
    username, password, account_name = load_snowflake_connection()
    upload_new_data()