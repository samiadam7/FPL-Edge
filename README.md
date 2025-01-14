# FPL-Edge

## Project Overview

This project is designed to provide insights into Fantasy Premier League (FPL) performance through an automated data pipeline and dashboard. The system collects real-world football statistics and FPL data, storing them in a Snowflake data warehouse for further analysis. The project aims to help FPL managers make informed decisions by predicting player performance and providing exploratory data visualizations.

---

## Business Objective

The goal of this project is to assist FPL managers in optimizing their fantasy football strategies by:

- Identifying high-performing players based on historical and real-time data.
- Providing data-driven insights for team selection and captain choices.
- Automating data collection, processing, and model prediction to reduce manual work.

**Key Benefits:**

- Improve decision-making with real-world performance metrics.
- Save time through automated data updates and predictions.
- Enhance strategic planning with data visualizations.

For more information, please check out this [presentation](FPL_Presentation.pdf).

## Technical Architecture

### System Components:

The system is designed using a modern data engineering stack with the following components:

1. **Data Collection:**
   `./src/pipeline/dbt_dag/include/data/collect_data`

   - **Source:** FBref for in-game statistics and FPL for fantasy data.
   - **Scraping:** BeautifulSoup and Requests for automated data extraction.

2. **Data Storage:**
   `./src/pipeline/dbt_dag/include/data/utils`

   - **Storage:** Snowflake Data Warehouse.
   - **Format:** Raw data and cleaned datasets stored in structured tables.

3. **Data Pipeline:**
   `./src/pipeline/dbt_dag/dags`

   - **Orchestration:** Apache Airflow with Docker.
   - **Workflow:** Separate DAGs for data ingestion and model training.

4. **Data Transformation:**
   `./src/pipeline/dbt_dag/dags/dbt_pipeline/models`

   - **DBT (Data Build Tool):** Used for data cleaning and transformation.

5. **Machine Learning:**
   `./src/pipeline/dbt_dag/include/models`

   - **Model:** Regression model for player points prediction.
   - **Pipeline:** Training DAG to retrain models when performance drops below a threshold.

6. **Dashboard:**
   - **Objective:** Web dashboard under development.
   - **Features:** Player stats visualization, predicted points, historical analysis.

---

## Data Flow

1. **Data Extraction:** BeautifulSoup and Requests scrape FBref and FPL stats.
2. **Data Ingestion:** Raw data uploaded to Snowflake.
3. **Data Transformation:** DBT cleans and transforms the data.
4. **Model Training:** Airflow DAG trains a regression model.
5. **Prediction:** Upcoming fixture predictions are stored in Snowflake.
6. **Dashboard Integration:** The dashboard fetches data from Snowflake for visualization.

---

## Project Progress Checklist

✅ **Connect to Snowflake in the `train_model.py`**
✅ **Implement Training DAG**
✅ **Create Upcoming Gameweek Table in Snowflake**
✅ **Export Predictions to Snowflake**
✅ **Implement Prediction Logic in `prediction_model.py`**
✅ **Integrate Predictions into Main DAG**
✅ **Define a Business Metric for Model Evaluation**
❌ **Create Dashboard for Data-Driven Decision Making**

---

## Key Metrics and Model Evaluation

The project plans to evaluate predictions based on a tangible business metric that reflects decision-making impact in FPL, such as:

- Points improvement from using the model's predictions.
- Return on investment for fantasy managers using the tool.

---

## Technologies Used

- **Data Collection:** Selenium, Python
- **Data Storage:** Snowflake
- **Pipeline Orchestration:** Apache Airflow (Docker)
- **Transformation:** DBT
- **Machine Learning:** Scikit-Learn
- **Dashboard:** Web-based (in development)

---

## Next Steps

- [ ] Complete the web-based dashboard for FPL insights.

---

## How to Run the Project

1. **Clone the Repository:**
   ```bash
   git clone <repo_url>
   ```
2. **Set Up Environment:**
   - Configure Snowflake credentials.
   - Set up a Docker environment for Airflow.
3. **Run the Airflow Pipeline:**
   ```bash
   docker-compose up
   ```
4. **Monitor Data Flow:**
   - Ensure data is ingested into Snowflake.
5. **Run the Model Training:**
   ```bash
   python train_model.py
   ```

---

## Contact

For questions or contributions, please reach out via the repository's issue tracker.

---

**Note:** This project is under active development and subject to changes as features are finalized.
