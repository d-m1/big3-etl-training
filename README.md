# big3-etl-training

This is a learning project (take it with a pinch of salt) that builds an ETL pipeline to analyze big 3 (Roger Federer, Rafa Nadal, Novak Djokovic) tennis data and calculate player metrics, normalizing surface matches and calculating a coefficient based on Matches & Ranking

Just created to learn a bit more about spark, airflow, metabase and pandas

## Components
- **ETL script file**: Uses airflow as orchestration layer to build the ETL pipeline.
- **Jobs:Extract**: Downloads data from Jeff Sackmann's tennis dataset.
- **Jobs:Transform**: Processes data and calculates metrics including a final grade.
- **Jobs:Load**: Loads data into PostgreSQL.

## Setup
1. Run `make install`.
1. Open the browser and go to `http://localhost:8080`. Default credentials: admin/admin
1. Trigger the DAG manually in the Airflow UI.

For metabase: 
1. Open the browser and go to `http://localhost:3000`.
1. Connect to PostgreSQL db:
   - **Host**: `postgres`
   - **Port**: `5432`
   - **Database Name**: `airflow`
   - **User**: `airflow`
   - **Password**: `airflow`


### Data Attribution & License
This project uses the ATP tennis data from [Jeff Sackmann's Tennis Data Repository](https://github.com/JeffSackmann/tennis_atp), licensed under the [Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License](https://creativecommons.org/licenses/by-nc-sa/4.0/).
