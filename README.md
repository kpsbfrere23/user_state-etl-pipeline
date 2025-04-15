# user_state-etl-pipeline
This pipeline extracts user data from the randomuser.me API, transforms it by grouping users based on their state, and loads the result into a MongoDB database.

Features

Daily extraction of 500+ random user records

Aggregates users by state in the transformation step

Loads structured results into a MongoDB collection

Dockerized and orchestrated with Apache Airflow
