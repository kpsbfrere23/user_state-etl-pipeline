from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
from pymongo import MongoClient


def fetch_users(**context):
    url = "https://randomuser.me/api/?results=50"
    response = requests.get(url)
    data = response.json()
    users = data["results"]

    df = pd.json_normalize(users)
    context["ti"].xcom_push(key="user_data", value=df.to_dict(orient="records"))


def save_users_to_mongo(**context):
    records = context["ti"].xcom_pull(key="user_data", task_ids="fetch_users")
    df = pd.DataFrame(records)

    # Group users by state
    grouped = df.groupby("location.state")

    # Connect to MongoDB
    client = MongoClient("mongodb://mongo:27017/")
    db = client["airflow_db"]
    collection = db["users_by_state"]

    # Clear collection before inserting new data
    collection.delete_many({})

    for state, group in grouped:
        users = group.to_dict(orient="records")
        doc = {
            "state": state,
            "users": users,
            "count": len(users),
            "timestamp": datetime.utcnow(),
        }
        collection.insert_one(doc)

    print("Inserted grouped users into MongoDB.")


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="fetch_and_store_users_mongo",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",  # run daily
    catchup=False,
    tags=["randomuser", "mongodb"],
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_users",
        python_callable=fetch_users,
        provide_context=True,
    )

    save_task = PythonOperator(
        task_id="save_to_mongo",
        python_callable=save_users_to_mongo,
        provide_context=True,
    )

    fetch_task >> save_task

