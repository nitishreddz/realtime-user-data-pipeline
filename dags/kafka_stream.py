"""
Airflow DAG for streaming user data from Random User API to Kafka.

This module defines an Airflow DAG that fetches random user data from an external API,
formats it, and streams it to a Kafka topic for downstream processing.
"""

import json
import logging
import time
from datetime import datetime
from typing import Dict, Any

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 2, 2, 13, 11)
}


def get_data() -> Dict[str, Any]:
    """
    Fetch random user data from the Random User API.
    
    Returns:
        Dict[str, Any]: A dictionary containing user information from the API.
        
    Raises:
        requests.RequestException: If the API request fails.
    """
    res = requests.get('https://randomuser.me/api/')
    res = res.json()
    res = res['results'][0]
    
    return res


def format_data(res: Dict[str, Any]) -> Dict[str, Any]:
    """
    Format raw API response data into a structured format for Kafka.
    
    Args:
        res (Dict[str, Any]): Raw user data from the Random User API.
        
    Returns:
        Dict[str, Any]: Formatted user data with standardized fields.
    """
    data = {}
    location = res['location']
    
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = (
        f"{str(location['street']['number'])} {location['street']['name']}, "
        f"{location['city']}, {location['state']}, {location['country']}"
    )
    data['postcode'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']
    
    return data


def stream_data() -> None:
    """
    Stream user data to Kafka topic continuously for 3 minutes.
    
    This function fetches user data from the API, formats it, and publishes
    it to the 'users_created' Kafka topic. The streaming continues for 180 seconds
    (3 minutes) before stopping.
    
    Raises:
        Exception: Logs any errors that occur during data streaming but continues execution.
    """
    # Initialize Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=['broker:29092'],
        max_block_ms=5000
    )
    
    curr_time = time.time()
    
    # Stream data for 3 minutes (180 seconds)
    while True:
        if time.time() > curr_time + 180:
            break
        
        try:
            # Fetch and format data
            res = get_data()
            res = format_data(res)
            
            logging.info(f"Record sent to producer: {res}")
            
            # Send data to Kafka topic
            producer.send(
                'users_created',
                json.dumps(res).encode('utf-8')
            )
            
        except Exception as e:
            logging.error(f'An error occurred: {e}')
            continue


# Define the DAG
with DAG(
    'user_automation',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='DAG to stream random user data from API to Kafka'
) as dag:
    
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )