import requests
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import logging
import subprocess
import re



logging.basicConfig(level=logging.INFO)

sources = ['https://www.dawn.com/', 'https://www.bbc.com/']

def extract(**kwargs):
    top_links = []
    for source in sources:
        logging.info(f"Extracting from {source}")
        reqs = requests.get(source)
        if reqs.status_code == 200:
            soup = BeautifulSoup(reqs.text, 'html.parser')
            for link in soup.find_all('a', href=True):
                title = link.text.strip()
                link_url = link['href']
                description = ""
                # Find the description if available
                if link.parent.name == "h3":
                    description = link.parent.find_next('p').text.strip()
                elif link.parent.name == "div":
                    description = link.parent.text.strip()
                top_links.append({'title': title, 'link': link_url, 'description': description, 'date': datetime.now(), 'website': source})
            logging.info(f"Extracted {len(top_links)} links from {source}")
        else:
            logging.error(f"Failed to extract from {source}. Status code: {reqs.status_code}")
    if top_links:
        kwargs['ti'].xcom_push(key='top_links', value=top_links)
    else:
        logging.error("No data extracted. Unable to push to XCom.")

def transform(**kwargs):
    ti = kwargs['ti']
    top_links = ti.xcom_pull(key='top_links', task_ids='extract_links')
    logging.info("Transforming data")
    df = pd.DataFrame(top_links)
    df['title'] = df['title'].apply(lambda x: re.sub(r'[^a-zA-Z0-9\s]', '', x.lower()))
    df['description'] = df['description'].apply(lambda x: re.sub(r'[^a-zA-Z0-9\s]', '', x.lower()))
    ti.xcom_push(key='transformed_data', value=df)
    return df

def load(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(key='transformed_data', task_ids='transform_data')
    logging.info("Loading data")
    df.to_csv('top_links.csv', index=False)
    logging.info(f"Length of DataFrame: {len(df)}")

    logging.info("CSV created")

    # Initialize Git
    if not os.path.exists('.git'):
        try:
            subprocess.run(["git", "init"], capture_output=True, text=True, check=True)
            logging.info("Git initialized")
        except subprocess.CalledProcessError as e:
            logging.error(f"Failed to initialize Git: {e.stderr}")
    else:
        logging.info("Git already initialized")

    # Initialize DVC if not already initialized
    if not os.path.exists('.dvc'):
        try:
            subprocess.run(["dvc", "init"], capture_output=True, text=True, check=True)
            logging.info("DVC initialized")
        except subprocess.CalledProcessError as e:
            logging.error(f"Failed to initialize DVC: {e.stderr}")
    else:
        logging.info("DVC already initialized")

    if not os.path.exists('airflow\dvc\config'):
        logging.info("Adding Google Drive as remote storage...")
        try:
            subprocess.run(["dvc", "remote", "add", "-d", "gdrive", "gdrive://1ZkKLL4GlflIbu9f5DayQwN4tXTKfujay"], capture_output=True, text=True, check=True)
            logging.info("Google Drive added as remote storage")
        except subprocess.CalledProcessError as e:
            logging.error(f"Error adding Google Drive as remote storage: {e}")

    logging.info("Adding CSV file to DVC...")
    try:
        subprocess.run(["dvc", "add", "top_links.csv"], capture_output=True, text=True, check=True)
        logging.info("CSV file added to DVC")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error adding CSV file to DVC: {e}")

    # Push data to Google Drive using DVC
    logging.info("Pushing data to Google Drive using DVC...")
    try:
        subprocess.run(["dvc", "push"], capture_output=True, text=True, check=True)
        logging.info("Data pushed to Google Drive using DVC")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error pushing data to Google Drive using DVC: {e.stderr}")
    
    try:
        subprocess.run(["git", "remote", "add", "origin", "https://github.com/Aeiman191/Web-links-Extraction.git"], capture_output=True, text=True, check=True)
        logging.info("remote addedd")
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to add remote: {e.stderr}")
    
        # Add all files to the git repository
    try:
        subprocess.run(["git", "add", "."], capture_output=True, text=True, check=True)
        logging.info("Added all files to Git")
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to add files to Git: {e.stderr}")
        
        # Commit changes
    try:
        subprocess.run(["git", "commit", "-m", "Added top_links.csv"], capture_output=True, text=True, check=True)
        logging.info("Committed changes to Git")
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to commit changes to Git: {e.stderr}")
        
        # Push changes to Git
    try:
        subprocess.run(["git", "push", "-u", "origin", "main"], capture_output=True, text=True, check=True)
        logging.info("Pushed changes to Git")
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to push changes to Git: {e.stderr}")

default_args = {
    'owner': 'airflow-demo',
    'start_date': datetime(2024, 5, 10),
    'catchup': False,
}

dag = DAG(
    'top-links',
    default_args=default_args,
    description='A simple DAG to extract top links, transform, and push to Google Drive using DVC',
    schedule_interval='@daily',
)

task1 = PythonOperator(
    task_id="extract_links",
    python_callable=extract,
    provide_context=True,
    dag=dag,
)

task2 = PythonOperator(
    task_id="transform_data",
    python_callable=transform,
    provide_context=True,
    dag=dag,
)

task3 = PythonOperator(
    task_id="load_to_drive",
    python_callable=load,
    provide_context=True,
    dag=dag,
)

task1 >> task2 >> task3
