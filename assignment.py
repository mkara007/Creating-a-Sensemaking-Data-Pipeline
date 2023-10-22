# The DAG object (needed to instantiate a DAG)
from airflow import DAG
from datetime import timedelta

# Operators (needed to operate)
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

# Task Functions (used to facilitate tasks)
import urllib.request
import time
import glob, os
import json

# Scrape web pages
from bs4 import BeautifulSoup

# Pull course catalog pages
def catalog():
    """
    iterates over a list of URLs, fetches the content of each URL, 
    and saves that content locally with a filename derived from the URL.
    """
    def pull(url):
        response = urllib.request.urlopen(url).read()
        data = response.decode('utf-8')
        return data

    def store(data, file):
        f = open(file, 'w+') # Opens the specified file in 'write' mode ('w+').
        f.write(data)
        f.close()
        print('wrote file: ' + file)

    urls = [
        'http://student.mit.edu/catalog/m1a.html',
        'http://student.mit.edu/catalog/m1b.html',
        'http://student.mit.edu/catalog/m1c.html',
        'http://student.mit.edu/catalog/m2a.html',
        'http://student.mit.edu/catalog/m2b.html',
        'http://student.mit.edu/catalog/m2c.html',
        'http://student.mit.edu/catalog/m3a.html',
        'http://student.mit.edu/catalog/m3b.html',
        'http://student.mit.edu/catalog/m4a.html',
        'http://student.mit.edu/catalog/m4b.html',
        'http://student.mit.edu/catalog/m4c.html',
        'http://student.mit.edu/catalog/m4d.html',
        'http://student.mit.edu/catalog/m4e.html',
        'http://student.mit.edu/catalog/m4f.html',
        'http://student.mit.edu/catalog/m4g.html',
        'http://student.mit.edu/catalog/m5a.html',
        'http://student.mit.edu/catalog/m5b.html',
        'http://student.mit.edu/catalog/m6a.html',
        'http://student.mit.edu/catalog/m6b.html',
        'http://student.mit.edu/catalog/m6c.html',
        'http://student.mit.edu/catalog/m7a.html',
        'http://student.mit.edu/catalog/m8a.html',
        'http://student.mit.edu/catalog/m8b.html',
        'http://student.mit.edu/catalog/m9a.html',
        'http://student.mit.edu/catalog/m9b.html',
        'http://student.mit.edu/catalog/m10a.html',
        'http://student.mit.edu/catalog/m10b.html',
        'http://student.mit.edu/catalog/m11a.html',
        'http://student.mit.edu/catalog/m11b.html',
        'http://student.mit.edu/catalog/m11c.html',
        'http://student.mit.edu/catalog/m12a.html',
        'http://student.mit.edu/catalog/m12b.html',
        'http://student.mit.edu/catalog/m12c.html',
        'http://student.mit.edu/catalog/m14a.html',
        'http://student.mit.edu/catalog/m14b.html',
        'http://student.mit.edu/catalog/m15a.html',
        'http://student.mit.edu/catalog/m15b.html',
        'http://student.mit.edu/catalog/m15c.html',
        'http://student.mit.edu/catalog/m16a.html',
        'http://student.mit.edu/catalog/m16b.html',
        'http://student.mit.edu/catalog/m18a.html',
        'http://student.mit.edu/catalog/m18b.html',
        'http://student.mit.edu/catalog/m20a.html',
        'http://student.mit.edu/catalog/m22a.html',
        'http://student.mit.edu/catalog/m22b.html',
        'http://student.mit.edu/catalog/m22c.html'
    ]

    for url in urls:
        index = url.rfind('/') + 1 # rfind('/') method is used to find the last occurrence of the slash (/) character in the URL
        data = pull(url)
        file = url[index:]
        store(data, file)
        print('pulled: ' + file)
        print('--- waiting ---')
        time.sleep(15)

def combine():
    """
    Takes all .html files in the current directory, reads their content, 
    and writes that content into a single file named combo.txt.
    """
    with open('combo.txt', 'w') as outfile:
        for file in glob.glob("*.html"):
            with open(file, 'r') as infile:
                outfile.write(infile.read())

def titles():
    """
    Reads the combined HTML content from combo.txt, extracts text from all h3 tags found in that content, 
    and saves the extracted texts in a JSON file named titles.json.
    """
    def store_json(data, file):
        with open(file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
            print('wrote file: ' + file)

    with open('combo.txt', 'r') as f:
        html = f.read().replace('\n', ' ').replace('\r', '')

    soup = BeautifulSoup(html, "html.parser")
    results = soup.find_all('h3')
    titles_list = [item.text for item in results]
    store_json(titles_list, 'titles.json')

def clean():
    """
    Reads the titles from titles.json, removes any unwanted characters, punctuation, and numbers from them, 
    filters out single-character words, and then saves the cleaned titles in a new JSON file named titles_clean.json.
    """
    def store_json(data, file):
        with open(file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
            print('wrote file: ' + file)

    with open('titles.json', 'r') as f:
        titles = json.load(f)

    cleaned_titles = []
    for title in titles:
        for char in '''!()-[]{};:'"\,<>./?@#$%^&*_~1234567890''':
            title = title.replace(char, '')
        cleaned_titles.append(' '.join([word for word in title.split() if len(word) > 1]))

    store_json(cleaned_titles, 'titles_clean.json')

from collections import Counter

def count_words():
    def store_json(data, file):
        with open(file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
            print('wrote file: ' + file)

    with open('titles_clean.json', 'r') as f:
        titles = json.load(f)

    words = []
    for title in titles:
        words.extend(title.split())

    counts = Counter(words)
    store_json(dict(counts), 'words.json')


# Define a new Airflow DAG
with DAG(
    # Name of the DAG
    "assignment",
    # The start date for the DAG (when it should start running)
    start_date=days_ago(1),
    # How often the DAG should run
    schedule_interval="@daily",
    # If set to False, it means the DAG won't run for any missed schedules
    catchup=False
) as dag:
    
    # Task to install the BeautifulSoup4 library using a bash command
    t0 = BashOperator(
        # Unique identifier for the task
        task_id='task_zero',
        # The bash command to be executed
        bash_command='pip install beautifulsoup4',
        # Number of retries in case of the task fails
        retries=2
    )

    # Task to run the 'catalog' function
    t1 = PythonOperator(
        task_id='task_one',
        # The Python function to be called by this task
        python_callable=catalog,
        # Ensures the task doesn't depend on previous runs
        depends_on_past=False
    )

    # Task to run the 'combine' Python function
    t2 = PythonOperator(
        task_id='task_two',
        python_callable=combine,
        depends_on_past=False
    )

    # Task to runthe 'titles' function
    t3 = PythonOperator(
        task_id='task_three',
        python_callable=titles,
        depends_on_past=False
    )

    # Task to run the 'clean' function
    t4 = PythonOperator(
        task_id='task_four',
        python_callable=clean,
        depends_on_past=False
    )

    # Task to run the 'count_words' function
    t5 = PythonOperator(
        task_id='task_five',
        python_callable=count_words,
        depends_on_past=False
    )

    t0 >> t1 >> t2 >> t3 >> t4 >> t5