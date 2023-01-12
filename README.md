Code to copy raw data from CSV and transform load into Postgres data and generate summary views.

### Automate with Airflow
Copy the `expenses_tracker_etl_dag.py` to your airflow dags folder and replace the values of:
```
params={
    "app_path":"<path/to/scripts>", 
    "file_path":"<path/to/data.csv>"
    }
```
