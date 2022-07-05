import json
import pathlib
import airflow
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


dag = DAG (
    dag_id="download_rocket_launches",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval=None,
)

create_swift_object_storage = BashOperator(
    task_id = "create_swift_object_storage",
    bash_command="source /app/openrc/openrc.sh; swift post swift_airflow_rocket_dag",
    dag=dag,
)
    

download_launches = BashOperator(
    task_id = "download_launches",
    bash_command="source /app/openrc/openrc.sh; curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'; swift upload swift_airflow_rocket_dag /tmp/launches.json --object-name launches.json",
    dag=dag,
)

def _get_pictures(): 
    
    # Ensure directory exists
    pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)
     
    ### Download json from swift container
    
    # Set OpenStack connectrion variable
    bashCommand = "source /app/openrc/openrc.sh"
    process = subprocess.run(bashCommand, shell=True, executable='/bin/bash')
    
    # retrieve from Swift container
    with SwiftService() as swift:
        for down_res in swift.download(container='swift_airflow_rocket_dag', objects=['tmp/launches.json']):
            if down_res['success']:
                        print("'%s' downloaded" % down_res['object'])
                    else:
                        print("'%s' download failed" % down_res['object'])
    # Download all pictures in launches.json
    with open("/tmp/launches.json") as f:
        launches = json.load(f)
        image_urls = [launch["image"] for launch in launches["results"]]
        for image_url in image_urls:
            try:
                response = requests.get(image_url)
                image_filename = image_url.split("/")[-1]
                target_file = f"/tmp/images/{image_filename}"
                with open(target_file, "wb") as f:
                    f.write(response.content)
                    print(f"Downloaded {image_url} to {target_file}")
            except requests_exceptions.MissingSchema:
                print(f"{image_url} appears to be an invalid URL.")
            except requests_exceptions.ConnectionError:
                print(f"Could not connect to {image_url}.")

get_pictures = PythonOperator( 
    task_id="get_pictures",
    python_callable=_get_pictures, 
    dag=dag,
)

notify = BashOperator(
    task_id="notify",
    bash_command='echo "There are now $(ls /tmp/images/ | wc -l) images."',
    dag=dag,
)

create_swift_object_storage >> download_launches >> get_pictures >> notify
