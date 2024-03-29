import json
import pathlib
import airflow
import requests
import os
import subprocess
import time
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from swiftclient.service import SwiftService
from swiftclient.service import SwiftUploadObject



dag = DAG (
    dag_id="download_rocket_launches",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval=None,
)

create_swift_object_storage = BashOperator(
    task_id = "create_swift_object_storage",
    bash_command="source /app/openrc/openrc.sh; swift post swift_tmp_dag;swift post rocket_pictures",
    dag=dag,
)
    

download_launches = BashOperator(
    task_id = "download_launches",
    bash_command="source /app/openrc/openrc.sh; curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'; swift upload swift_tmp_dag /tmp/launches.json --object-name launches.json",
    dag=dag,
)

def shell_source(script):
    """Sometime you want to emulate the action of "source" in bash,
    settings some environment variables. Here is a way to do it."""
    pipe = subprocess.Popen(". %s; env" % script, stdout=subprocess.PIPE, shell=True, encoding='utf8')
    output = pipe.communicate()[0]
    env = dict((line.split("=", 1) for line in output.splitlines()))
    os.environ.update(env)

def _get_pictures(): 
    
    # Ensure directory exists
    pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)
     
    ### Download json from swift container
    time.sleep(300)
    # Set OpenStack connection variables
    shell_source("/app/openrc/openrc.sh")
    print(f"OS_TENANT_NAME {os.getenv('OS_TENANT_NAME')}")
    # retrieve from Swift container
    options = {
      "auth_version" : os.getenv('OS_IDENTITY_API_VERSION'),
      "os_username" : os.getenv('OS_USERNAME'),
      "os_password" : os.getenv('OS_PASSWORD'),
      "os_tenant_name" : os.getenv('OS_TENANT_NAME'),
      "os_auth_url" :  os.getenv('OS_AUTH_URL'),
      "os_region_name" : os.getenv('OS_REGION_NAME'),
    }
    with SwiftService(options=options) as swift:
        for down_res in swift.download(container='swift_tmp_dag', objects=['launches.json'], options={"out_directory" : "/tmp"}):
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
                with SwiftService(options=options) as swift:
                    for up_res in swift.upload("rocket_pictures", [SwiftUploadObject(target_file, object_name=image_filename)]):
                        if up_res['success']:
                            print("'%s' uploaded" % target_file)
                        else:
                            print("'%s' upload failed" % target_file) 
                
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

cleaning = BashOperator(
    task_id = "cleaning",
    bash_command="source /app/openrc/openrc.sh; swift delete swift_tmp_dag",
    dag=dag,
)

create_swift_object_storage >> download_launches >> get_pictures >> notify >> cleaning
