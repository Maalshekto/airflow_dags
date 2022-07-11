import airflow
import time
from airflow.operators.python import PythonOperator
from airflow import DAG


from qiskit import IBMQ, assemble, transpile
from qiskit.circuit.random import random_circuit

def _real_quantum_backend(): 
  time.sleep(100)
  
def _simulator_perfect_quantum_backend():
  time.sleep(10)

def _simulator_noisy_quantum_backend():
  time.sleep(10)

def _print_result():
  time.sleep(2)
 

dag = DAG (
    dag_id="surprise_quantique",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval=None,
)

Q1 = PythonOperator( 
    task_id="real_quantum_backend",
    python_callable=_real_quantum_backend, 
    dag=dag,
)

Q2 = PythonOperator(
    task_id="simulator_perfect_quantum_backend",
    python_callable=_simulator_perfect_quantum_backend, 
    dag=dag,
)

Q3 = PythonOperator(
    task_id="simulator_noisy_quantum_backend",
    python_callable=_simulator_noisy_quantum_backend, 
    dag=dag,
)

res= PythonOperator(
    task_id="print_result",
    python_callable=_print_result(), 
    dag=dag,
)


[Q1, Q2, Q3] >> res
