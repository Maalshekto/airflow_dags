import airflow
import time
from airflow.operators.python import PythonOperator
from airflow import DAG


from qiskit import IBMQ, assemble, transpile
from qiskit.circuit.random import random_circuit

IBMQ.save_account('76416dc2d7a314e56fb9fafd05a24607c8060643a7a3265055655f27e48811d5692d4567c6a2fa82ce69490b237465164c4a9653a13594895eff039f27c6780d')
provider = IBMQ.load_account()
qx = random_circuit(5, 4, measure=True)

def _real_quantum_backend(): 
  backend = provider.backend.ibmq_lima
  transpiled = transpile(qx, backend=backend)
  job = backend.run(transpiled)
  retrieved_job.wait_for_final_state()
  retrieved_job.result()
  
  
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
    python_callable=_print_result, 
    dag=dag,
)


[Q1, Q2, Q3] >> res
