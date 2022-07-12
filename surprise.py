import airflow
import time
from airflow.operators.python import PythonOperator
from airflow import DAG


from qiskit import IBMQ, assemble, transpile, execute, Aer
from qiskit.circuit.random import random_circuit
from qiskit.providers.aer.noise import NoiseModel

IBMQ.save_account('76416dc2d7a314e56fb9fafd05a24607c8060643a7a3265055655f27e48811d5692d4567c6a2fa82ce69490b237465164c4a9653a13594895eff039f27c6780d')
provider = IBMQ.load_account()
qx = random_circuit(5, 4, measure=True)

#def _real_quantum_backend(): 
#  backend = provider.backend.ibmq_lima
#  transpiled = transpile(qx, backend=backend)
#  job = backend.run(transpiled)
#  retrieved_job = backend.retrieve_job(job.job_id())
#  retrieved_job.wait_for_final_state()
#  retrieved_job.result()
  

def _real_quantum_backend(): 
  time.sleep(60)
 
  
def _simulator_perfect_quantum_backend():
  backend = Aer.get_backend('aer_simulator')
  transpiled = transpile(qx, backend=backend)
  job = backend.run(transpiled)
  result = job.result()
  counts = result.get_counts(qx)
  print(counts)
  
def _simulator_noisy_quantum_backend():
  backend = provider.backend.ibmq_lima
  noise_model = NoiseModel.from_backend(backend)
  # Get coupling map from backend
  coupling_map = backend.configuration().coupling_map

  # Get basis gates from noise model
  basis_gates = noise_model.basis_gates
  
  result = execute(qx, Aer.get_backend('qasm_simulator'),
                 coupling_map=coupling_map,
                 basis_gates=basis_gates,
                 noise_model=noise_model).result()
  counts = result.get_counts(qx)
  print(counts)

def _print_result():
  time.sleep(2)
 

dag = DAG (
    dag_id="surprise",
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
