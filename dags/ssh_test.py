from datetime import timedelta, datetime
from my_modules import print
from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.python_operator import PythonOperator

from os import listdir
from os.path import isfile, join

script_dir = "dags/my_scripts"

# for local debugging
# script_dir = "my_scripts"

default_args = {
    'owner': 'Markus Gubler',
    'depends_on_past': False,
    'email': ['markus.gubler@mediapulse.ch'],
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime.now() - timedelta(minutes=20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(dag_id='ssh_operator_example',
          default_args=default_args,
          schedule_interval='0,10,20,30,40,50 * * * *',
          dagrun_timeout=timedelta(seconds=120))

# Any command you want to put here in the dag
t1_bash_v2 = """
cd ./Documents/ssh_test/
sh write_log.sh
"""

# Loading the bash commands from the scripts folder
# The bash commands can be called with the respective file names
allscripts = [f for f in listdir(script_dir) if isfile(join(script_dir, f))]
script_dict = {}
for s in allscripts:
    with open(join(script_dir, s), "r") as f:
        script_dict[s] = "\n" + "".join(f.readlines()) + "\n"

t2 = SSHOperator(
    ssh_conn_id='kusigubi_pi',
    task_id='default_ssh_operator',
    command=script_dict["bash_command_1"],
    dag=dag)

t1 = PythonOperator(
    task_id='print_python1',
    depends_on_past=False,
    python_callable=print.print_context,
    op_kwargs={'ds': script_dict["bash_command_1"]},
    dag=dag
)

t3 = PythonOperator(
    task_id='print_python2',
    depends_on_past=False,
    python_callable=print.print_context,
    op_kwargs={'ds': "done bashing on remote machine"},
    dag=dag
)

t1 >> t2 >> t3
