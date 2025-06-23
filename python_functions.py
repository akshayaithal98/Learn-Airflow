from airflow.models import Variable
import random

def generate_number(**context):
    r = random.randint(1, 10)
    context["ti"].xcom_push(key="number",value=r)
    print(f"Generated number: {r}")

def branch_function(**context):
    n = context["ti"].xcom_pull(key="number")
    
    if n is None:  
        raise ValueError("XCom value 'number' is missing or None!")
    
    return "bash_printing_larger" if n > 5 else "bash_printing_lower"

    
def get_variables(**context):
    bq_dataset=Variable.get("bq_dataset",None)
    print(f"dataset name :{bq_dataset}")
    prev_start_date_success=context.get("prev_start_date_success")
    print("prevous sucess date of dag: ",prev_start_date_success)
    task_instance=context.get("ti")
    task_id=task_instance.task_id
    print("task id of current task is: ",task_id)

def print_params(**context):
    name = context["params"].get("name", "default_value1")
    age = context["params"].get("age", "default_value2")  
    print(f"dag-Level Params: {name},{age}")

def print_triggered_configs(**context):
    triggered_config = context["dag_run"].conf if context["dag_run"] else {}
    
    print("Triggered Configurations:", triggered_config)




    



