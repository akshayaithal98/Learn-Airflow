 
## Work flow of the dag: 
                                start_task
                                    │
                                    ▼
                                task_1 
                                    │
                                    ▼
                            task_2 (Branching)
                                 /       \
                                /         \
                                ▼          ▼
                            task_3       task_4
                            ( >5 )       ( ≤5 )
                                │          │
                                ▼          ▼
                            task_5       task_6  
                                \         /
                                 ▼       ▼
                                  task_7 
                                    │
                                    ▼
                                ┌─────────┐
                                │         │
                                ▼         ▼
                            task_8     task_9 
                                \         /
                                ▼       ▼
                                task_10 
                                    │
                                    ▼
                                task_11
                                    │
                                    ▼
                                task_12
                                    │
                                    ▼
                                end_task   


## Steps of implementation:

1. Since we are doing in GCP, composer can be used which is managed service for airflow. Create composer environment by giving workload configurations(machine infos) to scheduler,dag processor,triggerer, web server. 
2. Since we are also having some pyspark tasks, we will run them on dataproc which is a managed spark service in gcp. So create a dataproc cluster with worker nodes and master node with required machine configurations.
3. Once we create a composer environment, it will give us a airflow web ui to see and manage all dags
4. It will also create a gcs bucker to store all files. Our bucket is : us-central1-airflows-bucket
5. in this bucket in dags folder upload the dag file, in data folder upload the python functions file.once u do this, our dag will appear in webui.
Inorder for the dag to get necessary python functions, we need to define sys path where to look for these files.sys.path.insert(0, "/home/airflow/gcs/data")
so the dag file will first(0) check this path for python_functions file before anything else.
6. for pyspark task, upload pyspark script in anywhere in gcs , but i chose to upload in the gcs bucket created by dataproc itself. dataproc-staging-us-central1
7. Do these in cloud shell : nano control_flow.py , nano python_functions.py and nano pyspark_script.py .Copy all codes in these files and save and exit.
8. Then push these files into required locations. for example dag file into us-central1-airflows-bucket/dags gcs path.
Command is: gsutil cp control_flow.py gs://us-central1-airflows-bucket/dags
Do similar for other files. 
9. In order to update content of the file: either nano into file and edit and copy file again. or delete the file using "rm -f file_name" command and do process again.
10. Once you have dag in webui, you can trigger there, or if it scheduled it will run in scheduled time, or trigger using command line or trigger using api using requests.post(airflow_url)

## Dag Tasks Explaination:
start_task : it is a dummy task indicating the start of execution of tasks in dag

task_1 : in this we generate a random number, push it to the xcom with key as number

task_2: in this we first pull the number from xcom, if it greater than 5, then we trigger the task_3 else task_4 will be triggered. this is a branch operator like if else condition, so the only one task will be executed and other is skipped.

task_3 and task_4 :these are just a printing using bash command,execution depends on the branching.

task_5 : this will be executed only if number is greater thn 5 else it will be skipped.since this is also comes under branching.this task runs a pyspark job using the bash command for submit job in dataproc

task_6: this also falls under branching, hence only executed is <5 else skipped. this runs a pyspark script in dataproc but using dataproc operator , not using bash command like previous one.

task_7:this task should execute regardless of the braching, i.e which ever path it takes, it converges here. so if we keep trigger rules as default it wont run, so i have changed it to NONE_FAILED_MIN_ONE_SUCCESS, that means no previous task should have failed, and atleast one success have happened in previous tasks. Now this task will fetch bq_dataset which is a variables i had set in airflow.and also it will show how to fetch context variables like prev_start_date_success which refers to previous task runs.

task_8: this task shows how to write a task using task flow api.

task_9: this is a sensor which checks for a demo.txt file in bucket path for 10min for every 30sec.if not found, it will fail, so next task wont be executed by default. 

task_10: this is a task using operator of bigquery. this will take a file in gca and puts data in bigquery table, if table is not there it will create. task_9 ensures the file is present in gcs .

task_11: this task shows how to use dag level params. in our dag we have set params as {"name":"akshay","age":25}, we can use these in anyof the tasks, while running we can also change these values. if we set these params in task level,then they are called task level params, which is only available to that particular task. 

task_12: this task shows how to fetch triggered config values. while running dag manually , we can send some values for the dag.
this is kind of similar to params, but this will be useful only for manual running, but params will always have value in scheduled dag runs . 
that means imagine most time name should have "akshay" as value, then just give in params instead of defining everytime using trigger configs.if we want to chaneg the value of params sometime we can do it while triggering . but triggered config is useful when u want to pass different values each time

end_task: this is a dummy task which indicates the execution of all upstream tasks.


