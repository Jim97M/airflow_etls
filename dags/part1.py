   #Define default arguments for DAG
default_args = {
    "owner": config["owner"],
    "start_date": datetime(2022, 2, 21),
    "retries": 0,
    "retry_delay": timedelta(minutes=5)
}

#Initialize the DAG
dag = DAG(
    dag_id="alert_" + config["nickname"] + "_dag",
    default_args = default_args,
    schedule_interval = config["interval"],
    catchup=False,
)