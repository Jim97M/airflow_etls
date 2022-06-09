with dag:
    t1= PythonOperator(
        task_id = "run_alert", python_callable = _run_alert, provide_context = True
    )

    