alert_files = glob.glob("dags/alerts/*.yaml")

for alert_path in alert_files:
    
    #Read YAML file
    with open(alert_path, "r") as stream:
        config = yaml.safe_load(stream)

    # Check if the alert is enabled
    if config["enabled"]:

        #Add to global scope
        globals()[config["nickname"]] = create_dag_alert(config)