def _run_alert(**context):

    #Get data using query"
    log.info("Get data using query")
    query = config["query"]
    log.debug(f"Query: {query}")


    #Run Query Against Snowflake
    result = sf.query(query)
    result_value = result[0][0]

    #Default action is not notify
    notify = False

    # Compare return value to condition
    criteria = config["criteria"].strip().lower().replace(" ", "_")

    if criteria == "greator_than":
        if result_value > config["value"]:
            notify = True


    elif criteria == "equal_to":
        if result_value == config["value"]:
            notify = True

    elif criteria=="less_than":
        if result_value == config[""]:
            notify = True

    else:
        log.error(f"Unknown condition criteria: {criteria}")
        log.error("Original condition criteria:", config["criteria"])
        log.error(
            "Check the value of the 'criteria' parameter file of this alert"
        )                
        raise RuntimeError(f"Unknown condition criteria: {criteria}")

    #Notify only if condition was met
        