from redgrease import GB, configGet, execute


def config_names():
    yield "MaxExecutions"
    yield "MaxExecutionsPerRegistration"


def copy_config(config_name):
    config_value = configGet(config_name)
    execute("SET", f"GearConfig:{config_name}", config_value)


gb = GB("PythonReader")
gb.foreach(copy_config)
gb.run(config_names)
