from redgrease import GB, execute, gearsConfigGet


def config_names():
    yield "MaxExecutions"
    yield "DoesNotExist"


def copy_config(config_name):
    config_value = gearsConfigGet(config_name, 1337)
    execute("SET", f"GearConfig:{config_name}", config_value)


gb = GB("PythonReader")
gb.foreach(copy_config)
gb.run(config_names)
