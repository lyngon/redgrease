import redgrease


def double(record):

    try:
        val = record["value"]
        key = record["key"]
    except KeyError as ex:
        err_msg = f"Record {record}, does not seem to be a normal record"
        redgrease.log(err_msg + f": {ex}")
        raise ValueError(err_msg) from ex

    try:
        val = redgrease.cmd.get(key)
    except Exception as ex:
        err_msg = f"Unable to get key '{key}'"
        redgrease.log(err_msg + f": {ex}")
        raise KeyError(err_msg) from ex

    try:
        val = float(val)
    except Exception as ex:
        err_msg = f"The value of the key {key} did not float very well"
        redgrease.log(err_msg + f": {ex}. Blubb, blubb!")
        raise TypeError(err_msg) from ex
    else:
        val *= 2
        redgrease.cmd.set(key, val)


redgrease.GB(redgrease.ReaderType.KeysReader).foreach(double).run()
