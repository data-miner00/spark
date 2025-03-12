# Pyspark Version Warning

Pyspark error when running `main.py` with version `3.4.0`. Issue subsided after downgrading version to `3.3.2`.

```
Traceback (most recent call last):
  File "D:\Workspace\projects\spark\main.py", line 8, in <module>
    df = spark.createDataFrame([(1, None, "a"), (2, "b", None), (3, "c", "d")], ["id", "col1", "col2"])
  File "C:\Users\User\.virtualenvs\spark-cdsr9IqH\lib\site-packages\pyspark\sql\session.py", line 1276, in createDataFrame
    return self._create_dataframe(
  File "C:\Users\User\.virtualenvs\spark-cdsr9IqH\lib\site-packages\pyspark\sql\session.py", line 1318, in _create_dataframe
    rdd, struct = self._createFromLocal(map(prepare, data), schema)
  File "C:\Users\User\.virtualenvs\spark-cdsr9IqH\lib\site-packages\pyspark\sql\session.py", line 962, in _createFromLocal
    struct = self._inferSchemaFromList(data, names=schema)
  File "C:\Users\User\.virtualenvs\spark-cdsr9IqH\lib\site-packages\pyspark\sql\session.py", line 834, in _inferSchemaFromList
    infer_array_from_first_element = self._jconf.legacyInferArrayTypeFromFirstElement()
  File "C:\Users\User\.virtualenvs\spark-cdsr9IqH\lib\site-packages\py4j\java_gateway.py", line 1322, in __call__
    return_value = get_return_value(
  File "C:\Users\User\.virtualenvs\spark-cdsr9IqH\lib\site-packages\pyspark\errors\exceptions\captured.py", line 169, in deco
    return f(*a, **kw)
  File "C:\Users\User\.virtualenvs\spark-cdsr9IqH\lib\site-packages\py4j\protocol.py", line 330, in get_return_value
    raise Py4JError(
py4j.protocol.Py4JError: An error occurred while calling o28.legacyInferArrayTypeFromFirstElement. Trace:
py4j.Py4JException: Method legacyInferArrayTypeFromFirstElement([]) does not exist
        at py4j.reflection.ReflectionEngine.getMethod(ReflectionEngine.java:318)
        at py4j.reflection.ReflectionEngine.getMethod(ReflectionEngine.java:326)
        at py4j.Gateway.invoke(Gateway.java:274)
        at py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)
        at py4j.commands.CallCommand.execute(CallCommand.java:79)
        at py4j.ClientServerConnection.waitForCommands(ClientServerConnection.java:182)
        at py4j.ClientServerConnection.run(ClientServerConnection.java:106)
        at java.base/java.lang.Thread.run(Thread.java:833)
```

**The general rule of thumb is to make sure that the Pyspark version and Spark version to be the same.**

If `pyspark==3.5.5`, then the Spark version should be `spark-3.5.5-bin-hadoop3.tgz`.

To install `pyspark` with a specific version with `pip`, run `pip install pyspark==3.5.5`.

Also make sure to set or update the `SPARK_HOME` environment variable that points to the folder where the Spark with the correct version resides.

```
export SPARK_HOME=C:/spark-3.5.5-bin-hadoop3
```
