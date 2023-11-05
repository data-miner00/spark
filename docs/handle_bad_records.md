| Parse Mode      | Behaviour                                                        |
| --------------- | ---------------------------------------------------------------- |
| `PERMISSIVE`    | Includes corrupt records in a `_corrupt_record` column (default) |
| `DROPMALFORMED` | Ignores all corrupted records                                    |
| `FAILFAST`      | Throws an exception when it meets corrupted records              |

```
df = spark.read.option("mode", "PERMISSIVE").option("columnOfCorruptrecord", "_corrupt_record").csv("file.csv", header=True, inferSchema=True)

df.cache()
display(df.filter("_corrupt_record is not null"))
df.unpersist()
```

## `badRecordsPath` option to store rejected records

```
corruptDf = spark.read.option("badRecordsPath", "/tmp/").json(sc.parallelize(data))

display(spark.read.json("/tmp/*/bad_records/*"))
```

```
%fs ls /tmp/channels/
```
