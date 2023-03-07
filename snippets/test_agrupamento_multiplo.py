import pyspark.sql.functions as f
from pyspark.sql.window import Window


data_procoabo = [
    (1,  123234878, r"_\ddd",     "Rogers","CC","FI", 20230124, "M", "Alameda", 6384.911133),
    (2,  123234872, r"_\d89e",    "Rogers","CA","FA", 20230125, "M", "Butte", 5099.380859),
    (3,  123234874, r"_\d89",     "Rogers","CA","FA", 20230124, "M", "Butte", 5501.95459),
    (4,  123234873, "_89",       "Rogers","CC","FE", 20230124, "M", "Butte", 7101.831055),
    (5,  152934485, "Anand",     "Manikutty","SC","FA", 20230127, "F", "Butte", 5235.987793),
    (6,  222364883, "Carol",     "Smith","SC","FA", 20230124, "M", "Fresno", 5580.146973),
    (7,  326587417, "Joe",       "Stevens","MG","FI", 20230124, "M", "San Joaquin", 5253.331055),
    (8,  332154719, "Mary-Anne", "Foster","SE","FE", 20230129, "F", "Kern", 4565.746094),
    (9,  332569843, "George",    "ODonnell","PR","FA", 20230127, "F", "Fresno", 5355.54834),
    (10, 546523478, "John",    "Doe","CC","FI", 20230124, "M", "Sacramento", 5036.211426),
    (11, 631231482, "David",    "Smith","CA","FE", 20230124, "M", "Merced", 4547.692383),
    (12, 654873219, "Zacary",    "Efron","CD","FA", 20230127, "M", "Fresno", 5447.345215),
    (13, 745685214, "Eric",    "Goldsmith","CC","FA", 20230124, "M", "Tulare", 6567.149414),
    (14, 845657245, "Elizabeth",    "Doe","SC","FA", 20230129, "F", "Tulare", 4818.612793),
    (15, 845657246, "Kumar",    "Swamy","CA","FI", 20230129, "F", "Tulare", 5621.456055)
    ]
schema_procoabo = [
    "id",
    "salary",
    "firstname",
    "lastname",
    "state",
    "department",
    "date",
    "gender",
    "county",
    "expenditure",
]
df_procoabo = spark.createDataFrame(data=data_procoabo, schema=schema_procoabo)

df_procoabo.show()

df_procoabo.show()
grupo = ["state", "department", "gender"]
w = Window.partitionBy(grupo).orderBy("date")
df_procoabo.withColumn("min", f.min("date").over(w)).withColumn("row", f.row_number().over(w)).filter(f.col("row") == 1).show()
""""
+-----+--------+------+-------+---+---------+---------+---------+----------+-----------+-----------+---+
|state|    date|gender|min(id)| id|   salary|firstname| lastname|department|     county|expenditure|row|
+-----+--------+------+-------+---+---------+---------+---------+----------+-----------+-----------+---+
|   CD|20230124|     M|     12| 12|654873219|   Zacary|    Efron|        FA|     Fresno|5447.345215|  1|
|   CA|20230129|    F4|     15| 15|845657246|    Kumar|    Swamy|        FI|     Tulare|5621.456055|  1|
|   CC|20230124|     M|      1|  1|123234878|    _\ddd|   Rogers|        FI|    Alameda|6384.911133|  1|
|   CA|20230124|     M|      3|  3|123234874|    _\d89|   Rogers|        FA|      Butte| 5501.95459|  1|
|   PR|20230124|     F|      9|  9|332569843|   George| ODonnell|        FA|     Fresno| 5355.54834|  1|
|   SC|20230127|     F|      5|  5|152934485|    Anand|Manikutty|        FA|      Butte|5235.987793|  1|
|   SC|20230124|     F|     14| 14|845657245|Elizabeth|      Doe|        FA|     Tulare|4818.612793|  1|
|   CA|20230125|     M|      2|  2|123234872|   _\d89e|   Rogers|        FA|      Butte|5099.380859|  1|
|   SC|20230124|     M|      6|  6|222364883|    Carol|    Smith|        FA|     Fresno|5580.146973|  1|
|   SE|20230124|     F|      8|  8|332154719|Mary-Anne|   Foster|        FE|       Kern|4565.746094|  1|
|   MG|20230124|     M|      7|  7|326587417|      Joe|  Stevens|        FI|San Joaquin|5253.331055|  1|
+-----+--------+------+-------+---+---------+---------+---------+----------+-----------+-----------+---+
"""
