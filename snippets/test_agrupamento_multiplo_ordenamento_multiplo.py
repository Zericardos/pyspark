import pyspark.sql.functions as f
from pyspark.sql.window import Window


data_procoabo_2 = [
    (1,  123234878, r"_\ddd",    "Rogers","CC","FI", 20103110, 20230124, "M", "Alameda", 6384.911133),
    (2,  123234872, r"_\d89e",   "Rogers","CA","FA", 20103111, 20230125, "M", "Butte", 5099.380859),
    (3,  123234874, r"_\d89",    "Rogers","CA","FA", 20103112, 20230124, "M", "Butte", 5501.95459),
    (4,  123234873, "_89",       "Rogers","CC","FE", 20104110, 20230124, "M", "Butte", 7101.831055),
    (5,  152934485, "Anand",     "Manikutty","SC","FA", 20104110, 20230127, "F", "Butte", 5235.987793),
    (6,  222364883, "Carol",     "Smith","SC","FA", 20103115, 20230124, "M", "Fresno", 5580.146973),
    (7,  326587417, "Joe",       "Stevens","MG","FI", 20103110, 20230124, "M", "San Joaquin", 5253.331055),
    (8,  332154719, "Mary-Anne", "Foster","SE","FE", 20103115, 20230129, "F", "Kern", 4565.746094),
    (9,  332569843, "George",    "ODonnell","PR","FA", 20103116, 20230127, "F", "Fresno", 5355.54834),
    (10, 546523478, "John",      "Doe","CC","FI", 20103116, 20230124, "M", "Sacramento", 5036.211426),
    (11, 631231482, "David",     "Smith","CA","FE", 20103116, 20230124, "M", "Merced", 4547.692383),
    (12, 654873219, "Zacary",    "Efron","CD","FA", 20103116, 20230127, "M", "Fresno", 5447.345215),
    (13, 745685214, "Eric",      "Goldsmith","CC","FA", 20103110, 20230124, "M", "Tulare", 6567.149414),
    (14, 845657245, "Elizabeth", "Doe","SC","FA", 20103110, 20230129, "F", "Tulare", 4818.612793),
    (15, 845657246, "Kumar",     "Swamy","CA","FI", 20103110, 20230129, "F", "Tulare", 5621.456055),
    (16, 158787946, "Joao",      "Borges","SC","FA",20090106,20230124,"M","Tulare",7821.57892),
    (17, 897123057, "Joaquina",  "Carlota","SC","FA",20090106,20230125,"F","Sacramento",5872.5898722),
    (18, 897123057, "Jose",      "Ricardo","SC","FA",20090106,20230126,"M","Sacramento",5872.5898722),
    (19, 897123057, "Tadeu",     "Schimidt","SC","FA",20090106,20230126,"M","Sacramento",5872.5898722),
    (20, 897123057, "Franck",    "Nuncio","SC","FA",20090106,20230126,"M","Sacramento",5872.5898722),
    (21, 214548783, "Gerusa",    "Albuquerqu","SC","FA",20090106,20230126,"F","Sacramento",5872.5898722),
    (22, 578975482, "Pedro",     "Sanson","SC","FA",20090106,20230122,"M","Sacramento",5872.5898722),
    (23, 578975482, "Jung",      "Bugre","SC","FA",20090106,20230122,"M","Sacramento",5872.5898722),
    (24, 578975482, "Laura",     "Martins","SC","FA",20160809,20230122,"F","Sacramento",5872.5898722),
    (25, 578975482, "Larissa",   "Martins","SC","FA",20360809,20230125,"F","Sacramento",5872.5898722),
    (26, 578975482, "Debora",    "Tavares","SC","FA",20360809,20230125,"F","Sacramento",5872.5898722),
    (27, 578975482, "Festa",     "Eduardo","SC","FA",20360809,20230125,"M","Sacramento",5872.5898722),
    ]
schema_procoabo_2 = [
    "id",
    "salary",
    "firstname",
    "lastname",
    "state",
    "department",
    "initdate",
    "enddate",
    "gender",
    "county",
    "expenditure",
]
df_procoabo_2 = spark.createDataFrame(data=data_procoabo_2, schema=schema_procoabo_2)

df_procoabo_2.show()

df_procoabo_2.show()
grupo = ["state", "department", "gender"]
w = Window.partitionBy(grupo).orderBy(f.col("initdate").asc(), f.col("enddate").asc())
df_procoabo_2.withColumn(
    "initdate", f.max("initdate").over(w)
).withColumn("enddate", f.max("enddate").over(w)).withColumn(
    "row", f.row_number().over(w)).filter(f.col("row") == 1).drop("row").show()
""""
+---+---------+---------+---------+-----+----------+--------+--------+------+-----------+------------+
| id|   salary|firstname| lastname|state|department|initdate| enddate|gender|     county| expenditure|
+---+---------+---------+---------+-----+----------+--------+--------+------+-----------+------------+
|  7|326587417|      Joe|  Stevens|   MG|        FI|20103110|20230124|     M|San Joaquin| 5253.331055|
| 12|654873219|   Zacary|    Efron|   CD|        FA|20103116|20230127|     M|     Fresno| 5447.345215|
| 11|631231482|    David|    Smith|   CA|        FE|20103116|20230124|     M|     Merced| 4547.692383|
|  9|332569843|   George| ODonnell|   PR|        FA|20103116|20230127|     F|     Fresno|  5355.54834|
|  4|123234873|      _89|   Rogers|   CC|        FE|20104110|20230124|     M|      Butte| 7101.831055|
| 22|578975482|    Pedro|   Sanson|   SC|        FA|20090106|20230122|     M| Sacramento|5872.5898722|
|  1|123234878|    _\ddd|   Rogers|   CC|        FI|20103110|20230124|     M|    Alameda| 6384.911133|
| 15|845657246|    Kumar|    Swamy|   CA|        FI|20103110|20230129|     F|     Tulare| 5621.456055|
|  2|123234872|   _\d89e|   Rogers|   CA|        FA|20103111|20230125|     M|      Butte| 5099.380859|
| 17|897123057| Joaquina|  Carlota|   SC|        FA|20090106|20230125|     F| Sacramento|5872.5898722|
|  8|332154719|Mary-Anne|   Foster|   SE|        FE|20103115|20230129|     F|       Kern| 4565.746094|
| 13|745685214|     Eric|Goldsmith|   CC|        FA|20103110|20230124|     M|     Tulare| 6567.149414|
+---+---------+---------+---------+-----+----------+--------+--------+------+-----------+------------+

"""
