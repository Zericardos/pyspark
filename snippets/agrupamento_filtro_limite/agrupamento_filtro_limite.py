# agrupamento simples com limite e filtro
# Uso: re-or-se-e-h
import pyspark.sql.functions as f
from pyspark.sql.window import Window


data_reorseeh = [
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
schema_reorseeh_2 = [
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
df_reorseeh = spark.createDataFrame(data=data_reorseeh, schema=schema_reorseeh)

df_reorseeh.show()
grupo = "state"
w = Window.partitionBy(grupo).orderBy("date")
df_4_auxiliar = df_reorseeh.filter("department = 'FA'")
df_4_auxiliar.show()
w2 = Window.partitionBy("state").orderBy("state")
df_4_auxiliar.groupBy("state").agg(f.count("state").alias("typecount")).join(df_reorseeh, ["state"], "outer").withColumn("row", f.row_number().over(w2)).filter(f.col("row") == 1).orderBy("state").na.fill(0).show()
# +-----+---------+---+---------+---------+---------+----------+--------+--------+------+-----------+-----------+---+
# |state|typecount| id|   salary|firstname| lastname|department|initdate| enddate|gender|     county|expenditure|row|
# +-----+---------+---+---------+---------+---------+----------+--------+--------+------+-----------+-----------+---+
# |   CA|        2|  2|123234872|   _\d89e|   Rogers|        FA|20103111|20230125|     M|      Butte|5099.380859|  1|
# |   CC|        1|  1|123234878|    _\ddd|   Rogers|        FI|20103110|20230124|     M|    Alameda|6384.911133|  1|
# |   CD|        1| 12|654873219|   Zacary|    Efron|        FA|20103116|20230127|     M|     Fresno|5447.345215|  1|
# |   MG|        0|  7|326587417|      Joe|  Stevens|        FI|20103110|20230124|     M|San Joaquin|5253.331055|  1|
# |   PR|        1|  9|332569843|   George| ODonnell|        FA|20103116|20230127|     F|     Fresno| 5355.54834|  1|
# |   SC|       15|  5|152934485|    Anand|Manikutty|        FA|20104110|20230127|     F|      Butte|5235.987793|  1|
# |   SE|        0|  8|332154719|Mary-Anne|   Foster|        FE|20103115|20230129|     F|       Kern|4565.746094|  1|
# +-----+---------+---+---------+---------+---------+----------+--------+--------+------+-----------+-----------+---+
