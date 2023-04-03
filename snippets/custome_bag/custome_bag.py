# function customebag

df_customebag = df_reprcabc.select(
    "*",
    f.explode(f.split(f.concat_ws(
        '#',
        f.concat_ws(
            ',',
            f.lit('A,C1'),
            f.expr(
                "CASE "
                "  WHEN state = 'CC' THEN '1'"
                "  ELSE '0' "
                "END"
            )
        ),
        f.concat_ws(
            ',',
            f.lit('B,C3'),
            f.expr(
                "CASE "
                "  WHEN state = 'SC' THEN '1'"
                "  ELSE '0' "
                "END"
            )
            )
        ), '#')).alias('temp')
)
print(df_reprcabc.count())
print(df_customebag.count())
#df_customebag.show()
col_customebag = f.split(df_customebag['temp'], ',')
df_customebag_2 = df_customebag.select(
    "*",
    col_customebag.getItem(0).alias("Char1"),
    col_customebag.getItem(1).alias("Char2"),
    col_customebag.getItem(2).alias("Char3"),
).drop("temp")
print(df_customebag_2.count())
#df_customebag_2.show()