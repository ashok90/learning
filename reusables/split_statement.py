import sqlparse
import sql_metadata


sql_text = ""

statements = sqlparse.split(sql_text)

table_list = []


for i in statements:
    table_list = sql_metadata.get_query_tables(i)
    all.extend(table_list)
    if len(table_list) < 1:
        print("Table Not available")
    elif len(table_list) == 1:
        print("Only one table is present {}".format(table_list))
    else:
        append_str=""
        for e in table_list[1:]:
            append_str= append_str+table_list[0] +'->' + e + '\n'
print(append_str)
