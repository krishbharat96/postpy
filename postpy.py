import re
import pandas as pd
import cStringIO as io

string_exec = '{str_table} = {str_table} + """{line}"""'

def op_dataframe(tbl):
    return pd.read_csv(io.StringIO(tbl), delimiter = '\t', dtype=object, encoding='utf-8')

def create_header_string(header_arr):
    string = ""
    if len(header_arr) > 0:
        for n in range(len(header_arr)):
            if (n == 0):
                string = header_arr[0]
            else:
                string = string + "\t" + header_arr[n]
        string = string + "\n"
    return string
               
def get_tables(filename, table_name_arr):
    table_act_arr = []
    tbl_copy = dict()
    file = open(filename, "r")

    for table in table_name_arr:
        exec(table + '_str = ""')

    str_table = ""
    on_table = False
    for line in file:
        if (line.strip() == "\."):
            on_table = False
        if not (str_table == "") and (on_table == True):
            exec(string_exec.format(str_table=str_table, line=line))
        if "-- Name:" in line:
            tbl_name = line.split(";")[0].replace("-- Name: ", "").strip()
            if not tbl_name in table_act_arr and "TABLE" in line.split(";")[1]:
                table_act_arr.append(tbl_name)
        if "COPY" in line[:4]:
            on_table = False
            str_table = ""
            tbl = line.split("(")[0].replace("COPY ", "").strip()
            if tbl in table_name_arr:
                on_table = True
                str_table = tbl + "_str"
            columns = re.search(r'\((.*?)\)',line)
            column_string = ""
            if not columns is None:
                column_string = columns.group(1)
                column_arr = column_string.split(",")
                column_string = []
                for c in column_arr:
                    column_string.append(c.strip())
            tbl_copy.update({tbl:column_string})

    print table_act_arr
    print tbl_copy

    return_arr = []
    for t in table_name_arr:
        header_str = create_header_string(tbl_copy[t])
        var_str = t + "_str"
        exec("full_str = header_str + "  + var_str )
        return_arr.append(op_dataframe(full_str))
    return return_arr

def get_table(filename, table_name):
    table_act_arr = []
    tbl_copy = dict()
    file = open(filename, "r")

    exec(table_name + '_str = ""')

    str_table = ""
    on_table = False
    for line in file:
        if (line.strip() == "\."):
            on_table = False
        if not (str_table == "") and (on_table == True):
            exec(string_exec.format(str_table=str_table, line=line))
        if "-- Name:" in line:
            tbl_name = line.split(";")[0].replace("-- Name: ", "").strip()
            if not tbl_name in table_act_arr and "TABLE" in line.split(";")[1]:
                table_act_arr.append(tbl_name)
        if "COPY" in line[:4]:
            on_table = False
            str_table = ""
            tbl = line.split("(")[0].replace("COPY ", "").strip()
            if tbl == table_name:
                on_table = True
                str_table = tbl + "_str"
            columns = re.search(r'\((.*?)\)',line)
            column_string = ""
            if not columns is None:
                column_string = columns.group(1)
                column_arr = column_string.split(",")
                column_string = []
                for c in column_arr:
                    column_string.append(c.strip())

            tbl_copy.update({tbl:column_string})

    print table_act_arr
    print tbl_copy

    header_str = create_header_string(tbl_copy[table_name])
    var_str = table_name + "_str"
    exec("full_str = header_str + "  + var_str )
    full_df = op_dataframe(full_str)
    
    return full_df

def left_join(df1, col1, df2, col2, suffixl="_l", suffixr="_r"):
    fin_df = df1.merge(df2, left_on=col1, right_on=col2, how='left', suffixes=(suffixl, suffixr))
    return fin_df

def right_join(df1, col1, df2, col2, suffixl="_l", suffixr="_r"):
    fin_df = df1.merge(df2, left_on=col1, right_on=col2, how='right', suffixes=(suffixl, suffixr))
    return fin_df

def inner_join(df1, col1, df2, col2, suffixl="_l", suffixr="_r"):
    fin_df = df1.merge(df2, left_on=col1, right_on=col2, how='left', suffixes=(suffixl, suffixr))
    return fin_df

def outer_join(df1, col1, df2, col2, suffixl="_l", suffixr="_r"):
    fin_df = df1.merge(df2, left_on=col1, right_on=col2, how='left', suffixes=(suffixl, suffixr))
    return fin_df


