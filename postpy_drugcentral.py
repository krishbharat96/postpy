import pandas as pd
import cStringIO as io
import re

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

def process_structures(struct_str, col_arr):
    struc = io.StringIO(struct_str)
    overall_arr = []
    one_arr = []
    on_struc = False
    on_blank = False
    for line in struc:
        split = line.split("\t")
        if (on_blank == True):
            if len(split) < 1:
                continue
            else:
                temp_split = split
                if (temp_split[0] == ""):
                    temp_split.pop(0)
                one_arr = one_arr + temp_split
                temp_arr = one_arr
                for n in range(len(one_arr)):
                    if one_arr[n] == "\n":
                        temp_arr.pop(n)
                one_arr = temp_arr
                overall_arr.append(one_arr)
                on_blank = False
        else:
            if len(split) > 20 and not (split[0].strip() == "") and not (split[0].strip() == "t") and not (split[0].strip() == "f") and not (split[1].strip() == "t") and not (split[1].strip() == "f"):
                one_arr = split
                overall_arr.append(one_arr)
                one_arr = []
            
            else:
                if len(split) < 2:
                    on_struc = True
                else:
                    if not on_struc == True:
                        if (split[0].isdigit() == True):
                            if (len(split) == 18):
                                one_arr = one_arr + (line.replace("\n", "struc").split("\t"))
                            else:
                                on_blank = True
                                one_arr = line.replace("\n", "").split("\t")
                                last_index = len(one_arr) - 1
                                if one_arr[last_index] == "":
                                    one_arr.pop(last_index)
                        elif (split[0] == ""):
                            split_temp = split
                            split_temp.pop(0)
                            one_arr = one_arr + split_temp
                            overall_arr.append(one_arr)
                            one_arr = []               
                if 'M  END' in line:
                    on_struc = False
    largest = 0
    extract_id = []
    id_arr = []
    for elem in overall_arr:
        if not elem[0] in id_arr:
            id_arr.append(elem[0])
        else:
            extract_id.append(elem[0])

    new_arr = overall_arr
    empty_arr = []
    num = 0
    for n in range(len(overall_arr)):
        if len(overall_arr[n]) < 57:
            num = n

    new_arr.pop(num)

    overall_df = pd.DataFrame(new_arr, columns = col_arr, dtype=object)
    return overall_df
                
def get_tables(filename, table_name_arr):
    table_act_arr = []
    tbl_copy = dict()
    file = open(filename, "r")

    dup_arr = table_name_arr

    is_struc = False
    if 'structures' in table_name_arr:
        is_struc = True
        for n in table_name_arr:
            if (table_name_arr[n] == 'structures'):
                struc_num = n
        table_name_arr.pop(struc_num)
    
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
            if tbl in table_name_arr and not tbl == 'structures':
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

    if (table_name == 'structures'):
        full_df = process_structures(structures_str, tbl_copy[table_name])
    else:
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
    
def select(df1, column_arr):
    fin_df = df1[column_arr]
    return fin_df
