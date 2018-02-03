# postpy
This an easy-to-use module that extracts table information directly from PostgreSQL dump files and extracts the tables as pandas dataframes. The information within the dataframes are also queryable using some functions that are found within the module. This has been designed for the SPOKE medical knowledge network (look at krishbharat96/SPOKE, and RBVI/SPOKE) in order to easily auto-update the data within SPOKE utilizing publicly available resources that sometimes appear as PostgreSQL files. In addition to the postpy.py module, there is also a postpy_drugcentral.py module for parsing the DrugCentral Postgres database (since there are molecular structures within that database which are "parsed out" through this module, since they would be problematic to store within a pandas dataframe). The Python Pandas package is required for this module. 

1. In order to download postpy, use (within the directory that you will be parsing the .sql file):
```
wget https://raw.githubusercontent.com/krishbharat96/postpy/master/postpy.py
```
2. To use postpy, make sure you have unzipped and extracted the postgres .sql dump (let's call the file psqldmp.sql), and type in the commands below:

  a. Import package:
  ```
  import postpy as ppy
  ```
  b. You can obtain a single table or even obtain multiple tables by using the get_table(dump_filename, tablename) and           get_tables(dump_filename, ['table1', 'table2', ...])  methods respectively. For the get_tables method, the dataframes will     be stored as an array.
  ```
  single_table_df = ppy.get_table(psqldump.sql, 'single_table')
  ```
  ```
  many_tables_df_arr = ppy.get_tables(psqldump.sql, ['table1', 'table2', 'table3'])
  ```
  c. After obtaining the dataframes, you can perform joins on the pandas dataframes either through using pandas tools or also   using some parts of this package as well (which are derived from pandas tools). Simple left, right, outer, and inner joins     can be performed using this module. You can call the join methods through left_join(df1, col1, df2, col2, suffixl=(lsuf),     suffixr=(rsuf)). Col1 and Col2 are the two columns that you will be joining on. The two suffixes are, by default an           underscore followed by 'l', and 'r' respectively.
  ```
  joined_df = table1_df.left_join(df1, 'col1', df2, 'col2', suffixl='_l', suffixr='_r')
  ```
  
  
