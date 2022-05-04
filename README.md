# db2_table_compare

**Content:**
1. tools.py
   Contains the routines to compare tables
2. compare_sheet.py
   Worksheet for the table comparison

**How To**   
The worksheet consists of three areas   
Area No.1: 
```
# Default-Werte: Nicht Ändern`
src = tools.Db()
trg = tools.Db()
line = tools.Compare()
```
This area contains some default values and should not be changed.

Area No.2:
```
src_con = src.connect('server',50000,'database','','','','conuser','sesuser')
trg_con = trg.connect('server',50000,'database','','','','conuser','sesuser')
```
In this area you should type in:
   the servername which contains the database (source or target) "could be the same"
   the port of the database on this server
   the database name
   three fields for ssl (if you don't use SSL you can leave it blank)
   the user to connect to the database
   the security-user (this is just needed if you use my db2-security-system)
   
Area No.3:
```
line.execute(src_con,'source_schema','*',trg_con,'target_schema','*')
```
In this area you type in the source-schema, the source-table, the target-schema and the target-table

**Execute**
To run the program: python compare_sheet.py
The program requests you for the password for the user on the source-server.
Then it requests you for the password for the user on the target server.
Then the program compares the table from the source with the table in the target
and print the SQL-STATEMENTS to make the target-table equal to the source-table.

Now you just copy the SQL-STATEMENTS and run it in the target database and make the
target-table equal to the source-table.
