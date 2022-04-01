#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import ibm_db
import tkinter as tk
from tkinter import ttk, simpledialog

def connect(server,port,db,ssl_path,ssl_key,ssl_stash,user,session_user):
    connection = ""
    ssl:str = ""
    if ssl_path != "":
        ssl_path = ssl_path.replace("\\","/")
        if ssl_key != "":
            if ssl_stash != "":
                ssl = ";SSLClientKeystoredb="+str(ssl_path)+"/"+str(ssl_key)+\
                    ";SSLClientKeystash="+str(ssl_path)+"/"+str(ssl_stash)+\
                    ";SECURITY=ssl;"
    tmp_pwd = simpledialog.askstring(title=f"{db}",\
        prompt=f"PASSWORD for user >> {user} <<",show="*")
    if tmp_pwd:
        conn_str = "DATABASE="+str(db)+";HOSTNAME="+str(server)+\
            ";PORT="+str(port)+";PROTOCOL=TCPIP;UID="+str(user)+\
            ";PWD="+str(tmp_pwd)+ssl
        try:
            connection = ibm_db.connect(conn_str,"","")
            print(f'Connection to database {db} for user {user} = OK')
        except Exception as e:
            print(f'Connection to database {db} for user {user} = ERROR')
            print(f"Error:{e}")
        if session_user != "":
            sql = "SET SESSION_USER = "+str(session_user)
            ibm_db.exec_immediate(connection,sql)
    return connection


def check_table(db,schema,name):
    flag:bool = False
    cursor = ""
    row:dict = {}
    print(f"-- Check if Tabelle {schema}.{name} exists:")
    sql = "SELECT count(*) AS ANZAHL FROM syscat.tables WHERE tabschema = '"+str(schema)+\
        "' AND tabname = '"+str(name)+"'"
    try:
        cursor = ibm_db.exec_immediate(db,sql)
    except Exception as e:
        print("Exec Error:",str(e))
    try:
        row = ibm_db.fetch_assoc(cursor)
    except Exception as e:
        print("Fetch Error:",str(e))
    if row['ANZAHL'] > 0:
        print(f'-- Table {schema}.{name} = OK')
        flag = True
    else:
        print(f'Table {schema}.{name} = ERROR')
    return flag


def get_table_struct(db,schema,name):
    """ Einlesen der Table-Spezifikationen """
    sql = """SELECT tabschema, tabname, tbspace, index_tbspace, long_tbspace, 
                    temporaltype
             FROM syscat.tables
             WHERE tabschema = '"""+schema+"""'
             AND tabname = '"""+name+"""'"""
    cursor = ibm_db.exec_immediate(db,sql)
    row = ibm_db.fetch_assoc(cursor)
    while row != False:
        table_struct = {}
        table_struct['TABSCHEMA'] = row['TABSCHEMA']
        table_struct['TABNAME'] = row['TABNAME']
        table_struct['TBSPACE'] = row['TBSPACE']
        table_struct['INDEX_TBSPACE'] = row['INDEX_TBSPACE']
        table_struct['LONG_TBSPACE'] = row['LONG_TBSPACE']
        table_struct['TEMPORALTYPE'] = row['TEMPORALTYPE']
        table_struct['COLNAME'] = {}
        row = ibm_db.fetch_assoc(cursor)
    if table_struct != {}:
        """ Einlesen der Feld-Spezifikationen """
        sql = """SELECT col.colno,col.colname,col.typename,col.length,col.scale,
                        col.nulls,
                        CASE WHEN col.default IS NOT NULL 
                            THEN CAST(col.default as VARCHAR(100)) 
                            ELSE '' 
                        END as default,
                        col.generated,col.identity,atr.start,atr.increment,
                        atr.minvalue,atr.maxvalue,atr.cycle,atr.cache,atr.order,
                        col.keyseq,
                        per.periodname,col.rowbegin,col.rowend,col.transactionstartid,
                        per.historytabschema,per.historytabname,per.begincolname,per.endcolname,per.periodtype
                 FROM syscat.columns as col 
                     LEFT OUTER JOIN syscat.colidentattributes atr 
                         ON (col.tabschema,col.tabname,col.colname) = (atr.tabschema,atr.tabname,atr.colname)
                     LEFT OUTER JOIN syscat.periods            per 
                         ON (col.tabschema,col.tabname) = (per.tabschema,per.tabname) 
                         AND (col.colname = per.begincolname 
                              OR col.colname = per.endcolname)
                 WHERE col.tabschema = '"""+schema+"""'
                 AND col.tabname = '"""+name+"""'
                 ORDER BY col.colno"""
        cursor = ibm_db.exec_immediate(db,sql)
        row = ibm_db.fetch_assoc(cursor)
        while row != False:
            name = row['COLNAME']
            table_struct['COLNAME'][str(name)] = {}
            table_struct['COLNAME'][str(name)]['TYPENAME'] = row['TYPENAME']
            table_struct['COLNAME'][str(name)]['LENGTH'] = row['LENGTH']
            table_struct['COLNAME'][str(name)]['SCALE'] = row['SCALE']
            table_struct['COLNAME'][str(name)]['NULLS'] = row['NULLS']
            table_struct['COLNAME'][str(name)]['DEFAULT'] = row['DEFAULT']
            table_struct['COLNAME'][str(name)]['GENERATED'] = row['GENERATED']
            table_struct['COLNAME'][str(name)]['IDENTITY'] = row['IDENTITY']
            table_struct['COLNAME'][str(name)]['START'] = row['START']
            table_struct['COLNAME'][str(name)]['INCREMENT'] = row['INCREMENT']
            table_struct['COLNAME'][str(name)]['MINVALUE'] = row['MINVALUE']
            table_struct['COLNAME'][str(name)]['MAXVALUE'] = row['MAXVALUE']
            table_struct['COLNAME'][str(name)]['CYCLE'] = row['CYCLE']
            table_struct['COLNAME'][str(name)]['CACHE'] = row['CACHE']
            table_struct['COLNAME'][str(name)]['ORDER'] = row['ORDER']
            table_struct['COLNAME'][str(name)]['KEYSEQ'] = row['KEYSEQ']
            table_struct['COLNAME'][str(name)]['PERIODNAME'] = row['PERIODNAME']
            table_struct['COLNAME'][str(name)]['ROWBEGIN'] = row['ROWBEGIN']
            table_struct['COLNAME'][str(name)]['ROWEND'] = row['ROWEND']
            table_struct['COLNAME'][str(name)]['TRANSACTIONSTARTID'] = row['TRANSACTIONSTARTID']
            table_struct['COLNAME'][str(name)]['HISTORYTABSCHEMA'] = row['HISTORYTABSCHEMA']
            table_struct['COLNAME'][str(name)]['HISTORYTABNAME'] = row['HISTORYTABNAME']
            table_struct['COLNAME'][str(name)]['BEGINCOLNAME'] = row['BEGINCOLNAME']
            table_struct['COLNAME'][str(name)]['ENDCOLNAME'] = row['ENDCOLNAME']
            table_struct['COLNAME'][str(name)]['PERIODTYPE'] = row['PERIODTYPE']
            row = ibm_db.fetch_assoc(cursor)
    return table_struct
    pass
    
    
def get_index_struct(db,schema,name):
    """ Einlesen der Indexe """
    """ UNIQUERULE -D Permits duplicates
                   -U Unique
                   -P Primary key
                   
        INDEXTYPE  -BLOCK Block Index
                   -CLUS  Clustering
                   -DIM   Dimension
                   -REG   Regular Index
    """
    index_struct:dict = {}
    sql = """SELECT indschema,indname,colnames,uniquerule,indextype,reverse_scans 
             FROM syscat.indexes
             WHERE tabschema = '"""+schema+"""'
             AND tabname = '"""+name+"""'"""
    cursor = ibm_db.exec_immediate(db,sql)
    row = ibm_db.fetch_assoc(cursor)
    sequence = 1
    while row != False:
        index_struct[sequence] = {}
        index_struct[sequence]['INDSCHEMA'] = row['INDSCHEMA']
        index_struct[sequence]['INDNAME'] = row['INDNAME']
        index_struct[sequence]['COLNAMES'] = row['COLNAMES']
        index_struct[sequence]['UNIQUERULE'] = row['UNIQUERULE']
        index_struct[sequence]['INDEXTYPE'] = row['INDEXTYPE']
        index_struct[sequence]['REVERSE_SCANS'] = row['REVERSE_SCANS']
        sequence += 1
        row = ibm_db.fetch_assoc(cursor)
    return index_struct


def list_tables(db,schema):
    table_list:list = []
    sql = f"SELECT tabname FROM syscat.tables WHERE tabschema = {schema}"
    cursor = ibm_db.exec_immediate(db,sql)
    row = ibm_db.fetch_assoc(cursor)
    while row != False:
        table_list.append(row['TABNAME'])
        row = ibm_db.fetch_assoc(cursor)
    return table_list


root = tk.Tk()
root.withdraw()

