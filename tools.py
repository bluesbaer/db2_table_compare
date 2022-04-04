#!/usr/bin/env python3

import ibm_db
import tkinter as tk
from tkinter import ttk, simpledialog

# --------------------------------------------------------------------------------
# VERSION 1 by Manfred Wagner
# --------------------------------------------------------------------------------
# Class: Db
# Interface to the database
# --------------------------------------------------------------------------------
class Db():

    def __init__(self):
        pass

    """ Connects to the database
        ssl_path, ssl_key, ssl_stash is used for encrypted access
        session_user is just used where it is implemented
    """
    def connect(self,server,port,db,ssl_path,ssl_key,ssl_stash,user,session_user):
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
            prompt=f"Input password for user >> {user} <<",show="*")
        # -- TEST --
        if tmp_pwd == "":
            tmp_pwd = 'IhMW&bue50'
        # -- TEST --
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
    
    """ Check if the table is available """
    def check_table(self,db,schema,name):
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
            print(f'-- Table {schema}.{name} = ERROR')
        return flag
    
    """ Get all information about the table """
    def get_table_struct(self,db,schema,name):
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
        
    """ Get the index-information for the table """        
    def get_index_struct(self,db,schema,name):
        """ Einlesen der Indexe """
        """ UNIQUERULE -D Permits duplicates
                       -U Unique
                       -P Primary key
                       
            INDEXTYPE  -BLOCK Block Index
                       -CLUS  Clustering
                       -DIM   Dimension
                       -REG   Regular Index
        """
        index_struct:list = []
        sql = """SELECT idx.tabschema,idx.tabname,idx.indschema,idx.indname,idx.uniquerule,idx.indextype,idc.colname,idc.colorder,idx.reverse_scans
                FROM syscat.indexes as idx JOIN syscat.indexcoluse as idc ON (idx.indschema,idx.indname) = (idc.indschema,idc.indname)
                WHERE idx.tabschema = '"""+schema+"""' AND idx.tabname = '"""+name+"""'
                ORDER BY idx.tabschema,idx.tabname,idx.indname,idc.colseq"""
        cursor = ibm_db.exec_immediate(db,sql)
        row = ibm_db.fetch_assoc(cursor)
        while row != False:
            tmp:dict = {}
            tmp["TABSCHEMA"] = row["TABSCHEMA"].strip()
            tmp["TABNAME"] = row["TABNAME"].strip()
            tmp["INDSCHEMA"] = row["INDSCHEMA"].strip()
            tmp["INDNAME"] = row["INDNAME"].strip()
            tmp["UNIQUERULE"] = row["UNIQUERULE"].strip()
            tmp["INDEXTYPE"] = row["INDEXTYPE"].strip()
            tmp["COLNAME"] = row["COLNAME"].strip()
            tmp["COLORDER"] = row["COLORDER"].strip()
            tmp["REVERSE_SCANS"] = row["REVERSE_SCANS"].strip()
            index_struct.append(tmp)
            row = ibm_db.fetch_assoc(cursor)
        return index_struct
    
    """" List all the tables for an specified schema """
    def list_tables(self,db,schema):
        table_list:list = []
        sql = f"SELECT tabname FROM syscat.tables WHERE tabschema = '{schema}'"
        cursor = ibm_db.exec_immediate(db,sql)
        row = ibm_db.fetch_assoc(cursor)
        while row != False:
            table_list.append(row['TABNAME'])
            row = ibm_db.fetch_assoc(cursor)
        return table_list


# --------------------------------------------------------------------------------
# Class: Table
# Prepares all the information about the table
# Table: tabschema,tabname,tbspace,index_tbspace,long_tbspace
# Column: datatype, nullable, default, generated, identity, period,system_time,business_time
# --------------------------------------------------------------------------------
class Table():

    def __init__(self,tabstruct:dict) -> None:
        self.tabstruct:dict = tabstruct
        self.table_info:dict = {}
        self.field_dict:dict = {}
        self.attribute_dict:dict = {}
        self.period_dict:dict = {'SYSTEM_TIME':'N', 'BUSINESS_TIME':'N'}
        self.fill_table_info()
        self.fill_field_dict()
        #self.fill_attribute_dict()
        pass

    """ Get the table information """
    def fill_table_info(self):
        tmptyp = '--'
        if self.tabstruct['TEMPORALTYPE'] != 'N':
            if self.tabstruct['TEMPORALTYPE'] == 'A':
                tmptyp = 'Business Time'
            if self.tabstruct['TEMPORALTYPE'] == 'B':
                tmptyp = 'Bitemporal'
            if self.tabstruct['TEMPORALTYPE'] == 'S':
                tmptyp = 'System Time'
        self.table_info['TABSCHEMA'] = self.tabstruct['TABSCHEMA'].strip()
        self.table_info['TABNAME'] = self.tabstruct['TABNAME'].strip()

        self.table_info['TBSPACE'] = self.tabstruct['TBSPACE']
        self.table_info['INDEX_TBSPACE'] = self.tabstruct['INDEX_TBSPACE']
        self.table_info['LONG_TBSPACE'] = self.tabstruct['LONG_TBSPACE']
        self.table_info['TEMPORALTYPE'] = tmptyp

    """ Get the field (column) information """
    def fill_field_dict(self):
        for field_name in self.tabstruct['COLNAME'].keys():
            self.field_dict[field_name] = {}
            self.field_dict[field_name]['DATATYPE'] = self.generate_datatype(self.tabstruct['COLNAME'][field_name])
            self.field_dict[field_name]['NULLABLE'] = self.generate_nullable(self.tabstruct['COLNAME'][field_name])
            self.field_dict[field_name]['DEFAULT'] = self.generate_default(self.tabstruct['COLNAME'][field_name])
            self.field_dict[field_name]['GENERATED'] = self.generate_generate(self.tabstruct['COLNAME'][field_name])
            self.field_dict[field_name]['IDENTITY'] = self.generate_identity(self.tabstruct['COLNAME'][field_name])
            self.field_dict[field_name]['PERIOD'] = self.generate_period(self.tabstruct['COLNAME'][field_name])

    """ Creates the datatype """
    def generate_datatype(self,struct:dict) -> str:
        datatype = struct['TYPENAME']
        length = struct['LENGTH']
        scale = struct['SCALE']
        tmp:str = ""
        if datatype in ['SMALLINT','INTEGER','INT','BIGINT','FLOAT','REAL','DOUBLE',\
            'DOUBLE PRECISION','DECFLOAT','DATE','TIME']:
            tmp = f"{datatype} "
        elif datatype in ['DECIMAL','DEC','NUMERIC']:
            tmp = f"{datatype}({length},{scale}) "
        elif datatype in ['CHAR','CHARACTER','CHARACTER VARYING','VARCHAR']:
            tmp = f"{datatype}({length}) "
        elif datatype in ['TIMESTAMP']:
            tmp = f"{datatype}({length-1}) "   
        elif datatype in ['BLOB','CLOB']:
            tmp = f"{datatype}({length})"
        return tmp

    """ Creates the not null information """
    def generate_nullable(self,struct:dict) -> bool:
        tmp:bool = True
        if struct['NULLS'] == 'N':
            tmp = False
        return tmp

    """ Creates the default information """
    def generate_default(self,struct:dict)-> str:
        tmp:str = ""
        if struct['DEFAULT']:
            tmp = f"DEFAULT {struct['DEFAULT']}"
        return tmp
        pass

    """ Creates the generated information """
    def generate_generate(self,struct:dict) -> str:
        tmp:str = ""
        if struct['GENERATED'] == 'A':
            tmp = 'GENERATED ALWAYS'       
        elif struct['GENERATED'] == 'D':
            tmp = 'GENERATED BY DEFAULT'
        return tmp
        pass

    """ Creates the identity information """
    def generate_identity(self,struct:dict) -> dict:
        tmp:dict = {'IDENTITY':'N', 'START':0, 'INCREMENT':0, 'MINVALUE':0, 'MAXVALUE':0,
            'CACHE':'', 'ORDER':''}
        if struct['IDENTITY'] == 'Y':
            tmp['IDENTITY'] = "AS IDENTITY" 
            tmp['START'] = struct['START']
            tmp['INCREMENT'] = struct['INCREMENT']
            tmp['MINVALUE'] = f"MINVALUE {struct['MINVALUE']}"
            if int(struct['MAXVALUE']) >= 9000000000000000000:
                tmp['MAXVALUE'] = "NO MAXVALUE"
            else:
                tmp['MAXVALUE'] = f"MAXVALUE {struct['MAXVALUE']}"
            if struct['CACHE'] > 1:
                tmp['CACHE'] = f"CACHE {struct['CACHE']}"
            else:
                tmp['CACHE'] = "NO CACHE"
            if struct['ORDER'] == 'N':
                tmp['ORDER'] = "NO ORDER"
            else:
                tmp['ORDER'] = "ORDER"
        return tmp

    """ Creates the period information """
    def generate_period(self,struct:dict):
        tmp = ""
        if struct['ROWBEGIN'] == 'Y':
            tmp = 'AS ROW BEGIN'
        elif struct['ROWEND'] == 'Y':
            tmp = 'AS ROW END'
        elif struct['TRANSACTIONSTARTID'] == 'Y':
            tmp = 'AS TRANSACTION START ID'
        if struct['PERIODNAME'] == 'SYSTEM_TIME':
            self.period_dict['SYSTEM_TIME'] = f"PERIOD SYSTEM_TIME({struct['BEGINCOLNAME']},{struct['ENDCOLNAME']})"
        if struct['PERIODNAME'] == 'BUSINESS_TIME':
            self.period_dict['BUSINESS_TIME'] = f"PERIOD BUSINESS_TIME({struct['BEGINCOLNAME']},{struct['ENDCOLNAME']})"
        return tmp
        pass

    """ For printing the table class """
    def __str__(self):
        _ = f"Table: {self.table_info['TABSCHEMA']}.{self.table_info['TABNAME']} "
        _ += f"Tablespace: {self.table_info['TBSPACE']}/{self.table_info['INDEX_TBSPACE']}/"
        _ += f"{self.table_info['LONG_TBSPACE']} "
        _ += f"Temporaltype: {self.table_info['TEMPORALTYPE']}"
        _ += "\nFields: "
        for name in self.field_dict.keys():
            _ += f"{name}, "
        return _


# --------------------------------------------------------------------------------
# Class: Compare
class Compare():

    """ Compare
        src_db ....... Source Database Connection
        src_schema ... Source Schema 
        src_table .... Source Table (for all tables in this schema = '*')
        trg_db ....... Target Database Connection
        trg_schema ... Target_schema 
        trg_table .... Target Table (for all tables in this schema = '*')
        """

    def __init__(self):
        self.db = Db()
        self.source_database = ""
        self.cmd_list:list = []
        self.idx_list:list = []
        self.period:dict = {'SYSTEM':{'STATUS':'N', 'VON':'', 'BIS':'', 'TS':''}, 'BUSINESS':{'STATUS':'N', 'VON':'', 'BIS':''}}
        self.history:dict = {'SOURCE':'', 'TARGET':''}
        pass
    
    """ Main-Routine to compare tables """
    def execute(self,src_db,src_schema,src_table,trg_db,trg_schema,trg_table):
        self.source_database = src_db
        self.source_schema = src_schema
        if src_table == '*':
            """ Every table in the whole schema has to be checked """
            _tablist:list = []
            # get the list of tables
            _tablist = self.db.list_tables(src_db,src_schema)
            # now check for each table from source if there is a table in the target
            for _table in _tablist:
                if 'TH_' not in _table and 'V_' not in _table:
                    print(f"Table: {_table}")
                    src_table_struct = self.db.get_table_struct(src_db,src_schema,_table)
                    src_index_struct = self.db.get_index_struct(src_db,src_schema,_table)
                    self.check_hist_table('SOURCE',src_table_struct)
                    trg_status = self.db.check_table(trg_db,trg_schema,_table)
                    if trg_status:
                        # if the target table exists ( ALTER TABLE )
                        trg_table_struct = self.db.get_table_struct(trg_db,trg_schema,_table)
                        trg_index_struct = self.db.get_index_struct(trg_db,trg_schema,_table)
                        self.check_hist_table('TARGET',trg_table_struct)
                        self.alter_table(src_table_struct,trg_table_struct)
                        self.check_period(src_table_struct,trg_table_struct)
                        src_index_struct = self.index_rebuild(src_index_struct)
                        trg_index_struct = self.index_rebuild(trg_index_struct)
                        self.check_index(src_index_struct,trg_index_struct)
                        self.print_cmd(trg_schema,_table)
                    else:
                        # if the target table does not exist ( CREATE TABLE )
                        self.create_table(src_table_struct,trg_schema,_table)
                        # self.check_period(src_table_struct,???)
        else:
            """ Each table is declared """
            src_status = self.db.check_table(src_db,src_schema,src_table)
            if src_status:
                # If the sourcetable exist
                src_table_struct = self.db.get_table_struct(src_db,src_schema,src_table)
                src_index_struct = self.db.get_index_struct(src_db,src_schema,src_table)
                self.check_hist_table('SOURCE',src_table_struct)
                trg_status = self.db.check_table(trg_db,trg_schema,trg_table)
                if trg_status:
                    # if the target table exists ( ALTER TABLE )
                    trg_table_struct = self.db.get_table_struct(trg_db,trg_schema,trg_table)
                    trg_index_struct = self.db.get_index_struct(trg_db,trg_schema,trg_table)
                    self.check_hist_table('TARGET',trg_table_struct)
                    self.alter_table(src_table_struct,trg_table_struct)
                    self.check_period(src_table_struct,trg_table_struct)
                    src_index_struct = self.index_rebuild(src_index_struct)
                    trg_index_struct = self.index_rebuild(trg_index_struct)
                    self.check_index(src_index_struct,trg_index_struct)
                    self.print_cmd(trg_schema,trg_table)
                else:
                    # if the target table does not exist ( CREATE TABLE )
                    self.create_table(src_table_struct,trg_schema,trg_table)
            else:
                # If the sourcetable doesn't exist
                print(f"-- ?? Source-Table doesn't exist: DROP TABLE {src_schema}.{src_table} ??")

    # Target-Table exists ( Check COLUMNS )
    def alter_table(self,src_struct,trg_struct):
        source = Table(src_struct)
        target = Table(trg_struct)
        for src_feld in source.field_dict.keys():
            if src_feld in target.field_dict.keys():
                # COLUMN exists ( Check ATTRIBUTES )
                self.alter_column(src_feld,source.field_dict[src_feld],target.table_info['TABSCHEMA'],
                    target.table_info['TABNAME'],target.field_dict[src_feld])
            else:
                # COLUMN doesn't exist ( Add COLUMN )
                self.add_column(src_feld,source.field_dict[src_feld],target.table_info['TABSCHEMA'],
                    target.table_info['TABNAME'])
        pass

    # Erstellt aus den Feldern der Indexe eine Liste 
    def index_reduce(self,index):
        tmp:list = []
        for _ in index:
            tmp.append(_['column'])
        return tmp

    # Vergleicht die Indexe
    def check_index(self,left_index, right_index):
        # Alle Target-Indexe mit den Source-Indexen vergleichen
        for i_r in right_index:
            if i_r['column'] not in self.index_reduce(left_index):
                self.drop_idx(i_r)
        # Alle Source-Indexe mit den Target-Indexen vergleichen
        for i_l in left_index:
            if i_l['column'] not in self.index_reduce(right_index):
                self.build_idx(i_l,right_index)
    
    def index_rebuild(self,idx_records):
        index_list:list = []
        index:dict = {'table':'','index':'','rule':'','type':'','column':[],'order':[]}
        for idx_line in idx_records:
            if index['index'] != idx_line['INDSCHEMA']+'.'+idx_line['INDNAME']:
                if index['index'] != '':
                    index_list.append(index)
                index:dict = {'table':'','index':'','rule':'','type':'','column':[],'order':[]}
                index['table'] = idx_line['TABSCHEMA']+'.'+idx_line['TABNAME']
                index['index'] = idx_line['INDSCHEMA']+'.'+idx_line['INDNAME']
                index['rule'] = idx_line['UNIQUERULE']
                index['type'] = idx_line['INDEXTYPE']
                index['column'].append(idx_line['COLNAME'])
                index['order'].append(idx_line['COLORDER'])
            else:
                index['column'].append(idx_line['COLNAME'])
                index['order'].append(idx_line['COLORDER'])
        index_list.append(index)
        return index_list

    def build_idx(self,left_idx,right_idx):
        tmp_idx:dict = {'schema':'', 'table':''}
        for _ in right_idx:
            tmp_idx['schema'] = _['table'].split('.')[0]
            tmp_idx['table'] = _['table'].split('.')[1]
        left_idx['table'] = tmp_idx['schema']+'.'+tmp_idx['table']
        left_idx['index'] = tmp_idx['schema']+'.'+left_idx['index'].split('.')[1]
        text:str = ""
        if left_idx['rule'] == 'U':
            text += 'CREATE UNIQUE INDEX '+left_idx['index']
            text += ' ON '+left_idx['table']+' ('+','.join(left_idx['column'])
            text += ')'
        if left_idx['rule'] == 'P':
            text += 'ALTER TABLE '+left_idx['table']
            text += ' ADD CONSTRAINT '+left_idx['index'].split('.')[1]
            text += ' PRIMARY KEY ('+','.join(left_idx['column'])
            text += ')'
        if left_idx['type'] == 'CLUST':
            text += ' CLUSTER'
        text += ';'
        self.idx_list.append(text)

    def drop_idx(self,right_idx):
        if right_idx['index'] != '':
            text:str = ""
            text += 'DROP INDEX '+right_idx['index']+';'
            self.idx_list.append(text)

    # Check ATTRIBUTES ( ALTER COLUMN )
    def alter_column(self,src_feld,src_attribute,trg_schema,trg_table,trg_attribute):
        tmp:str = ""
        tmp += f"ALTER TABLE {trg_schema}.{trg_table} ALTER COLUMN {src_feld}"
        # Ckeck if the datatype between source and target is different
        if src_attribute['DATATYPE'] != trg_attribute['DATATYPE']:
            self.cmd_list.append(f"{tmp} SET DATA TYPE {src_attribute['DATATYPE']};")
        # Check if nullable-condition between source and target is different
        if src_attribute['NULLABLE'] != trg_attribute['NULLABLE']:
            if src_attribute['NULLABLE'] == True:
                self.cmd_list.append(f"{tmp} DROP NOT NULL;")
            else:
                self.cmd_list.append(f"{tmp} SET NOT NULL;")
        # Check if default between source and target is different
        if src_attribute['DEFAULT'] != trg_attribute['DEFAULT']:
            self.cmd_list.append(f"{tmp} SET {src_attribute['DEFAULT']};")
        # Check if generated between source and target is different
        if src_attribute['GENERATED'] != trg_attribute['GENERATED']:
            # Check if generated between source and target is different
            src_identity = src_attribute['IDENTITY']
            trg_identity = trg_attribute['IDENTITY']
            if src_identity['IDENTITY'] != 'N':
                if src_identity['IDENTITY'] != trg_identity['IDENTITY']:
                    self.cmd_list.append(f"{tmp} SET {src_attribute['GENERATED']} {src_identity['IDENTITY']};")
                    self.cmd_list.append(f"{tmp} RESTART WITH {src_identity['START']};")
                    self.cmd_list.append(f"{tmp} SET INCREMENT BY {src_identity['INCREMENT']};")
                    self.cmd_list.append(f"{tmp} SET {src_identity['MINVALUE']};")
                    self.cmd_list.append(f"{tmp} SET {src_identity['MAXVALUE']};")
                    self.cmd_list.append(f"{tmp} SET {src_identity['CACHE']};")
                    self.cmd_list.append(f"{tmp} SET {src_identity['ORDER']};")
                elif src_attribute['PERIOD'] != trg_attribute['PERIOD']:
                    pass
                else:
                    self.cmd_list.append(f"{tmp} SET {src_attribute['GENERATED']};")
            else:
                self.cmd_list.append(f"{tmp} DROP IDENTITY;")

    # generate COLUMN + ATTRIBUTES
    def add_column(self,src_feld,src_attribute,trg_schema,trg_table):
        tmp:str = ""
        tmp += f"ALTER TABLE {trg_schema}.{trg_table} ADD COLUMN {src_feld}"
        # add the datatype
        tmp += f" {src_attribute['DATATYPE']}"
        # add not null
        if src_attribute['NULLABLE'] == False:
            tmp += " NOT NULL"
        # add default
        if src_attribute['DEFAULT']:
            tmp += f" {src_attribute['DEFAULT']}"
        # add generated
        if src_attribute['GENERATED']:
            tmp += f" {src_attribute['GENERATED']}"
            # add identity
            src_identity = src_attribute['IDENTITY']
            if src_identity['IDENTITY'] != 'N':
                tmp += f" {src_identity['IDENTITY']}"
                tmp += f" (START WITH {src_identity['START']}"
                tmp += f" INCREMENT BY {src_identity['INCREMENT']}"
                tmp += f" {src_identity['MINVALUE']}"
                tmp += f" {src_identity['MAXVALUE']}"
                tmp += f" {src_identity['CACHE']}"
                tmp += f" {src_identity['ORDER']})"
            # add period 'SYSTEM_TIME' only
            if src_attribute['PERIOD'] != '':
                tmp += f" {src_attribute['PERIOD']}"
        self.cmd_list.append(f"{tmp};")

    # Table doesn't exist ( CREATE TABLE )
    def create_table(self,struct,schema,table):
        source = Table(struct)
        tmp:str  = ""
        count:int = len(source.field_dict.keys())
        self.cmd_list.append(f"CREATE TABLE {schema}.{table} (")
        for _, src_feld in enumerate(source.field_dict.keys(),1):
            tmp = f"   {src_feld}"
            tmp += f" {source.field_dict[src_feld]['DATATYPE']}"
            if source.field_dict[src_feld]['NULLABLE'] == False:
                tmp += " NOT NULL"
            if source.field_dict[src_feld]['DEFAULT']:
                tmp += f" {source.field_dict[src_feld]['DEFAULT']}"
            # add generated
            if source.field_dict[src_feld]['GENERATED']:
                tmp += f" {source.field_dict[src_feld]['GENERATED']}"
                # add identity
                src_identity = source.field_dict[src_feld]['IDENTITY']
                if src_identity['IDENTITY'] != 'N':
                    tmp += f" {src_identity['IDENTITY']}"
                    tmp += f" (START WITH {src_identity['START']}"
                    tmp += f" INCREMENT BY {src_identity['INCREMENT']}"
                    tmp += f" {src_identity['MINVALUE']}"
                    tmp += f" {src_identity['MAXVALUE']}"
                    tmp += f" {src_identity['CACHE']}"
                    tmp += f" {src_identity['ORDER']})"
                # add period 'SYSTEM_TIME' only
                if source.field_dict[src_feld]['PERIOD'] != '':
                    tmp += f" {source.field_dict[src_feld]['PERIOD']}"
            if _ < count:
                tmp += ","
            self.cmd_list.append(tmp)
        self.cmd_list.append(") IN <TBSPACE> INDEX IN <INDEX_TBSPACE> LONG IN <LONG_TBSPACE> ;")
        self.check_hist_table('SOURCE',struct)
        self.check_hist_table('TARGET',{'COLNAME':{'DUMMY':{'HISTORYTABNAME':''}}})
        self.check_period(struct,{'TEMPORALTYPE':'N'})

        src_index_struct = self.db.get_index_struct(self.source_database,struct['TABSCHEMA'],struct['TABNAME'])
        src_index_struct = self.index_rebuild(src_index_struct)
        trg_index:dict = {'table':'','index':'', 'rule':'', 'type':'', 'column':[], 'order':[]}
        trg_index['table'] = schema +'.'+ table
        trg_index_struct:list = []
        trg_index_struct.append(trg_index)
        self.check_index(src_index_struct,trg_index_struct)

        self.print_cmd(schema,table)
        pass

    # Check if the table has an historytable
    def check_hist_table(self,type,struct):
        column_section = struct['COLNAME']
        for feld in column_section.keys():
            if column_section[feld]['HISTORYTABNAME'] != None:
                self.history[type] = column_section[feld]['HISTORYTABNAME']

    
    def check_period(self,source,target):
        """ Period => 'N', 'A', 'B', 'S' 
            'N' 'N' = No action
            'A' 'A' = No action
            'S' 'S' = No action
            'B' 'B' = No action
            ----------------------------------------------
            'N' 'A' = DROP BUSINESS_TIME
            'S' 'B' = DROP BUSINESS_TIME
            ----------------------------------------------
            'N' 'S' = DROP SYSTEM_TIME
            'A' 'B' = DROP SYSTEM_TIME
            ----------------------------------------------
            'A' 'N' = ADD  BUSINESS_TIME
            'B' 'S' = ADD  BUSINESS_TIME
            ----------------------------------------------
            'S' 'N' = ADD  SYSTEM_TIME
            'B' 'A' = ADD  SYSTEM_TIME
            ----------------------------------------------
            'N' 'B' = DROP BUSINESS_TIME, DROP SYSTEM_TIME
            'B' 'N' = ADD  BUSINESS_TIME, ADD  SYSTEM_TIME
        """
        if source['TEMPORALTYPE'] ==   'N' and target['TEMPORALTYPE'] == 'A':
            #print("DROP BUSINESS_TIME")
            self.period['BUSINESS']['STATUS'] = 'D'
        if source['TEMPORALTYPE'] == 'S' and target['TEMPORALTYPE'] == 'B':
            #print("DROP BUSINESS_TIME")
            self.period['BUSINESS']['STATUS'] = 'D'
        if source['TEMPORALTYPE'] == 'N' and target['TEMPORALTYPE'] == 'S':
            #print("DROP SYSTEM_TIME")
            self.period['SYSTEM']['STATUS'] = 'D'
        if source['TEMPORALTYPE'] == 'A' and target['TEMPORALTYPE'] == 'B':
            #print("DROP SYSTEM_TIME")
            self.period['SYSTEM']['STATUS'] = 'D'
        if source['TEMPORALTYPE'] == 'N' and target['TEMPORALTYPE'] == 'B':
            #print("DROP SYSTEM_TIME, DROP BUSINESS_TIME")
            self.period['SYSTEM']['STATUS'] = 'D'
            self.period['BUSINESS']['STATUS'] = 'D'
        #            
        if source['TEMPORALTYPE'] == 'A' and target['TEMPORALTYPE'] == 'N':
            #print("ADD BUSINESS_TIME")
            self.period['BUSINESS']['STATUS'] = 'A'
            self.check_period_fields('B',source['COLNAME'])
        if source['TEMPORALTYPE'] == 'B' and target['TEMPORALTYPE'] == 'S':
            #print("ADD BUSINESS_TIME")
            self.period['BUSINESS']['STATUS'] = 'A'
            self.check_period_fields('B',source['COLNAME'])
        if source['TEMPORALTYPE'] == 'S' and target['TEMPORALTYPE'] == 'N':
            #print("ADD SYSTEM_TIME")
            self.period['SYSTEM']['STATUS'] = 'A'
            self.check_period_fields('S',source['COLNAME'])
        if source['TEMPORALTYPE'] == 'B' and target['TEMPORALTYPE'] == 'A':
            #print("ADD SYSTEM_TIME")
            self.period['SYSTEM']['STATUS'] = 'A'
            self.check_period_fields('S',source['COLNAME'])
        if source['TEMPORALTYPE'] == 'B' and target['TEMPORALTYPE'] == 'N':
            #print("ADD SYSTEM_TIME, ADD BUSINESS_TIME")
            self.period['SYSTEM']['STATUS'] = 'A'
            self.period['BUSINESS']['STATUS'] = 'A'
            self.check_period_fields('S',source['COLNAME'])
            self.check_period_fields('B',source['COLNAME'])


    def check_period_fields(self,type,source):
        if type == 'B':
            for column in source.keys():
                if source[column]['PERIODTYPE'] == 'A':
                    self.period['BUSINESS']['VON'] = source[column]['BEGINCOLNAME']
                    self.period['BUSINESS']['BIS'] = source[column]['ENDCOLNAME']
        if type == 'S':
            for column in source.keys():
                if source[column]['PERIODTYPE'] == 'S':
                    self.period['SYSTEM']['VON'] = source[column]['BEGINCOLNAME']
                    self.period['SYSTEM']['BIS'] = source[column]['ENDCOLNAME']

        pass

    # Print the SQL-Statements for the Target-Table
    def print_cmd(self,schema,table):
        flag:bool = False
        if self.cmd_list:
            cora:str = 'C' if 'CREATE' in self.cmd_list[0] else 'A'
        for _, cmd in enumerate(self.cmd_list):
            flag = True
            if _ % 3 == 0 and _ != 0 and cora == 'A':
                print(f"CALL SYSPROC.ADMIN_CMD('REORG TABLE {schema}.{table}');")
            print(cmd)
        if flag == True and cora == 'A':
            print(f"CALL SYSPROC.ADMIN_CMD('REORG TABLE {schema}.{table}');")
        print("          ")
        if self.period['SYSTEM']['STATUS'] == 'A':
            print(f"ALTER TABLE {schema}.{table} ADD PERIOD SYSTEM_TIME ({self.period['SYSTEM']['VON']},{self.period['SYSTEM']['BIS']});")
        if self.period['SYSTEM']['STATUS'] == 'D':
            print(f"ALTER TABLE {schema}.{table} DROP PERIOD SYSTEM_TIME;")
        if self.period['BUSINESS']['STATUS'] == 'A':
            print(f"ALTER TABLE {schema}.{table} ADD PERIOD BUSINESS_TIME ({self.period['BUSINESS']['VON']},{self.period['BUSINESS']['BIS']});")
        if self.period['BUSINESS']['STATUS'] == 'D':
            print(f"ALTER TABLE {schema}.{table} DROP PERIOD BUSINESS_TIME;")
        if self.period['SYSTEM']['STATUS'] in ['A','D'] or self.period['BUSINESS']['STATUS'] in ['A','D']:
            print(f"CALL SYSPROC.ADMIN_CMD('REORG TABLE {schema}.{table}');")
        print("          ")
        if self.history['SOURCE']:
            if not self.history['TARGET']:
                print(f"CREATE TABLE {schema}.{self.history['SOURCE']} LIKE {schema}.{table};")
                print(f"ALTER TABLE {schema}.{table} ADD VERSIONING USE HISTORY TABLE {schema}.{self.history['SOURCE']};")
                print(f"CALL SYSPROC.ADMIN_CMD('REORG TABLE {schema}.{table}');")
        print("          ")
        for idx in self.idx_list:
            print(idx)
        print("--------------------------------------------------------------------------------")
        self.idx_list:list = []
        self.cmd_list:list = []
        self.history = {'SOURCE':'', 'TARGET':''}

root = tk.Tk()
root.withdraw()



