#!/usr/bin/env python3

import tools

# --------------------------------------------------------------------------------
# VERSION 1 by Manfred Wagner
# --------------------------------------------------------------------------------
# BESCHREIBUNG:
# Dieses Script dient als Vorlage und sollte vor Gebrauch umkopiert werden.
# Nach dem das Script mit einem anderen Namen umkopiert wurde, kann das
# kopierte Script geändert werden.
#
# SELECT 'line.execute(src_con,"'||strip(tabschema)||'","'||strip(tabname)||'",trg_con,"{targetSchema}","'||strip(tabname)||'")'
# FROM syscat.tables
# WHERE tabschema = {sourceschema}
# AND tabname NOT LIKE 'TH%'
# --------------------------------------------------------------------------------

# --------------------------------------------------------------------------------
# Default-Werte: Nicht Ändern
src = tools.Db()
trg = tools.Db()
line = tools.Compare()
# --------------------------------------------------------------------------------

# --------------------------------------------------------------------------------
# 1. Parameter für die Datenbank (mit SSL-Verschlüsselung, mit Session-User)
#    src_con=src_connect('SERVER','PORT','DATENBANK','SSL-PFAD','SSL-KEY','SSL-STASH','USER','SESSION_USER')
#    trg_con=trg_connect('SERVER','PORT','DATENBANK','SSL-PFAD','SSL-KEY','SSL-STASH','USER','SESSION_USER')
#
# 2. Parameter für die Datenbank (mit SSL-Verschlüsselung, ohne Session-User)
#    src_con=src_connect('SERVER','PORT','DATENBANK','SSL-PFAD','SSL-KEY','SSL-STASH','USER','')
#    trg_con=trg_connect('SERVER','PORT','DATENBANK','SSL-PFAD','SSL-KEY','SSL-STASH','USER','')
#
# 3. Parameter für die Datenbank (ohne SSL-Verschlüsselung, mit Session-User)
#    src_con=src_connect('SERVER','PORT','DATENBANK','','','','USER','SESSION_USER')
#    trg_con=trg_connect('SERVER','PORT','DATENBANK','','','','USER','SESSION_USER')
#
# 4. Parameter für die Datenbank (ohne SSL-Verschlüsselung, ohne Session-User)
#    src_con=src_connect('SERVER','PORT','DATENBANK','','','','USER','')
#    trg_con=trg_connect('SERVER','PORT','DATENBANK','','','','USER','')
#
src_con = src.connect('server',50000,'database','','','','conuser','sesuser')
trg_con = trg.connect('server',50000,'database','','','','conuser','sesuser')
# --------------------------------------------------------------------------------

# --------------------------------------------------------------------------------
# 1. Parameter für das Compare einzelner Tabellen
#    line.execute(src_con,'QUELL-SCHEMA','QUELL-TABELLE',trg_con,'ZIEL-SCHEMA','ZIEL-TABELLE')
#
# 2. Parameter für das Compare eines ganzen Schema
#    line.execute(src_con,'QUELL-SCHEMA','*',trg_con,'ZIEL-SCHEMA','*')
#
# SCHEMA
#line.execute(src_con,'SSRC','*',trg_con,'STRG','*')
# TABLE
#line.execute(src_con,'SSRC','T_SOURCE',trg_con,'STRG','T_SOURCE')
# 
line.execute(src_con,'source_schema','*',trg_con,'target_schema','*')
# --------------------------------------------------------------------------------
