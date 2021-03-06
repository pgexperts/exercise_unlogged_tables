# -*- coding: utf-8 *-*
#/usr/bin/python

import psycopg2
import random
import subprocess
from optparse import OptionParser

parser = OptionParser()

parser.add_option('-t', "--total", action="store", type="int", dest="total", default=5000)
parser.add_option('-r', "--rows", action="store", type="int", dest="rows", default=1000)

(options, args) = parser.parse_args()

conn = psycopg2.connect("dbname=unlogged_test user=jeff")
cur = conn.cursor()
conn.set_session(autocommit=True)
cur.execute("SET synchronous_commit = 'off'")

for tablenum in xrange(options.total):
    table = '"unlogged_tbl_' + str(tablenum) + '"'
    SQL = "CREATE UNLOGGED TABLE " + table + "(foo text);"
    print SQL
    cur.execute(SQL)
    #conn.commit()
    noisewords = subprocess.Popen(["/home/jeff/git/noisewords/noisewords.py", "--rows=%d" % (options.rows) ], stdout=subprocess.PIPE)
    #copy_text = "COPY " + table + "FROM stdin"
    #cur.copy_expert(copy_text, noisewords.stdout)
    for noiseline in noisewords.stdout:
        insert_sql = "INSERT INTO " + table + " VALUES ('%s')" % noiseline
        #print insert_sql
        cur.execute(insert_sql)
    #conn.commit()
    vacuum_sql = "VACUUM ANALYZE " + table
    print vacuum_sql
    cur.execute(vacuum_sql)

for tablenum in xrange(options.total):
    table = '"unlogged_tbl_' + str(tablenum) + '"'
    SQL = "SELECT foo FROM" + table
    print SQL
    cur.execute(SQL)
    throwaway = cur.fetchone()[0]

cur.close()
conn.close()
