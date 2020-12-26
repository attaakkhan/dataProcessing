#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2
from testHelper import RANGE_TABLE_PREFIX
from testHelper import RROBIN_TABLE_PREFIX
from testHelper import RATING_COLNAME


def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

















def deleteAllPublicTablesWithPrefix(openconnection,prefix):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' and table_name like '{0}%'".format(prefix))
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()
def loadRatings(ratingstablename, ratingsfilepath, openconnection):
	cur=openconnection.cursor()
	if not checkTableExists(openconnection,ratingstablename):
	       # print "CREATE TABLE {0} (userid int, movieid int,rating float) ;".format(ratingstablename)
	        cur.execute("CREATE TABLE {0} (userid int, movieid int,rating float);".format(ratingstablename))
		#print "Table created:",ratingstablename 
		a=open('test_data.txt')
		l=a.readline()
		listToInsert=[]
		query='insert into {0} values {1}.'.format(ratingstablename,'{1}')
		#print query
		batchSize=100000;
		count=0;
		i=1
		while l:
			#print l
			#print "asdfas"
			row=l.split('::')
			#print len(row)
			if len(row)<4:
				break;
			val='({0},{1},{2})'.format(row[0],row[1],row[2])
			
		#	print val
			listToInsert.append(val)
			if len(listToInsert)==batchSize:
			 	str=''
				for x in listToInsert:
					str=str+x+','
				str=str[0:-1]
				print 'inserting Batch:{0} of size:{1}'.format(i,batchSize) 
				cur.execute('insert into {0} values {1}'.format(ratingstablename,str))
				i=i+1				
				listToInsert=[]
			l=a.readline()
		str=''
		#print len(listToInsert)
		for x in listToInsert:
			str=str+x+','
		str=str[0:-1]
		print 'inserting Batch:{0} of size:{1}'.format(i,len(listToInsert)) 
		cur.execute('insert into {0} values {1}'.format(ratingstablename,str))
		listToInsert=[]
							
			
		
	print 'copied'
def insertIntoTable(tbName, list,con):
    str='{0}{1}'
    #print list
    bs=100000
    count=0
    j=1
    cur=con.cursor();
    for i in list:
	if count ==bs:
		str=str[0:-7]
	#	print 'inserting Batch:{0} of size:{1}'.format(j,bs) 
		cur.execute('insert into {0} values {1}'.format(tbName,str))
		j=j+1
		str='{0}{1}'
		count=0
	
        str=str.format(i,',{0}{1}')
	count=count+1
    str=str[0:-7]
   #print str
    
    #print 'inserting to',tbName
   # print 'inserting Last Batch:{0}'.format(j) 
    cur.execute('insert into {0} values {1}'.format(tbName,str))
def rangePartition(ratingstablename, numberofpartitions, openconnection):
    interval=5.0/numberofpartitions
    cur=openconnection.cursor()
    #print '11'
    cur.execute('select * from {0} where {1} >= {2} and {1} <= {3}'.format(ratingstablename,RATING_COLNAME,0,interval))
    #print '111'
    rows = cur.fetchall()
    #print '12'
    cur.execute('CREATE TABLE {0}{1} (userid int, movieid int,rating float)'.format(RANGE_TABLE_PREFIX,0,RATING_COLNAME,0,interval,ratingstablename))
    #print '13'
    insertIntoTable('{0}{1}'.format(RANGE_TABLE_PREFIX, 0), rows, openconnection)
    #print '14'
    lowerbound = interval

    for i in range (1, numberofpartitions):
	#print i
        rows=[]
        cur.execute(
            'select * from {0} where {1} > {2} and {1} <= {3}'.format(ratingstablename, RATING_COLNAME, lowerbound,
                                                                                          lowerbound + interval ))
        rows = cur.fetchall()
        cur.execute('CREATE TABLE {0}{1} (userid int, movieid int,rating float)'.format(RANGE_TABLE_PREFIX, i,
                                                                                                  RATING_COLNAME, interval,
                                                                                                  interval+lowerbound,
                                                                                                  ratingstablename))
        insertIntoTable('{0}{1}'.format(RANGE_TABLE_PREFIX, i), rows, openconnection)
        lowerbound += interval


    #print '444466666'
  #  query='CREATE TABLE {0} (CHECK({2} >={3} and {2}<={4})) INHERITS({01});
   # ratingstablenam + ' ' + RANGE_TABLE_PREFIX
    #pass
#CREATE TABLE ratings rang (CHECK(rating >=0 and rating<=2)) INHERITS(ratings);

def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    cur=openconnection.cursor()
    for i in range(0, numberofpartitions):
        cur.execute(
            "select userid,movieid,{3} from (select *, row_number() over () from {0}) as temp where (row_number-1)%{1}= {2}".format(
                ratingstablename, numberofpartitions, i,RATING_COLNAME))
        rows=cur.fetchall()
        cur.execute(
            'CREATE TABLE {0}{1} (userid int, movieid int,rating float)'.format(RROBIN_TABLE_PREFIX, i))

        insertIntoTable('{0}{1}'.format(RROBIN_TABLE_PREFIX,i),rows,openconnection)



def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    dbcur = openconnection.cursor()
    query = ''
    dbcur.execute(
        "SELECT count(*) FROM {0}".format(ratingstablename))
    rowValue = dbcur.fetchone()[0]
    dbcur.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' and table_name like '{0}%'".format(
            RROBIN_TABLE_PREFIX))
    partitionNum = dbcur.fetchone()[0]
    dbcur.execute('insert into {0}{1} values ({2},{3},{4})'.format(RROBIN_TABLE_PREFIX,rowValue%partitionNum,userid,itemid,rating))



def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    dbcur = openconnection.cursor()
    query=''
    dbcur.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' and table_name like '{0}%'".format(
            RANGE_TABLE_PREFIX))
    partitonNum=dbcur.fetchone()[0]
    #print partitonNum
    interval=5.0/partitonNum
    lowerbound=interval
    if rating>=0 and rating<=interval:
        dbcur.execute('insert into {0}{1} values ({2},{3},{4});'.format(RANGE_TABLE_PREFIX, 0, userid, itemid, rating))
        return

    for i in range(1,partitonNum):
        if rating>lowerbound and rating<=interval+lowerbound:
            dbcur.execute('insert into {0}{1} values ({2},{3},{4});'.format(RANGE_TABLE_PREFIX,i,userid,itemid,rating))
            return
        lowerbound += interval




def checkTableExists(dbcon, tablename):
    dbcur = dbcon.cursor()
    dbcur.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{0}'
        """.format(tablename.replace('\'', '\'\'')))
    if dbcur.fetchone()[0] == 1:
        dbcur.close()
        return True

    dbcur.close()
    return False



























def createDB(dbname='dds_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    finally:
        if cursor:
            cursor.close()
