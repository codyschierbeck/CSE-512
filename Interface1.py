import psycopg2
import os
import sys


def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    with open(ratingsfilepath,"r") as ratings_file:
            ratings_file  = open(ratingsfilepath, "r")
            create_table_query = "CREATE TABLE " + ratingstablename + """
            (userid INTEGER NOT NULL,
            movieid INTEGER NOT NULL,
            rating REAL NOT NULL,
            PRIMARY KEY(userid,movieid)
            );"""
    
            with openconnection.cursor() as cursor:
                cursor.execute(create_table_query)
                query = "INSERT INTO " + ratingstablename + """VALUES (%d, %d, %f) """
                lines = ratings_file.read().split()
                for line in lines:
                    split = line.split("::")
                    tup = (split[0],split[1],split[2])
                    cursor.execute(query,tup)
                
                

    

    pass # Remove this once you are done with implementation
    


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    with openconnection.cursor() as cursor:
        select_ratings = "SELECT rating FROM %s" %ratingstablename
        cursor.execute(select_ratings)
    pass # Remove this once you are done with implementation


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    pass # Remove this once you are done with implementation


def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    pass # Remove this once you are done with implementation


def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    pass # Remove this once you are done with implementation


def rangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    pass #Remove this once you are done with implementation


def pointQuery(ratingValue, openconnection, outputPath):
    pass # Remove this once you are done with implementation


def createDB(dbname='dds_assignment1'):
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
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()

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
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()
connection = getOpenConnection()
loadRatings("table","test_data1.txt",connection)
print("Done")
