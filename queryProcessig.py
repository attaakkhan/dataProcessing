#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
from Assignment1 import RROBIN_TABLE_PREFIX, RANGE_TABLE_PREFIX
def isbetween(i,j,k):
    if i>=j and i<=k:
        return True
    return False
def isbetween1(i,j,k):
    if i>j and i<=k:
        return True
    return False
def have(list,k):
    for i in list:
        if i==k:
            return True
    return False
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    cur = openconnection.cursor()
    cur.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' and table_name like '{0}%'".format(
            RANGE_TABLE_PREFIX.lower() ))

    min =0
    max=5
    fileList=[]
    noOfPartitions = cur.fetchone()[0]
    step=5.0/(float)(noOfPartitions)
    i=0
    list=[]
    while min<max:
        lowerLimit = min
        upperLimit = min + step
        if i == 0:
            if isbetween(ratingMinValue,lowerLimit,upperLimit):
                list.append(i)
        else:
            if isbetween1(ratingMinValue,lowerLimit,upperLimit) or isbetween1(ratingMaxValue,lowerLimit,upperLimit)or (ratingMinValue<lowerLimit and ratingMaxValue>=upperLimit):
                list.append(i)
        min = upperLimit
        i=i+1

    print list

    rangeMap = {}
    if len(list)==1:
        cur.execute("select * from {0} where rating >= {1} and rating <= {2}".format( RANGE_TABLE_PREFIX+str(list[0]),ratingMinValue,ratingMaxValue ))
        rows = cur.fetchall()
        rangeMap[RANGE_TABLE_PREFIX+str(list[0])]=rows

    else:

        index=0
        for part in list:
            if index==0:
                cur.execute("select * from {0} where rating >= {1}".format(RANGE_TABLE_PREFIX + str(list[index]),
                                                                                     ratingMinValue, ratingMaxValue))
                rows = cur.fetchall()
                rangeMap[RANGE_TABLE_PREFIX + str(index)] = rows

            elif index == len(list)-1:
                cur.execute("select * from {0} where rating <= {1}".format(RANGE_TABLE_PREFIX + str(list[index]),
                                                                            ratingMaxValue))
                rows = cur.fetchall()
                rangeMap[RANGE_TABLE_PREFIX + str(index)] = rows
            else:
                cur.execute("select * from {0}".format(RANGE_TABLE_PREFIX + str(list[index])))
                rows = cur.fetchall()
                rangeMap[RANGE_TABLE_PREFIX + str(index)] = rows
            index=index+1

    cur.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' and table_name like '{0}%'".format(    RROBIN_TABLE_PREFIX.lower()))


    noOfPartitions2 = cur.fetchone()[0]
    roundMap={}
    for k in range(noOfPartitions2):
        cur.execute("select * from {0} where rating >= {1} and rating <= {2}".format(RROBIN_TABLE_PREFIX + str(k),ratingMinValue, ratingMaxValue))
        rows = cur.fetchall()
        roundMap[RROBIN_TABLE_PREFIX + str(k)] = rows

    fileList.append(['PartitionName','UserID','MovieID','Rating'])
    rindex=0
    for r in range(noOfPartitions):
        tup=rangeMap.get(RANGE_TABLE_PREFIX+str(rindex))
        if tup!=None and len(tup)>0:
            rlist=[]
            for rr in tup:
                rlist = []
                rlist.append(RANGE_TABLE_PREFIX+str(rindex))
                rlist.append(rr[0])
                rlist.append(rr[1])
                rlist.append(rr[2])
                fileList.append(rlist)
        rindex=rindex+1
    rindex = 0
    for r in range(noOfPartitions2):
        tup = roundMap.get(RROBIN_TABLE_PREFIX + str(rindex))
        if tup != None and len(tup) > 0:

            for rr in tup:
                rlist = []
                rlist.append(RROBIN_TABLE_PREFIX + str(rindex))
                rlist.append(rr[0])
                rlist.append(rr[1])
                rlist.append(rr[2])
                fileList.append(rlist)
        rindex = rindex + 1
    writeToFile('RangeQueryOut.txt',fileList)


























def PointQuery(ratingsTableName, ratingValue, openconnection):
    cur = openconnection.cursor()
    cur.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' and table_name like '{0}%'".format(
            RANGE_TABLE_PREFIX.lower()))
    noOfPartitions = cur.fetchone()[0]

    step=5.0/(float)(noOfPartitions)
    min=0
    max=5
    ii=0
    found=False

    while min<max:
        lower=min
        upper=min+step
        if ii == 0:

              if isbetween(ratingValue, lower, upper):
                found= True
                break
        else:
            if isbetween1(ratingValue, lower, upper):
                found=True
                break
        min = upper
        ii = ii + 1
    rangeMap = {}
    if found:
        cur.execute("select * from {0} where rating = {1}".format(RANGE_TABLE_PREFIX + str(ii),
                                                                                      ratingValue))
        rows = cur.fetchall()
        rangeMap[RANGE_TABLE_PREFIX + str(ii)] = rows

    cur.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' and table_name like '{0}%'".format(
            RROBIN_TABLE_PREFIX.lower()))

    noOfPartitions2 = cur.fetchone()[0]

    roundMap = {}
    for k in range(noOfPartitions2):
        cur.execute("select * from {0} where rating ={1}".format(RROBIN_TABLE_PREFIX + str(k),
                                                                                      ratingValue))
        rows = cur.fetchall()
        roundMap[RROBIN_TABLE_PREFIX + str(k)] = rows
    fileList=[]
    fileList.append(['PartitionName', 'UserID', 'MovieID', 'Rating'])
    rindex = 0
    for r in range(noOfPartitions):
        tup = rangeMap.get(RANGE_TABLE_PREFIX + str(rindex))
        if tup != None and len(tup) > 0:
            rlist = []
            for rr in tup:
                rlist = []
                rlist.append(RANGE_TABLE_PREFIX + str(rindex))
                rlist.append(rr[0])
                rlist.append(rr[1])
                rlist.append(rr[2])
                fileList.append(rlist)
        rindex = rindex + 1
    rindex = 0
    for r in range(noOfPartitions2):
        tup = roundMap.get(RROBIN_TABLE_PREFIX + str(rindex))
        if tup != None and len(tup) > 0:

            for rr in tup:
                rlist = []
                rlist.append(RROBIN_TABLE_PREFIX + str(rindex))
                rlist.append(rr[0])
                rlist.append(rr[1])
                rlist.append(rr[2])
                fileList.append(rlist)
        rindex = rindex + 1
    writeToFile('PointQueryOut.txt' ,fileList)








def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()
