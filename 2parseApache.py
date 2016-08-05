# -*- coding: utf-8 -*-
"""
Created on Thu Aug  4 10:45:45 2016

@author: gbatchelor
"""

import sys
import pymysql
import datetime
from datetime import date, timedelta
import os
try:
    import urllib.request as urllib2
except ImportError:
    import urllib2
import codecs



dateToStart   = str(sys.argv[2])
password      = str(sys.argv[3])
irctype       = sys.argv[4]

dateS = datetime.datetime(int(dateToStart[0:4]),int(dateToStart[4:-2]),int(dateToStart[6:]))
activemqCutOffDate=datetime.datetime(2015,4,28,0,0,0,0)

# a long if statment that changes which irc data is stored based upon the 4th
# argument of the commnad line 

if irctype == 'activemq':
    datasource_id = str(sys.argv[1])
    newDS   = int(datasource_id)
    forge_id = '36'
    if dateS > activemqCutOffDate:
        urlstem = "http://irclogs.dankulp.com/logs/irclogger_logs/apache-activemq"
    else:
        urlstem="http://irclogs.dankulp.com/logs/irclogger_logs/activemq"
    if not os.path.exists(datasource_id):
        os.makedirs(datasource_id)
elif irctype == 'aries':
    datasource_id = str(sys.argv[1])
    newDS   = int(datasource_id)
    forge_id = '38'
    urlstem = "http://irclogs.dankulp.com/logs/irclogger_logs/apache-aries"
    if not os.path.exists(datasource_id):
        os.makedirs(datasource_id)
elif irctype == 'camel':
    datasource_id = str(sys.argv[1])
    newDS   = int(datasource_id)
    forge_id = '34'
    urlstem = "http://irclogs.dankulp.com/logs/irclogger_logs/camel"
    if not os.path.exists(datasource_id):
        os.makedirs(datasource_id)
elif irctype == 'cxf':
    datasource_id = str(sys.argv[1])
    newDS   = int(datasource_id)
    forge_id = '37'
    urlstem = "http://irclogs.dankulp.com/logs/irclogger_logs/cxf"
    if not os.path.exists(datasource_id):
        os.makedirs(datasource_id)
elif irctype == 'kalumet':
    datasource_id = str(sys.argv[1])
    newDS   = int(datasource_id)
    forge_id = '39'
    urlstem = "http://irclogs.dankulp.com/logs/irclogger_logs/kalumet"
    if not os.path.exists(datasource_id):
        os.makedirs(datasource_id)
elif irctype == 'karaf':
    datasource_id = str(sys.argv[1])
    newDS   = int(datasource_id)
    forge_id = '40'
    urlstem = "http://irclogs.dankulp.com/logs/irclogger_logs/karaf"
    if not os.path.exists(datasource_id):
        os.makedirs(datasource_id)
elif irctype == 'servicemix':
    datasource_id = str(sys.argv[1])
    newDS   = int(datasource_id)
    forge_id = '41'
    urlstem = "http://irclogs.dankulp.com/logs/irclogger_logs/servicemix"
    if not os.path.exists(datasource_id):
        os.makedirs(datasource_id)
else:
    print("invalid irc type, must be one of openstack, dev, infra, meeting, meeting-alt, meeting-3, or dns")



if datasource_id and dateToStart:
    try:
        db1 = pymysql.connect(host='grid6.cs.elon.edu',
                                  database='ossmole_merged',
                                  user='megan',
                                  password=password,
                                  use_unicode=True,
                                  charset='utf8')

    except pymysql.Error as err:
        print(err)

    try:
         db2 = pymysql.connect(host='flossdata.syr.edu',
                                  database='ossmole_merged',
                                  user='megan',
                                  password=password,
                                  use_unicode=True,
                                  charset='utf8')
    except pymysql.Error as err:
        print(err)

    cursor1 = db1.cursor()
    cursor2 = db2.cursor()

    # make directory and save file
    os.mkdir(datasource_id)

    # get yesterday's date
    yesterday = datetime.datetime.now() - timedelta(days = 1)
    print ("yesterday's date is:",yesterday)
    dateS = datetime.datetime(int(dateToStart[0:4]),int(dateToStart[4:-2]),int(dateToStart[6:]))

    while(dateS <= yesterday):
        print("working on ...")
        print(dateS)

        # get yyyy, mm, dd and put into URL
        # get yyyy, mm, dd and put into URL
        yyyy = dateS.year
        mm   = dateS.month
        dd   = dateS.day

        # put leading zeroes on mm and dd
        if (mm < 10):
            mm = "0" + str(mm)
        if (dd < 10):
            dd = "0" + str(dd)


        # get file
        # Log URLs are in this format:
        # http://irclogs.dankulp.com/logs/irclogger_log/apache-activemq?date=2011-11-28&raw=on
        urlFile = "?date=" + str(yyyy) + "-" + str(mm) + "-" + str(dd) + "&raw=on";
        fullURL = urlstem + urlFile
        print ("getting URL", fullURL)

        try:
            html = urllib2.urlopen(fullURL).read()
        except urllib2.HTTPError as error:
            print(error)
        else:
            fileLoc = datasource_id + "/" + str(yyyy) + str(mm) + str(dd)
            outfile = codecs.open(fileLoc,'w')
            outfile.write(str(html))
            outfile.close()

        insertQuery="INSERT INTO `datasources`(`datasource_id`,\
                `forge_id`,\
                `friendly_name`,\
                `date_donated`,\
                `contact_person`,\
                `comments`,\
                `start_date`,\
                `end_date`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
        #======
        # LOCAL
        #======
        try:
            cursor1.execute(insertQuery,(newDS,
                    forge_id,
                    'ActiveMQ IRC '+ str(yyyy) + str(mm) + str(dd),
                    datetime.datetime.now(),
                    'msquire@elon.edu',
                    fileLoc,
                    datetime.datetime.now(),
                    datetime.datetime.now()))
        except pymysql.Error as error:
            print(error)
            db1.rollback()
        #======
        # REMOTE
        #======
        try:
            cursor2.execute(insertQuery,(newDS,
                    forge_id,
                    'ActiveMQ IRC '+ str(yyyy) + str(mm) + str(dd),
                    datetime.datetime.now(),
                    'msquire@elon.edu',
                    fileLoc,
                    datetime.datetime.now(),
                    datetime.datetime.now()))
        except pymysql.Error as error:
            print(error)
            db2.rollback()

        #increment date by one
        dateS  = dateS + timedelta(days=1)
        newDS += 1
    cursor1.close()
    cursor2.close()
    db1.close()
    db2.close()
else:
	print ("You need both a datasource_id and a date to start on your commandline.")
