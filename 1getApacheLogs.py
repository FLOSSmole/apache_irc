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
import re

datasource_id = str(sys.argv[1])
dateToStart   = str(sys.argv[2])
password      = str(sys.argv[3])
irctype       = sys.argv[4]

dateS = datetime.datetime(int(dateToStart[0:4]), int(dateToStart[4:-2]),
                          int(dateToStart[6:]))

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

    # get start date from command line, cutoffdate and urlstem from web
    dateS = datetime.datetime(int(dateToStart[0:4]), int(dateToStart[4:-2]),
                              int(dateToStart[6:]))

    newDS = int(datasource_id)

    selectQuery = "SELECT `forge_id`,`forge_home_page`\
            FROM `forges`\
            WHERE `forge_long_name` LIKE '%apache "+irctype+"%'"

    cursor1.execute(selectQuery)
    rows = cursor1.fetchall()

    forge_id = rows[0][0]
    menuURL = rows[0][1]

    try:
        longhtml = urllib2.urlopen(menuURL).read()
    except urllib2.HTTPError as error:
        print(error)

    html2 = longhtml[0:2000]
    html2 = bytes.decode(html2)

    menuHTML = re.search('date=(..........)', html2)
    reUrlStem = re.search("alternate forms: <a href=\\\'(.*?)\?", html2)

    if menuHTML and reUrlStem:
        menuHTMLGroup = menuHTML.group(1)
        urlStem = reUrlStem.group(1)
        urlStem = 'http://irclogs.dankulp.com'+urlStem
        cutOffDate = datetime.datetime.strptime(str(menuHTMLGroup), '%Y-%m-%d')

    while(dateS <= cutOffDate):
        print("working on ...")
        print(dateS)
        dateString = str(dateS)
        trunDateString = dateString[0:10]
        dayOfTheWeek = datetime.datetime.strptime(trunDateString, '%Y-%m-%d').strftime('%a')

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
        urlFile = "?date=" + str(yyyy) + "-" + str(mm) + "-" + str(dd) + "," +\
            dayOfTheWeek + "&raw=on"
        fullURL = urlStem + urlFile
        print("getting URL", fullURL)

        try:
            html = urllib2.urlopen(fullURL).read()
        except urllib2.HTTPError as error:
            print(error)
        else:
            fileLoc = datasource_id + "/" + str(yyyy) + str(mm) + str(dd)
            outfile = codecs.open(fileLoc, 'w')
            outfile.write(str(html))
            outfile.close()

        insertQuery = "INSERT INTO `datasources`(`datasource_id`,\
                `forge_id`,\
                `friendly_name`,\
                `date_donated`,\
                `contact_person`,\
                `comments`,\
                `start_date`,\
                `end_date`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
        # ======
        # LOCAL
        # ======
        try:
            cursor1.execute(insertQuery, (newDS,
                            forge_id,
                            'ActiveMQ IRC ' + str(yyyy) + str(mm) + str(dd),
                            datetime.datetime.now(),
                            'msquire@elon.edu',
                            fileLoc,
                            datetime.datetime.now(),
                            datetime.datetime.now()))
        except pymysql.Error as error:
            print(error)
            db1.rollback()
        # ======
        # REMOTE
        # ======
        try:
            cursor2.execute(insertQuery, (newDS,
                            forge_id,
                            'ActiveMQ IRC ' + str(yyyy) + str(mm) + str(dd),
                            datetime.datetime.now(),
                            'msquire@elon.edu',
                            fileLoc,
                            datetime.datetime.now(),
                            datetime.datetime.now()))
        except pymysql.Error as error:
            print(error)
            db2.rollback()

        # increment date by one
        dateS = dateS + timedelta(days=1)
        newDS += 1
    cursor1.close()
    cursor2.close()
    db1.close()
    db2.close()
else:
	print("You need both a datasource_id and a date to start on your\
             commandline.")
