# -*- coding: utf-8 -*-
"""
This program is free software; you can redistribute it
and/or modify it under the GPLv2

Copyright (C) 2004-2016 Megan Squire <msquire@elon.edu>
Contributions from:
Evan Ashwell - converted from perl to python
Greg Batchelor - made code dymanic between multiple irc logs

We're working on this at http://flossmole.org - Come help us build
an open and accessible repository for data and analyses for open
source projects.

If you use this code or data for preparing an academic paper please
provide a citation to

Howison, J., Conklin, M., & Crowston, K. (2006). FLOSSmole:
A collaborative repository for FLOSS research data and analyses.
International Journal of Information Technology & Web Engineering, 1(3), 17–26.

and

FLOSSmole (2004-2016) FLOSSmole: a project to provide academic access to data
and analyses of open source projects.  Available at http://flossmole.org
################################################################
usage:
> python3 1getApacheLogs.py <datasource_id> <date> <password> <irctype>

example usage:
> python3 1getApacheLogs.py  20150530 password KFI2

irctype choices as of August 24, 2016:
cxi2 (CXF v2)
mqi2 (ActiveMQ v2)
KFI2 (Karaf v2)
SMI (ServiceMix)
SMI2 (ServiceMix v2)

purpose:
collect apache irc data from web and put key components inside of a database

each date = one datasource_id
each line is incremented with a number for that date
################################################################
"""

import sys
import pymysql
from datetime import datetime
from datetime import timedelta
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
irctype       = str(sys.argv[4])

dateS = datetime(int(dateToStart[0:4]),
                 int(dateToStart[4:-2]),
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

    os.mkdir(datasource_id)

    dateS = datetime(int(dateToStart[0:4]),
                     int(dateToStart[4:-2]),
                     int(dateToStart[6:]))

    newDS = int(datasource_id)

    selectQuery = "SELECT forge_id, forge_home_page \
                   FROM forges \
                   WHERE forge_abbr = %s"
    cursor1.execute(selectQuery, (irctype,))
    rows = cursor1.fetchall()

    forge_id = rows[0][0]
    menuURL = rows[0][1]

    try:
        print("opening menuURL:", menuURL)
        longhtml = urllib2.urlopen(menuURL).read()
    except urllib2.HTTPError as error:
        print(error)

    html2 = longhtml[0:2000]
    html2 = bytes.decode(html2)

    menuHTML = re.search('date=(..........)', html2)
    reUrlStem = re.search("alternate forms: <a href=\\\'(.*?)\?", html2)
    reFriendlyNameSuffix = re.search('<h1>#(.*?)</h1>', html2)

    if menuHTML and reUrlStem:
        menuHTMLGroup = menuHTML.group(1)
        friendlyNameSuffix = reFriendlyNameSuffix.group(1)
        urlStem = reUrlStem.group(1)
        urlStem = 'http://irclogs.dankulp.com'+urlStem
        cutOffDate = datetime.strptime(str(menuHTMLGroup), '%Y-%m-%d')

    while(dateS <= cutOffDate):
        print("working on ...")
        print(dateS)
        dateString = str(dateS)
        trunDateString = dateString[0:10]
        dayOfTheWeek = datetime.strptime(trunDateString,
                                         '%Y-%m-%d').strftime('%a')

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
        urlFile = "?date=" \
                  + str(yyyy) + "-" + str(mm) + "-" + str(dd) \
                  + "," \
                  + dayOfTheWeek \
                  + "&raw=on"
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

        insertQuery = "INSERT INTO datasources(datasource_id,\
                forge_id,\
                friendly_name,\
                date_donated,\
                contact_person,\
                comments,\
                start_date,\
                end_date) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
        # ======
        # LOCAL
        # ======
        try:
            cursor1.execute(insertQuery, (newDS,
                            forge_id,
                            friendlyNameSuffix + " "
                                               + str(yyyy)
                                               + str(mm)
                                               + str(dd),
                            datetime.now(),
                            'msquire@elon.edu',
                            fileLoc,
                            datetime.now(),
                            datetime.now()))
        except pymysql.Error as error:
            print(error)
            db1.rollback()
        # ======
        # REMOTE
        # ======
        try:
            cursor2.execute(insertQuery, (newDS,
                            forge_id,
                            friendlyNameSuffix + " "
                                               + str(yyyy)
                                               + str(mm)
                                               + str(dd),
                            datetime.now(),
                            'msquire@elon.edu',
                            fileLoc,
                            datetime.now(),
                            datetime.now()))
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
    print("You need both a datasource_id and a password to start.")
