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
> python3 2parseApacheLogs.py <datasource_id> <password> <irctype>

example usage:
> python3 2parseApacheLogs.py 68288 password CXI2

irctype choices as of August 24, 2016:
CXI2 (CXF v2)
MQI2 (ActiveMQ v2)
KFI2 (Karaf v2)
SMI2 (ServiceMix v2)
AAI  (Aries)

purpose:
parse out key components, store in a database
################################################################
"""

import pymysql
import sys
import re
import codecs
import datetime

datasource_id = str(sys.argv[1])
newDS         = int(datasource_id)
password      = sys.argv[2]
ircType       = sys.argv[3]

# A long if statement that changes which database you insert into based on the
# third argument of the commandline

if ircType == 'MQI2':
    tableName = 'apache_activemq_irc'
elif ircType == 'AAI':
    tableName = 'apache_aries_irc'
elif ircType == 'CXI2':
    tableName = 'apache_cxf_irc'
elif ircType == 'KFI2':
    tableName = 'apache_karaf_irc'
elif ircType == 'SMI2':
    tableName = 'apache_servicemix_irc'
else:
    print("invalid irc type")
    exit

if datasource_id and password:
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
        db2 = pymysql.connect(host='grid6.cs.elon.edu',
                              database='apache_irc',
                              user='megan',
                              password=password,
                              use_unicode=True,
                              charset='utf8')
    except pymysql.Error as err:
        print(err)

    try:
        db3 = pymysql.connect(host='flossdata.syr.edu',
                              database='apache_irc',
                              user='megan',
                              password=password,
                              use_unicode=True,
                              charset='utf8')
    except pymysql.Error as err:
        print(err)

    cursor1 = db1.cursor()
    cursor2 = db2.cursor()
    cursor3 = db3.cursor()

    selectQuery = "SELECT d.datasource_id, d.comments \
                FROM datasources d \
                INNER JOIN forges f on f.forge_id = d.forge_id \
                WHERE datasource_id >= %s \
                AND f.forge_abbr = %s"
    cursor1.execute(selectQuery, (datasource_id, ircType))
    rows = cursor1.fetchall()

    for row in rows:
        newDS = int(row[0])
        fileLoc = row[1]
        print("==================\n")

        # open the file
        print("working on: ", fileLoc, "(", newDS, ")")
        log  = codecs.open(fileLoc, 'r', encoding='utf-8', errors='ignore')
        line = log.read()
        line = line[2:]
        line = line[:-3]
        log  = line.split("\\n")

        # Parse out details
        linenum = 0
        for line in log:
            linenum     += 1
            send_user    = ""
            timelog      = ""
            line_message = ""
            messageType  = ""

            # date is in the filename, in the format:
            # 48039/20140306
            datelog = ""
            formatting = re.search("^(.*?)\/(.*?)$", fileLoc)
            if formatting:
                tempdate = formatting.group(2)
                correctForm = re.search("^(\d\d\d\d)(\d\d)(\d\d)", tempdate)
                if correctForm:
                    datelog = correctForm.group(1)\
                            + "-"\
                            + correctForm.group(2)\
                            + "-"\
                            + correctForm.group(3)
                else:
                    datelog = formatting.group(2)

            # parse out rest of details & insert
            # 1. get system message vs regular message, parse
            # 2. insert
            #
            # here are the two patterns:
            # [20:05] <zmhassan> any tips would be gladly appreciated
            # [16:09] *** jbonofre has quit IRC (Excess Flood)
            regularMessage = re.search('^\[(.*?)\]\s+\<(.*?)\>\s+(.*?)$', line)
            systemMessage = re.search('^\[(.*?)\]\s+\*\*\*\s+(.*?)$', line)
            if regularMessage:  # regular message
                timelog = regularMessage.group(1)
                send_user = regularMessage.group(2)
                line_message = regularMessage.group(3)
                messageType = "message"
            elif systemMessage:  # system message
                messageType = "system"
                timelog = systemMessage.group(1)
                line_message = systemMessage.group(2)

            if ((datasource_id) and (messageType != "")):
                insertQuery = "INSERT IGNORE INTO " \
                              + tableName \
                              + "(datasource_id, \
                              line_num, \
                              full_line_text, \
                              line_message, \
                              date_of_entry, \
                              time_of_entry, \
                              type, \
                              send_user, \
                              last_updated) \
                              VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                currDate = datetime.datetime.now()

                # ======
                # LOCAL
                # ======
                try:
                    cursor2.execute(insertQuery,
                                    (newDS,
                                     linenum,
                                     line,
                                     line_message,
                                     datelog,
                                     timelog,
                                     messageType,
                                     send_user,
                                     currDate))
                    db2.commit()
                except pymysql.Error as error:
                    print(error)
                    db2.rollback()

                # ======
                # REMOTE
                # ======

                try:
                    cursor3.execute(insertQuery,
                                    (newDS,
                                     linenum,
                                     line,
                                     line_message,
                                     datelog,
                                     timelog,
                                     messageType,
                                     send_user,
                                     currDate))
                    db3.commit()
                except pymysql.Error as error:
                    print(error)
                    db3.rollback()

    cursor1.close()
    cursor2.close()
    cursor3.close()

    db1.close()
    db2.close()
    db3.close()

else:
    print ("Missing command arguments.")
