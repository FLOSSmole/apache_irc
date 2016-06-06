#!/usr/bin/perl
## This program is free software; you can redistribute it
## and/or modify it under the same terms as Perl itself.
## Please see the Perl Artistic License 2.0.
## 
## Copyright (C) 2004-2015 Megan Squire <msquire@elon.edu>
##
## We're working on this at http://flossmole.org - Come help us build 
## an open and accessible repository for data and analyses for open
## source projects.
##
## If you use this code or data for preparing an academic paper please
## provide a citation to 
##
## Howison, J., Conklin, M., & Crowston, K. (2006). FLOSSmole: 
## A collaborative repository for FLOSS research data and analyses. 
## International Journal of Information Technology and Web Engineering, 1(3), 17–26.
##
## and
##
## FLOSSmole (2004-2015) FLOSSmole: a project to provide academic access to data 
## and analyses of open source projects.  Available at http://flossmole.org 
#
# --------------------------------------------------
# subroutine: parseFile
# takes: two database connections (local and remote) and a datasource_id
# purpose:
# --get each file on disk
# --pull out the lines
# --parse out the pieces of the lines
# --write each line to the irc table in both local and remote db
# --------------------------------------------------
def parseFile(dbh2, dbh3, ds, fileLoc):
    p_dbh2  = dbh2
    p_dbh3  = dbh3
    p_ds    = ds
    p_fileLoc = fileLoc
    cursor= dbh2.cursor()
    #date is in the filename, in the format:
    # 48039/20140306
    datelog =""
    formatting= re.search("^(.*?)\/(.*?)$",p_fileLoc)
    if formatting:
        tempdate = formatting.group(2)
        print ("got ", tempdate," for date");
        
        dateCorrection= re.search("^(\d\d\d\d)(\d\d)(\d\d)",tempdate)
        if dateCorrection:
            datelog = dateCorrection.group(1) + "-" + dateCorrection.group(2) + "-" + dateCorrection.group(3)
            fileLoc= formatting.group(1)+ "/" + datelog
        else:
            datelog=formatting.group(2)
    # open the file
    print ("opening file: " + p_fileLoc );

    try:
        log = codecs.open(fileLoc, 'r', encoding='utf-8', errors='ignore')
        line=log.read()
        line= line[2:]
        line= line[:-3]
        log=line.split("\\n")
    except pymysql.Error as err:
        print(err)
    print(log)
    
    linenum = 0
    for line in log:   
        print(line)
        linenum= linenum+1
        send_user = ""
        timelog = ""
        line_message = ""
        messageType = ""
        
        # parse out rest of details & insert
        # 1. get system message vs regular message, parse
        # 2. insert
        #
        # here are the two patterns:
        # [20:05] <zmhassan> any tips would be gladly appreciated
        # [16:09] *** jbonofre has quit IRC (Excess Flood)
        regularMessage= re.search('^\[(.*?)\]\s+\<(.*?)\>\s+(.*?)$',line)
        systemMessage= re.search('^\[(.*?)\]\s+\*\*\*\s+(.*?)$',line)
        if regularMessage: #regular message
            timelog = regularMessage.group(1)
            send_user = regularMessage.group(2)
            line_message = regularMessage.group(3)
            messageType = "message";
        elif systemMessage: # system message
            messageType = "system";
            timelog = systemMessage.group(1)
            line_message = systemMessage.group(2)
    	
        print("inserting row ", linenum," for ", datelog)

        #======
        # LOCAL
        #======                
        if ((p_ds) and (messageType != "")):
            try:
                insertQuery= "INSERT INTO `apache_activemq_irc`(`datasource_id`,\
                `line_num`,\
                `full_line_text`,\
                `line_message`,\
                `date_of_entry`,\
                `time_of_entry`\
                ,`type`\
                ,`send_user`\
                ,`last_updated`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                cursor.execute(insertQuery,(p_ds,linenum,line,line_message,datelog,timelog, messageType, send_user,datetime.datetime.now()))
                dbh2.commit()
            except pymysql.Error as error:
                print(error)
                dbh2.rollback()
    cursor.close()

"""
        #======
        # SYR
        #======
        if (($p_ds) && ($type ne ""))
        {
            my $insert3 = $p_dbh3->prepare(qq{
                            INSERT IGNORE INTO apache_activemq_irc
                                (datasource_id, 
                                line_num,
                                full_line_text,
                                line_message,
                                date_of_entry,
                                time_of_entry,
                                type,
                                send_user,
                                last_updated) 
                            VALUES (?,?,?,?,?,?,?,?,NOW())
                            });
            $insert3->execute($p_ds, $linenum, $line, $line_message, $datelog, $timelog, $type, $send_user)
                or die "Couldn't execute statement on SYR: " . $insert3->errstr;
            $insert3->finish();
        } 
    }
}
"""	
################################################################
# usage:
# > perl 2parseActiveMQIRCLogs.pl <datasource_id> 
#
# example usage:
# > perl 2parseActiveMQIRCLogs.pl 48483 
#
# purpose: 
# open each IRC log in the directory, parse out the interesting bits
# notes:
# START WITH THE FIRST DS IN THE DB YOU ARE INTERESTED IN
# Does not need to be the latest one
#
################################################################
import pymysql
import sys
import re
import codecs
import datetime

datasource_id = str(sys.argv[1])
password= str(sys.argv[2])
forge_id = 36

if (datasource_id):
	# connect to db (once at local grid6, and once at Syracuse)
	# dsn takes the format of "DBI:mysql:ossmole_merged:grid6.cs.elon.edu"
    try:
        dbh2 = pymysql.connect(host='grid6.cs.elon.edu',
                                  database='test',
                                  user='eashwell',
                                  password=password,
                                  charset='utf8')
    
    except pymysql.Error as err:
        print(err)
    try:
        dbh3 = pymysql.connect(host='grid6.cs.elon.edu',
                                  database='test',
                                  user='eashwell',
                                  password=password,
                                  charset='utf8')
    
    except pymysql.Error as err:
        print(err)
        """
    try:
         dbh3 = pymysql.connect(host='flossdata.syr.edu',
                                  database='rubygems',
                                  user='megan',
                                  password=password,
                                  charset='utf8')
    except pymysql.Error as err:
        print(err)
        dbh2= "remote"
        """
    cursor= dbh2.cursor()
    
    selectQuery="select datasource_id, comments \
		from test.datasources \
		where datasource_id >= %s \
		and forge_id=%s"  
    cursor.execute(selectQuery,(datasource_id,forge_id))
    rows=cursor.fetchall()    
    
    print(rows)
     
    for row in rows : 
        ds= row[0]
        fileLoc= row[1]
        print ("==================\n")
        parseFile(dbh2, dbh3, ds, fileLoc)
    cursor.close()
    dbh2.close()
    dbh3.close()
    print("done")
else:
	print ("You need both a datasource_id and a date to start on your commandline.")
	exit;

