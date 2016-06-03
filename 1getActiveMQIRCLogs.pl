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
################################################################
# usage:
# > perl 1getActiveMQIRCLogs.pl <new_datasource_id> <date-to-start> 
#
# THIS DATASOURCE IS THE NEXT ONE AVAIL IN THE DB - AND IT WILL GET INCREMENTED
# DATE TO START is the oldest un-collected date; 
# the script will go from there through yesterday in order
# example usage:
# > perl 1getActiveMQIRCLogs.pl 48039 20140310
#
# purpose: 
# grab all the IRC logs from http://irclogs.dankulp.com
# parse these files looking for facts to populate the aries irc table
################################################################
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

# --------------------------------------------------
# get the files from the web site
# starting with the date given on the command line
# store each URL as a local file
# --------------------------------------------------
def grabFiles(dbh1, dbh2, datasource_id, urlstem, date_to_start):
    p_dbh1= dbh1
    p_dbh2= dbh2
    p_datasource_id = datasource_id
    p_urlstem= urlstem
    p_date_to_start = date_to_start
    cursor= dbh1.cursor()
    newds = p_datasource_id
    
     #get yesterday's date
    yesterday= datetime.datetime.now()-timedelta(days= 1)
    print ("yesterday's date is:",yesterday)
    dates= datetime.datetime(int(p_date_to_start[0:4]),int(p_date_to_start[4:-2]),int(p_date_to_start[6:]))

    while(dates <= yesterday):
        print("working on ...")
        print(date)
        
        
        # get yyyy, mm, dd and put into URL
                # get yyyy, mm, dd and put into URL
        yyyy = dates.year
        mm   = dates.month
        dd   = dates.day
        
        # put leading zeroes on mm and dd
        if (mm < 10):
            mm = "0" + str(mm)
        if (dd < 10):
            dd = "0" + str(dd)
        fileName= "/"+str(yyyy)+"-"+str(mm)+ "-"+str(dd)
        
        # get file
        # Log URLs are in this format:
        # http://irclogs.dankulp.com/logs/irclogger_log/apache-activemq?date=2011-11-28&raw=on
        filestem = "?date=" + str(yyyy) + "-" + str(mm) + "-" + str(dd) + "&raw=on";
        newURL = p_urlstem + filestem
        print ("getting URL ",newURL)
        
        saveLoc = p_datasource_id + "/" + str(yyyy) + str(mm) + str(dd)
        print ("...saving as: ", saveLoc)        
        
        try:
            html = urllib2.urlopen(newURL).read()
        except urllib2.HTTPError as error:
            print(error)
        else:
            fileloc = datasource_id + fileName
            outfile = codecs.open(fileloc,'w')
            outfile.write(str(html))
            outfile.close()		   

        #======
        # LOCAL
        #======
        try:
            insertQuery="INSERT INTO `datasources`(`datasource_id`,\
            `forge_id`,\
            `friendly_name`,\
            `date_donated`,\
            `contact_person`,\
            `comments`,\
            `start_date`,\
            `end_date`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(insertQuery,(newds,forge_id,'ActiveMQ IRC '+ str(yyyy) + str(mm) + str(dd),datetime.datetime.now(),'msquire@elon.edu',saveLoc,datetime.datetime.now(),datetime.datetime.now()))
        except pymysql.Error as error:
            print(error)
            dbh1.rollback()
        #increment date by one
        dates= dates + timedelta(days=1)
        newds= int(newds) +1
"""
        #=====
        # SYR
        #=====
		my $insertQuery2 = $p_dbh2->prepare(qq{INSERT IGNORE INTO ossmole_merged.datasources
						(datasource_id,
						forge_id,
						friendly_name,
						date_donated,
						contact_person,
						comments,
						start_date,
						end_date)
						VALUES (?,?,?,now(),'msquire\@elon.edu',?,now(),now())});
	
		$insertQuery2->execute($newds, $forge_id, 'ActiveMQ IRC '. $yyyy.$mm.$dd, $saveLoc)
		  or die "Couldn't execute statement on SYR: " . $insertQuery2->errstr;
		$insertQuery2->finish();
        
        
        #increment date by one
        date->add(days=>1);
        $newds++;
"""
datasource_id = str(sys.argv[1])
date_to_start = str(sys.argv[2])
password= str(sys.argv[3])
urlstem = "http://irclogs.dankulp.com/logs/irclogger_log/apache-activemq";
forge_id = 36;

if datasource_id and date_to_start:
    try:
        dbh1 = pymysql.connect(host='grid6.cs.elon.edu',
                                  database='test',
                                  user='eashwell',
                                  password=password,
                                  charset='utf8')
    
    except pymysql.Error as err:
        print(err)
    try:
         dbh2 = pymysql.connect(host='flossdata.syr.edu',
                                  database='rubygems',
                                  user='megan',
                                  password=password,
                                  charset='utf8')
    except pymysql.Error as err:
        print(err)
        dbh2= "remote"
    
    os.mkdir (datasource_id)
    grabFiles(dbh1, dbh2, datasource_id, urlstem, date_to_start)
    dbh1.close()
else:
	print ("You need both a datasource_id and a date to start on your commandline.")
	exit;
