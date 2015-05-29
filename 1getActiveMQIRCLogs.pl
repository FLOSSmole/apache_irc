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
use LWP::Simple;
use strict;
use DBI;
use DateTime;

my $datasource_id = shift @ARGV;
my $date_to_start = shift @ARGV;
my $urlstem = "http://irclogs.dankulp.com/logs/irclogger_log/apache-activemq";
my $forge_id = 36;

if ($datasource_id && $date_to_start)
{
	# connect to db (once at local and once at remote backup server)
	# dsn takes the format of "DBI:mysql:ossmole_merged:grid6.cs.elon.edu"
	my $dsn1 = "DBI:mysql:ossmole_merged:hostname";
	my $dbh1 = DBI->connect($dsn1, "username", "password", {RaiseError=>1});
	
	my $dsn2 = "DBI:mysql:ossmole_merged:hostname";
	my $dbh2 = DBI->connect($dsn2, "username", "password", {RaiseError=>1});
	
	mkdir ($datasource_id);
	grabFiles($dbh1, $dbh2, $datasource_id, $urlstem, $date_to_start);
	
	$dbh1->disconnect(); 
	$dbh2->disconnect();
}
else
{
	print "You need both a datasource_id and a date to start on your commandline.";
	exit;
}

# --------------------------------------------------
# get the files from the web site
# starting with the date given on the command line
# store each URL as a local file
# --------------------------------------------------
sub grabFiles($dbh1, $dbh2, $datasource_id, $urlstem, $date_to_start)
{
    my $p_dbh1          = $_[0];
    my $p_dbh2          = $_[1];
    my $p_datasource_id = $_[2];
    my $p_urlstem       = $_[3];
    my $p_date_to_start = $_[4];
    
    my $newds = $p_datasource_id;
        
    #get yesterday's date
    my @yesterdayA = localtime( ( time() - ( 24 * 60 * 60 ) ) );
    my $yesterday = sprintf("%02d%02d%02d",($yesterdayA[5]+1900),$yesterdayA[4]+1,$yesterdayA[3]);
    print "yesterday's date is: $yesterday\n";
    my $date=DateTime->new(
    {
        year=>substr($p_date_to_start,0,4),
        month=>substr($p_date_to_start,4,2),
        day=>substr($p_date_to_start,6,2)
    });

    while($date->ymd('') le $yesterday)
    {
		print "working on ...";
		print $date->ymd('') . "\n";

		# get yyyy, mm, dd and put into URL
		my $yyyy = $date->year();
		my $mm   = $date->month();
		my $dd   = $date->day();

		# put leading zeroes on mm and dd
		if ($mm < 10)
		{
			$mm = "0" . $mm;
		}
		if ($dd < 10)
		{
			$dd = "0" . $dd;
		}
		# get file
		# Log URLs are in this format:
		# http://irclogs.dankulp.com/logs/irclogger_log/apache-activemq?date=2011-11-28&raw=on
		my $filestem = "?date=" . $yyyy . "-" . $mm . "-" . $dd . "&raw=on";
		my $newURL = $p_urlstem . $filestem;
		print "getting URL $newURL\n";

		my $saveLoc = $p_datasource_id . "/" . $yyyy.$mm.$dd;
		print "...saving as: $saveLoc";        
	
		my $status = getstore($newURL, $saveLoc);
		print "=======Error $status on $newURL" unless is_success($status);		   
	
		#======
		# LOCAL
		#======
		my $insertQuery1 = $p_dbh1->prepare(qq{INSERT IGNORE INTO ossmole_merged.datasources
						(datasource_id,
						forge_id,
						friendly_name,
						date_donated,
						contact_person,
						comments,
						start_date,
						end_date)
						VALUES (?,?,?,now(),'msquire\@elon.edu',?,now(),now())});
	
		$insertQuery1->execute($newds, $forge_id, 'ActiveMQ IRC '. $yyyy.$mm.$dd, $saveLoc)
		  or die "Couldn't execute statement on LOCAL: " . $insertQuery1->errstr;
		$insertQuery1->finish();
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
        $date->add(days=>1);
        $newds++;
    }    
}
