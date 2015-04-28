#!/usr/bin/python

import time 
import os
import ast
import psycopg2
import sys,getopt
import xmlrpclib

# Regarding command
from optparse import OptionParser
usage  = "usage: %prog -f inputfile  -r reponame -v comparedversion" 
parser = OptionParser(usage=usage) 
parser.add_option("-f", "--file", dest="filename", help="Specify new version file.", metavar="FILE") 
parser.add_option("-v", "--comparedversion", dest="comparedversion", help="Specify a compared version of the past project we did previously.", metavar="VERSION")
parser.add_option("-r", "--reponame", dest="reponame", help="Specify a compared repo name, if not specified, compared with all of repos in the certain version that you specify with option -v." , metavar="REPONAME") 

(options, args) = parser.parse_args()

#status
from enum import Enum
class Status(Enum):
     InNewNotInOld  = 'New Component in PCF 1.4 that was NOT in PCF 1.3'
     InOldNotInNew  = 'Old Component NOT in PCF 1.4 that WAS in PCF 1.3'
     HasDiffVersion = 'Changed Component in PCF 1.4 that is a DIFFERENT VERSION of Component that WAS in PCF 1.3'
     SameVersion    = 'Exact same component/version/license in BOTH PCF 1.4 AND PCF 1.3 (no change)'

#Environment variable
scotzilla_url = "https://10.111.113.246/xmlrpc.cgi"
dbname        = "pcf_production_08_04"
username      = "postgres"
scotzilla = xmlrpclib.Server(scotzilla_url).SCOTzilla
print "Get started with handling ["+options.filename+"] at "+time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

def getOldFromDB(oldDict):
  try:
     con = psycopg2.connect("dbname={0} user={1}".format(dbname,username) ) 
     cur = con.cursor()    
     cur.execute(sqlStr)
     oList = cur.fetchall()
 
     for row in oList:
         key = row[0]
         print row
         oldDict.setdefault(key,[])
         oldDict[key].append(row[1])


  except psycopg2.DatabaseError, e:
      print 'Error %s' % e    
      sys.exit(1)
    
    
  finally:   
    if con:
        con.close()

def getNewFromFile(newDict):
  line_input = f_input.readline()
  i = 0
  while line_input:
        pair = line_input.strip().split(',')
        key = pair[0]
        newDict.setdefault(key,[])
        #print pair
        i=i+1
        newDict[key].append(pair[1])
        line_input = f_input.readline()

  f_input.close()

#Compare and merge new and old
def compareAndMerge(nDict,oDict):
  outputDict = {}
  for key, value in nDict.iteritems():
      oVersionList = oDict.get(key) #return the value for key
      if oVersionList != None:
         for nVersion in value:
             for oVersion in oVersionList:
                 if nVersion == oVersion:  
                    outputDict[(key,nVersion,oVersion)] = Status.SameVersion
                 else:
                    outputDict[(key,nVersion,oVersion)] = Status.HasDiffVersion
      else:
         for nVersion in value:
             outputDict[(key,nVersion,'None')] = Status.InNewNotInOld

  for key,value in oDict.iteritems():
      nVersionList = nDict.get(key)
      if nVersionList == None:
          for oVersion in value:
              outputDict[(key,'None',oVersion)] = Status.InOldNotInNew

  return outputDict 

def writeOutputFIle(outputDict):
    f_output.write('name,new_version,old_version,status\n')
    for key,value in outputDict.iteritems():
        #if value == Status.SameVersion or value == Status.InNewNotInOld:
        #   response = scotzilla.find_master({
        #              'name': key[0],
        #              'version': key[1],
        #              'category': 'VMWsource'})
        #   if  response['stat'] == 'ok':
        #      f_output.write('{0},{1},{2},{3},{4},{5}\n'.format(key[0],key[1],key[2],value,response['data']['license_name'],response['data']['source_url']) )
        #   else:
        #      f_output.write('{0},{1},{2},{3}\n'.format(key[0],key[1],key[2],value) )
        #else:
           f_output.write('{0},{1},{2},{3}\n'.format(key[0],key[1],key[2],value) )  
        #f_output.flush()      
    f_output.close()

#start
f_input    = open(options.filename)
f_output   = file(options.filename+'.output','w')
sqlStr     = ' select name,version from container_packages where container_id =  (select id from containers where product_id = (select id from products where version like \'{0}%\' and deleted = false) and name = \'{1}\' and deleted = false) and deleted = false '.format(options.comparedversion , options.reponame)
print sqlStr
con        = None
oldDict    = {}
newDict    = {}
getOldFromDB(oldDict)
getNewFromFile(newDict)
output     = compareAndMerge(newDict,oldDict)
writeOutputFIle(output) 
#print oldDict
#print newDict
#print '------'
#print output

print "Completed ["+options.filename+"] at "+time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
print "---------------------------------------------------"
