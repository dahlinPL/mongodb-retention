#!/usr/bin/env python
"""
 Date: 23/03/2018
 Author: Marcin Slowinski (dahlinek@gmail.com)
 Description: A script to remove old data from mongoDB collections.
 Additionally, after remove, it rebuilds indexes on secondaries.
 Requires: MongoClient in python (pymongo)
 Requires: MonthDelta module (monthdelta)
 TODO: accept one host as parameter and detect other hosts using mongo commands
"""

import calendar
import logging
import sys
import argparse
from datetime import date

from monthdelta import monthdelta
from pymongo import MongoClient, errors

def parse_cmdline_args():
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(description="""
    A script to remove old data from mongoDB collections. Additionally, 
    it can rebuild indexes on secondaries. For big data storage it's 
    convinient to run this script twice, one day for remove old data, and 
    second day to rebuild index, to avoid performance issues.""")

    parser.add_argument("database",
                        help="Database to operate on.")
    parser.add_argument("server", nargs="+",
                        help="Server/servers to connect to in host:port "
                             "format. For replica set provide primary and "
                             "secondaries.")
    parser.add_argument("-u", "--username",
                        help='Mongo database user.')
    parser.add_argument("-p", "--password",
                        help="Mongo database user password.")
    parser.add_argument("--retention", type=int,
                        help="Remove all collections documents older "
                             "than retention months.")
    parser.add_argument("--rebuild", action="store_true",
                        help="Rebuild all indexes on all database "
                             "collections on all secondaries.")
    parser.add_argument("--logfile",
                        help="Set logfile. If not set, print to console.")
    parser.add_argument("--loglevel", default=logging.INFO,
                        help="Change loglevel (default is INFO). See python "
                             "logging module docs for possible values.")
    return parser.parse_args()

class MongoDB(object):
    """ main script class """

    def __init__(self, args):
        self.mongodb_host_with_port = args.server
        self.mongodb_db = args.database
        self.mongodb_user = args.username
        self.mongodb_password = args.password
        self.__conn = None

        logging.basicConfig(level=args.loglevel,
                            format='%(asctime)s %(levelname)s %(message)s',
                            filename=args.logfile)

    def is_master(self):
        """check if current host is replica set primary"""

        mongo_db = self.__conn['admin']
        master = mongo_db.command('isMaster')['ismaster']
        return master

    def connect(self, host_with_port):
        """Connect to MongoDB"""

        if self.__conn is not None:
            self.close()

        if self.mongodb_user is None:
            try:
                self.__conn = MongoClient('mongodb://%s' % (host_with_port))
            except errors.PyMongoError as we_all_gonna_die:
                logging.error('Error in MongoDB connection: %s',
                              str(we_all_gonna_die))
        else:
            try:
                self.__conn = MongoClient('mongodb://%s:%s@%s' %
                                          (self.mongodb_user,
                                           self.mongodb_password,
                                           host_with_port))
            except errors.PyMongoError as we_all_gonna_die:
                logging.error('Error in MongoDB connection: %s',
                              str(we_all_gonna_die))

    def cut_old_data(self, retention):
        """remove all collections documents older than retention months"""

        for host in self.mongodb_host_with_port:
            self.connect(host)
            if self.is_master():
                logging.info('Successfully connected to primary %s as %s.',
                             host, self.mongodb_user)
                break
        else:
            logging.error('Primary server not found! Exiting')
            sys.exit(1)

        mongo_db = self.__conn[self.mongodb_db]
        collections = mongo_db.collection_names(0)

        today = date.today()
        cut_off_date = today - monthdelta(retention)
        cut_off_timestamp = calendar.timegm(cut_off_date.timetuple()) * 1000
        logging.info('Retention months is set to %d - '
                     'removing documents older than %s.',
                     retention, cut_off_date)

        for coll in collections:
            collection = mongo_db.get_collection(coll)
            logging.info('Collection: %s', str(coll))
            how_many = collection.count()
            how_many_to_cut_off = collection.find(
                {"timestamp": {"$lt": cut_off_timestamp}}).count()
            logging.info('Number of documents total/to remove: %d/%d.',
                         how_many, how_many_to_cut_off)
            collection.remove({"timestamp": {"$lt": cut_off_timestamp}})
            how_many_left = collection.count()
            logging.info("Removed %d documents. "
                         "Documents left after remove: %d.",
                         how_many_to_cut_off, how_many_left)

    def rebuild_indexes(self):
        """rebuild all indexes on all database collections on all
        secondaries"""

        slave_found = False

        for host in self.mongodb_host_with_port:
            self.connect(host)
            if self.is_master():
                continue

            slave_found = True
            logging.info('Rebuilding indexes on secondary %s', str(host))
            mongo_db = self.__conn[self.mongodb_db]
            collections = mongo_db.collection_names(0)

            for coll in collections:
                collection = mongo_db.get_collection(coll)
                indexes_list = collection.index_information()
                logging.debug('Collection %s index list:', str(coll))
                for idx in indexes_list:
                    logging.debug('%s', str(idx))
                collection.reindex()
                logging.info('Collection %s indexes rebuilded.', str(coll))

        if not slave_found:
            logging.error('Secondary server not found! Exiting')
            sys.exit(1)

    def close(self):
        """Close connection"""

        if self.__conn is None:
            return
        self.__conn.close()
        self.__conn = None

if __name__ == '__main__':
    cmdline_args = parse_cmdline_args()
    mongodb = MongoDB(cmdline_args)
    if cmdline_args.retention:
        mongodb.cut_old_data(cmdline_args.retention)
    else:
        logging.info('called without --retention argument - skipping')
    if cmdline_args.rebuild:
        mongodb.rebuild_indexes()
    else:
        logging.info('called without --rebuild argument - skipping')
    mongodb.close()
