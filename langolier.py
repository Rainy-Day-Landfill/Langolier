#!/usr/bin/python3

from elasticsearch import Elasticsearch
from configparser import ConfigParser
from datetime import datetime, timedelta
from enum import Enum
import re
import os
import sys


# not necessary to extend Elasticsearch class just for a set of wrappers
# OOP Fundies
class IndexBuffet:
    def __init__(self, es_dns_name, es_user, es_pass):
        self.es_session = Elasticsearch([es_dns_name], port=80, scheme='http', http_auth=(es_user, es_pass))

    # returns the indices available to be deleted safely
    # getter/setters
    def available_indices(self):
        all_indices = self.es_session.indices.get_mapping()

        # filter out internal indices indicated by a "."
        return [index for index in all_indices.keys() if not index.startswith(".")]

    # returns a bool indicating whether the index name HAS a date
    # regex match
    def index_has_date(self, index_name):
        date_pattern = re.compile('.*-\d{4}\.\d{2}\.\d{2}')

        match = date_pattern.match(index_name)

        if match is not None:
            # it matched
            return True

        # it didn't match
        return False

    class DateExtractionException(Exception):
        pass


    # returns the date of the index (DateTime Obj)
    # regex capture, deserialization
    def index_date(self, index_name):
        # be safe
        if not self.index_has_date( index_name ):
            raise self.DateExtractionException("Tried to further process a date that should not be processed.")

        date_pattern = re.compile('.*-(\d{4}\.\d{2}\.\d{2})')

        capture = date_pattern.search(index_name)

        try:
            dt_obj = datetime.strptime(capture.group(1), "%Y.%m.%d")
        except Exception as e:
            raise self.DateExtractionException("Failed to extract date from index name in index_date")

        return dt_obj

    # returns a bool indicating if the index should be deleted
    # determined by whether its older than age_days
    # single purpose
    def index_date_is_expired(self, index_name, age_days):
        index_date = self.index_date(index_name)
        if index_date < datetime.now() - timedelta(days=age_days):
            return True
        return False

    # returns a list of indices that should be deleted
    # laziness
    def deletion_candidates(self, age_days):
        indices = self.available_indices()
        candidates = list()
        for index_name in indices:
            if self.index_has_date(index_name) and self.index_date_is_expired(index_name, age_days):
                candidates.append(index_name)

        return candidates

    # deletes a list of indices
    def delete_indices(self, indices):
        for index in indices:
            logs.submit(logs.INFO, "Deleting \"{0}\"".format(index))
            self.es_session.indices.delete(index=index)


def create_test_data( buffet ):
    # My birthday and SEVEN STRAIGHT DAYS OF CELEBRATION.
    buffet.es_session.indices.create(index='week-off-is-wise-2017.05.03', ignore=400)
    buffet.es_session.indices.create(index='week-off-is-wise-2017.05.04', ignore=400)
    buffet.es_session.indices.create(index='week-off-is-wise-2017.05.05', ignore=400)
    buffet.es_session.indices.create(index='week-off-is-wise-2017.05.06', ignore=400)
    buffet.es_session.indices.create(index='week-off-is-wise-2017.05.07', ignore=400)
    buffet.es_session.indices.create(index='week-off-is-wise-2017.05.08', ignore=400)
    buffet.es_session.indices.create(index='week-off-is-wise-2017.05.09', ignore=400)
    buffet.es_session.indices.create(index='week-off-is-wise-2017.05.10', ignore=400)
    # :D
    buffet.es_session.indices.create(index='week-off-is-wise-2018.05.03', ignore=400)


# Moar deserialization examples
class ConfigLoader:
    # the basic ini schema (all listed values required)
    def __init__(self, filename):
        # enforce required values in the config
        self.ini_schema = {
            'elasticsearch': ['user', 'pass', 'dns_name'],
            'logging': ['path']
        }

        if not os.path.exists(filename):
            raise ImportError("Config file \"{0}\" does not exist!".format(filename))

        self.config = ConfigParser()
        self.config.read(filename)

        for k, v in self.ini_schema.items():
            if not self.config.has_section(k):
                raise ImportError("\"{0}\" section missing from conf file.".format(k))
            else:
                for prop in v:
                    if not self.config.has_option(k, prop):
                        raise ImportError("\"{0}\" section is missing required property \"{1}\".".format(k, prop))

    def get_conf(self):
        return self.config


# ------------------------------------------------------------------------------
# logging api
# ------------------------------------------------------------------------------
# borrowed this pattern from one of my open source projects so I can get free QA
# on it
class logs(Enum):
    INFO = "II"
    WARNING = "WW"
    ERROR = "EE"
    ENQUEUE = "TX"

    @staticmethod
    def submit(log_type, msg):
        time = datetime.now()
        loggable_msg = "[{0}] [{1}] {2}".format(time, log_type.value, msg)

        if log_type == logs.INFO:
            #STDOUT
            print(loggable_msg, file=sys.stdout)

            # client log
            with open(config['logging'].get("path"), "a") as client_log:
                client_log.write("{0}\n".format(loggable_msg))

        if log_type == logs.WARNING:
            # STDOUT
            print(loggable_msg)

            # client error log
            with open(config['logging'].get("path"), "a") as error_log:
                error_log.write("{0}\n".format(loggable_msg))

        if log_type == logs.ERROR:
            # STDERR
            print(loggable_msg, file=sys.stderr)

            # client error log
            with open(config['logging'].get("path"), "a") as error_log:
                error_log.write("{0}\n".format(loggable_msg))

#        if log_type == logs.ENQUEUE:
#            # STDOUT for journald collection
#            print(msg, file=sys.stdout)



def Main( **kwargs ):

    buffet = IndexBuffet(
        config.get('elasticsearch', 'dns_name'),
        config.get('elasticsearch', 'user'),
        config.get('elasticsearch', 'pass')
    )

    create_test_data( buffet )

    deletes = buffet.deletion_candidates(3)

    buffet.delete_indices( deletes )

if __name__ == '__main__':

    conf_file_path = 'config.ini'
    configLoader = ConfigLoader(conf_file_path)
    config = configLoader.get_conf()

    Main()