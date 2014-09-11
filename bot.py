__author__ = 'blackms'

import feedparser
import urllib.request
import threading
import sys
import os
import pymysql
import time
from configparser import ConfigParser
from pprint import pprint
from threading import BoundedSemaphore

tracker_thread_pool = []

single_lock = threading.Lock()
sem = BoundedSemaphore(3)


def config_section_map():
    _config = ConfigParser()
    _config.read("bot.conf")
    main_dict = {}
    dict1 = {}
    for section in _config.sections():
        options = _config.options(section)
        for option in options:
            dict1[option] = _config.get(section, option)
            if dict1[option] == -1:
                print("skip: %s" % option)
        main_dict[section] = dict1
    return main_dict


class ExceptionTemplateBase(Exception):
    def __call__(self, *args):
        return self.__class__(*(self.args + args))

    def __str__(self):
        return ": ".join(self.args)


class ComplexException(ExceptionTemplateBase):
    def __init__(self, *args, **kwargs):
        self.error = kwargs.pop('error')
        super(ComplexException, self).__init__(*args, **kwargs)
        self.exc_type, self.exc_obj, self.exc_tb = sys.exc_info()
        self.file_name = os.path.split(self.exc_tb.tb_frame.f_code.co_filename)[1]

    def __str__(self):
        super(ComplexException, self).__str__()
        return "Raised Exception: %s in: %s at line: %s.\n\tMsg: %s" % (self.exc_type, self.file_name,
                                                                        self.exc_tb.tb_lineno, self.error)


class NoDestinationPath(ComplexException):
    def __init__(self, *args, **kwargs):
        super(NoDestinationPath, self).__init__(*args, **kwargs)


class GarbageCollector(threading.Thread):
    def __init__(self, **data):
        print("Starting Garbage Collector Thread")
        super(GarbageCollector, self).__init__()
        self.stop = False
        self.data = data

    def run(self):
        while self.stop is False:
            conn = pymysql.connect(**self.data)
            cur = conn.cursor()
            now = int(time.time())
            old_data = now - 345600
            cur.execute('DELETE FROM torrents WHERE added < %s' % old_data)
            conn.close()
            time.sleep(3600)


class Bot(threading.Thread):
    def __init__(self, data, username=None, password=None, rss_link=None, destination=None, tracker_id=None):
        super(Bot, self).__init__()
        self.username = username
        self.password = password
        self.destination = destination
        self.rss_link = rss_link
        self.tracker_id = tracker_id
        self.stop = False
        self.conn = pymysql.connect(**data)
        self.cur = self.conn.cursor()

    @staticmethod
    def _write_torrent_to_file(path, torrent_data):
        mode = 'wb'
        with single_lock:
            with open(path, mode) as _tw:
                _tw.write(torrent_data)

    def download_link(self, link, destination=None):
        file_name = link.split('/')[-1]
        destination = self.destination if destination is None else destination
        if destination is None:
            raise NoDestinationPath(error="No destination path specified.")
        path = '%s\%s' % (destination, file_name)
        print("Path: " + path)
        if self.username is None:
            print("No credential specified. Building request without auth header.")
            res = urllib.request.urlopen(link)
            self._write_torrent_to_file(path, res.read())
        else:
            request = urllib.request.Request(link)
            base64string = ('%s:%s' % (self.username, self.password)).strip().encode('ascii')
            request.add_header("Authorization", "Basic %s" % base64string)
            res = urllib.request.urlopen(request)
            self._write_torrent_to_file(path, res.read())

    def _update_list(self, entry):
        with single_lock:
            self.cur.execute('INSERT INTO torrents (url, added, tid) '
                             'VALUES ("%s", %s, %s);' % (entry, int(time.time()), self.tracker_id))
            self.conn.commit()

    def run(self):
        print("Starting main Thread")
        while self.stop is not True:
            try:
                self.cur.execute('SELECT url FROM torrents')
                entries = [x[0] for x in self.cur.fetchall()]
                feed = feedparser.parse(self.rss_link)
                for f in feed.entries:
                    if f.link not in entries:
                        print("Adding:")
                        pprint(f)
                        self.download_link(f.link)
                        self._update_list(f.link)
                time.sleep(2)
            except ComplexException as e:
                print(e)
        if self.stop is True:
            self.conn.close()

if __name__ == '__main__':
    config = config_section_map()
    trackers = [{k: v} for k, v in config.items() if 'tracker_' in k]
    db_auth_data = {'host': config['db']['host'],
                    'user': config['db']['user'],
                    'passwd': config['db']['pass'],
                    'db': config['db']['db']}
    _conn = pymysql.connect(**db_auth_data)
    _cur = _conn.cursor()
    for tracker in trackers:
        key = [k for k in tracker.keys()][0]
        _cur.execute('SELECT tname, tid FROM trackers WHERE tname = "%s";' % key)
        tracker_db_result = _cur.fetchall()
        if len(tracker_db_result) < 1:
            _cur.execute('INSERT INTO trackers (tname) VALUES ("{tracker}")'.format(tracker=key))
            _conn.commit()
            _cur.execute('SELECT tname, tid FROM trackers WHERE tname = "%s";' % key)
            tracker_db_result = _cur.fetchall()
        bot = Bot(db_auth_data,
                  username=tracker[key]['username'],
                  password=tracker[key]['password'],
                  rss_link=tracker[key]['rss_url'],
                  destination=tracker[key]['watchdog'],
                  tracker_id=dict(tracker_db_result)[key])
        bot.setDaemon(True)
        tracker_thread_pool.append(bot)
        bot.start()
    garbage_collector = GarbageCollector(**db_auth_data)
    garbage_collector.setDaemon(True)
    garbage_collector.start()
    _conn.close()
    try:
        for thread in tracker_thread_pool:
            thread.join()
        garbage_collector.join()
    except KeyboardInterrupt:
        print("Quitting...")
        for thread in tracker_thread_pool:
            thread.stop = True
        garbage_collector.stop = True
