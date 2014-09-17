__author__ = 'blackms'

import feedparser
import urllib.request
import threading
import sys
import os
import pymysql
import time
import ctypes
import re
import datetime
import sleekxmpp
import ssl
import utorrent as utorrent_module
import queue
from configparser import ConfigParser
from threading import BoundedSemaphore
from utorrent.connection import Connection

thread_pool = []

msg_queue = queue.Queue()
removed_q = queue.Queue()

single_lock = threading.Lock()
sem = BoundedSemaphore(3)

level1 = "   "
level2 = level1 * 2
level3 = level1 * 3

debug = 1


def config_section_map():
    _config = ConfigParser()
    _config.read("bot.conf")
    main_dict = {}
    for section in _config.sections():
        dict1 = {}
        options = _config.options(section)
        for option in options:
            dict1[option] = _config.get(section, option)
            if dict1[option] == -1:
                print("skip: %s" % option)
        main_dict[section] = dict1
    return main_dict


def run_in_thread(fn):
    def run(*k, **kw):
        _t = threading.Thread(target=fn, args=k, kwargs=kw)
        _t.start()
        return _t
    return run


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


class SendMsgBot(sleekxmpp.ClientXMPP):
    def __init__(self, jid, password, recipient, message):
        super(SendMsgBot, self).__init__(jid, password)
        self.recipient = recipient
        self.msg = message
        self.add_event_handler("session_start", self.start)

    # noinspection PyUnusedLocal
    def start(self, event):
        self.send_presence()
        self.get_roster()
        self.send_message(mto=self.recipient,
                          mbody=self.msg,
                          mtype='chat')
        self.disconnect(wait=True)


class StatsScheduler(threading.Thread):
    def __init__(self, conf):
        print("Starting Stats Scheduler Thread")
        super(StatsScheduler, self).__init__()
        self.conf = conf
        self.ut = UtorrentMgmt(conf['webui_host'],
                               conf['webui_username'],
                               conf['webui_password'])
        self.stop = False

    def run(self):
        while self.stop is False:
            try:
                msg = self.ut.get_stats(time_frame='1d')
                print("[thread.StatsScheduler.run] Pushing message in queue.")
                msg_queue.put(msg)
                for _ in range(1800):
                    time.sleep(1)
                    if self.stop is True:
                        break
            except ComplexException as e:
                print(e)
                pass
        if self.stop is True:
            print("[thread.StatsScheduler] has gone away.")


class SenderThread(threading.Thread):
    def __init__(self, jconf):
        super(SenderThread, self).__init__()
        self.stop = False
        self.jid = jconf['source_jid']
        self.jid_pwd = jconf['source_jid_pwd']
        self.dst = jconf['dest_jid']
        self.j_host = jconf['jabber_host']
        self.j_port = jconf['jabber_port']
        print("Starting Gtalk Sender Thread... Waiting asynchronous for messages in queue.")

    def run(self):
        while self.stop is False:
            print("[thread.SenderThread.run] Waiting for msg in queue...")
            while msg_queue.not_empty:
                try:
                    msg = msg_queue.get(timeout=10)
                    print("[thread.SenderThread.run] Message received in queue... Sending it.")
                    xmpp = SendMsgBot(self.jid, self.jid_pwd, self.dst, msg)
                    xmpp.register_plugin('xep_0030')
                    xmpp.register_plugin('xep_0199')
                    xmpp.ssl_version = ssl.PROTOCOL_SSLv3
                    if xmpp.connect((self.j_host, self.j_port)):
                        xmpp.process(thread=True, block=True)
                    msg_queue.task_done()
                    if self.stop is True:
                        break
                except ComplexException as e:
                    print(e)
                    pass
                except queue.Empty:
                    if self.stop is True:
                        break
                    pass
        if self.stop is True:
            print("[thread.SenderThread] has gone away.")


class TorrentRemover(object):
    def __init__(self, host, username, password):
        self._conn = Connection(host, username, password).utorrent(api='falcon')

    @run_in_thread
    def remove(self):
        sort_field = 'ratio'
        t_list = sorted(self._conn.torrent_list().items(), key=lambda x: getattr(x[1], sort_field), reverse=True)
        min_age = time.time() - 259200
        for _t in t_list:
            t_hash, t_data = _t[0], _t[1]
            if t_data.ratio < 3 or t_data.ul_speed > 0 or t_data.dl_speed > 0:
                continue
            if time.mktime(t_data.completed_on.timetuple()) < min_age:
                continue
            print(t_data.name)
            self._conn.torrent_stop(t_hash)
            self._conn.torrent_remove(t_hash, with_data=True)
            removed_q.put(t_data)


class UtorrentMgmt(object):
    def __init__(self, host, username, password):
        self._conn = Connection(host, username, password).utorrent(api='falcon')

    def get_stats(self, time_frame='1d'):
        res = self._conn.xfer_history_get()
        excl_local = self._conn.settings_get()["net.limit_excludeslocal"]
        torrents = self._conn.torrent_list()
        today_start = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        period = len(res["daily_download"])
        period_start = today_start - datetime.timedelta(days=period - 1)
        down_total_local = sum(res["daily_local_download"])
        down_total = sum(res["daily_download"]) - (down_total_local if excl_local else 0)
        up_total_local = sum(res["daily_local_upload"])
        up_total = sum(res["daily_upload"]) - (down_total_local if excl_local else 0)
        period_added_torrents = {k: v for k, v in torrents.items() if v.added_on >= period_start}
        period_completed_torrents = {k: v for k, v in torrents.items() if v.completed_on >= period_start}
        stat_msg = ""
        if time_frame == '31d':
            stat_msg += "Last {} days:\n".format(period)
            stat_msg += level1 + "Downloaded: {} (+{} local)\n".format(utorrent_module.human_size(down_total),
                                                                       utorrent_module.human_size(down_total_local))
            stat_msg += level1 + "  Uploaded: {} (+{} local)\n".format(utorrent_module.human_size(up_total),
                                                                       utorrent_module.human_size(up_total_local))
            stat_msg += level1 + "     Total: {} (+{} local)\n".format(
                utorrent_module.human_size(down_total + up_total),
                utorrent_module.human_size(
                    down_total_local + up_total_local))
            stat_msg += level1 + "Ratio: {:.2f}\n".format(up_total / down_total)
            stat_msg += level1 + "Added torrents: {}\n".format(len(period_added_torrents))
            stat_msg += level1 + "Completed torrents: {}\n".format(len(period_completed_torrents))
        elif time_frame == '1d':
            down_day_local = res["daily_local_download"][0]
            down_day = res["daily_download"][0] - (down_day_local if excl_local else 0)
            up_day_local = res["daily_local_upload"][0]
            up_day = res["daily_upload"][0] - (up_day_local if excl_local else 0)
            today_added_torrents = {k: v for k, v in torrents.items() if v.added_on >= today_start}
            today_completed_torrents = {k: v for k, v in torrents.items() if v.completed_on >= today_start}
            stat_msg += "Today:"
            stat_msg += level1 + "Downloaded: {} (+{} local)\n".format(utorrent_module.human_size(down_day),
                                                                       utorrent_module.human_size(down_day_local))
            stat_msg += level1 + "  Uploaded: {} (+{} local)\n".format(utorrent_module.human_size(up_day),
                                                                       utorrent_module.human_size(up_day_local))
            stat_msg += level1 + "     Total: {} (+{} local)\n".format(utorrent_module.human_size(down_day + up_day),
                                                                       utorrent_module.human_size(
                                                                           down_day_local + up_day_local))
            stat_msg += level1 + "Ratio: {:.2f}\n".format(up_day / down_day)
            stat_msg += level1 + "Added torrents: {}\n".format(len(today_added_torrents))
            stat_msg += level1 + "Completed torrents: {}\n".format(len(today_completed_torrents))
        return stat_msg


class GarbageCollector(threading.Thread):
    def __init__(self, **data):
        print("Starting Garbage Collector Thread")
        super(GarbageCollector, self).__init__()
        self.stop = False
        self.data = data

    def run(self):
        while self.stop is False:
            try:
                conn = pymysql.connect(**self.data)
                cur = conn.cursor()
                now = int(time.time())
                old_data = now - 345600
                cur.execute('DELETE FROM torrents WHERE added < %s' % old_data)
                conn.close()
                for _ in range(3600):
                    time.sleep(1)
                    if self.stop is True:
                        break
            except ComplexException as e:
                print(e)
        if self.stop is True:
            print("[thread.GarbageCollector] has gone away.")


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

    @run_in_thread
    def download_link(self, link, destination=None):
        file_name = link.split('/')[-1]
        destination = self.destination if destination is None else destination
        if destination is None:
            raise NoDestinationPath(error="No destination path specified.")
        path = '%s\%s' % (destination, file_name)
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

    @staticmethod
    def _get_free_space():
        free_bytes = ctypes.c_ulonglong(0)
        ctypes.windll.kernel32.GetDiskFreeSpaceExW(ctypes.c_wchar_p(u'e:\\'), None, None, ctypes.pointer(free_bytes))
        free_gb = int(round(free_bytes.value, 2)) / 1024 / 1024 / 1024
        return free_gb

    def run(self):
        print("Starting main Thread.")
        while self.stop is not True:
            try:
                self.cur.execute('SELECT url FROM torrents')
                entries = [x[0] for x in self.cur.fetchall()]
                feed = feedparser.parse(self.rss_link)
                for f in feed.entries:
                    if f.link not in entries:
                        m = re.match('.*Size: (\d+.\d+) GB.*', f.summary.split("\n")[1])
                        size = m.group(1) if m else 0
                        with single_lock:
                            print("[thread.Bot.run] New torrent fetched. Adding:\n"
                                  "\t[ ] {link}\n\t[ ]Size: {size} GB".format(link=f.link.split("/")[-1],
                                                                              size=size))
                        while self._get_free_space() < float(size):
                            print("[thread.Bot.run] No space available to add new torrents. Spawning Remover...")
                            tr = TorrentRemover(config['torrent_webui']['webui_host'],
                                                config['torrent_webui']['webui_username'],
                                                config['torrent_webui']['webui_password'])
                            t_thread = tr.remove()
                            t_thread.join()
                            t_data = removed_q.get(block=True)
                            print("[thread.Bot.run] Removed: {} With Ratio: {}".format(t_data.name, t_data.ratio))
                        print("[thread.Bot.run] Spawning new thread for download the link...")
                        self.download_link(f.link)
                        self._update_list(f.link)
                        free_space = self._get_free_space()
                        msg = "Added new torrent: {torrent}\n" \
                              "Torrent Size: {torrent_size}\n" \
                              "Free Space (GB): {free_space}\n".format(torrent=f.link.split("/")[-1],
                                                                       torrent_size=size,
                                                                       free_space=round(free_space, 2))
                        msg_queue.put(msg)
                time.sleep(2)
            except ComplexException as e:
                print(e)
        if self.stop is True:
            self.conn.close()
            print("[thread.bot:{}] has gone away.".format(self.tracker_id))


if __name__ == '__main__':
    config = config_section_map()
    trackers = [{k: config[k]} for k in config.keys() if 'tracker_' in k]
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
        thread_pool.append(bot)
        bot.start()
    _conn.close()
    garbage_collector = GarbageCollector(**db_auth_data)
    garbage_collector.setDaemon(True)
    thread_pool.append(garbage_collector)
    sender_thread = SenderThread(config['jabber'])
    sender_thread.setDaemon(True)
    thread_pool.append(sender_thread)
    stats_scheduler = StatsScheduler(config['torrent_webui'])
    stats_scheduler.setDaemon(True)
    thread_pool.append(stats_scheduler)

    stats_scheduler.start()
    garbage_collector.start()
    sender_thread.start()

    while len(thread_pool) > 0:
        try:
            threads = [t.join(1) for t in thread_pool if t is not None and t.isAlive()]
        except KeyboardInterrupt:
            print("Ctrl-c received! Sending kill to threads...")
            for t in thread_pool:
                t.stop = True
                thread_pool.remove(t)
