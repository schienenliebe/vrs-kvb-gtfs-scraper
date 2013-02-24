# -*- encoding: utf-8 -*-
import os
import time
from datetime import datetime, timedelta
from collections import defaultdict, namedtuple
from lxml import etree
from StringIO import StringIO
import glob
import plistlib
import logging
import pickle


import requests

logging.basicConfig(format='%(asctime)-6s: %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('VRSInfoScraper')
logger.setLevel(logging.INFO)


def convert_time(hours, minutes, seconds=0, first_time=None):
    absolute = hours * 3600 + minutes * 60 + seconds
    if first_time is not None and absolute < first_time:
        hours = hours + 24
    return "%02d:%02d:%02d" % (hours, minutes, seconds)


Agency = namedtuple('Agency', ['agency_name', 'agency_url', 'agency_timezone'])
Route = namedtuple('Route', ['route_id', 'route_short_name', 'route_long_name',
            'route_type', 'route_first', 'route_last'])
StopTime = namedtuple('StopTime', ['trip_id', 'arrival_time',
    'departure_time', 'stop_id', 'stop_sequence'])
Stop = namedtuple('Stop', ['stop_id', 'stop_name', 'stop_lat', 'stop_lon'])
Trip = namedtuple('Trip', ['route_id', 'service_id', 'trip_id'])
Calendar = namedtuple('Calendar', ['service_id', 'monday', 'tuesday', 'wednesday',
            'thursday', 'friday', 'saturday', 'sunday', 'start_date', 'end_date'])


class Routing(object):
    path = 'gtfs'
    files = ['agency', 'routes', 'stop_times', 'stops', 'trips', 'calendar']
    id_maker = None

    def __init__(self, routes):
        self.routes = routes

    def route_line(self, line, time):
        raise NotImplemented

    def make_id(self):
        i = 0
        while True:
            yield i
            i += 1

    def get_id(self):
        if self.id_maker is None:
            self.id_maker = self.make_id()
        return self.id_maker.next()

    def find_next_datetime(self, calendar, seconds):
        """
        Finds the next date that matches calendar
        and advances it a certain number of seconds
        """
        start_date = calendar.start_date
        year = int(start_date[:4])
        month = int(start_date[4:6])
        day = int(start_date[6:8])
        current = datetime(year, month, day)
        current += timedelta(seconds=seconds)
        while True:
            if calendar[current.weekday() + 1] == '1':
                break
            current += timedelta(days=1)
        return current

    def start(self, seconds=6 * 3600):
        try:
            self.stops, self.stop_name_cache, self.stop_cache = pickle.load(file('cache.pickle'))
        except IOError:
            pass
        self.trips = []
        self.stop_times = []
        for route in self.routes:
            logger.info("Going for %s" % str(route))
            for calendar in self.calendar:
                time = self.find_next_datetime(calendar, seconds)
                logger.info('Get stop times for %s and %s at %s' % (calendar.service_id, route, time))
                trips = self.get_trips(calendar.service_id, route.route_id, route.route_first,
                                                    route.route_last, time)
                for trip_id, stop_times in trips:
                    if not stop_times:
                        continue
                    self.stop_times.extend(stop_times)
                    self.trips.append(Trip(route.route_id, calendar.service_id, trip_id))

    def save(self):
        pickle.dump([self.stops, self.stop_name_cache, self.stop_cache], file('cache.pickle', 'w'))
        for filepart in self.files:
            self.save_file(filepart, getattr(self, filepart))

    def save_file(self, name, values):
        with file(os.path.join(self.path, '%s.txt' % name), 'w') as f:
            if not values:
                return
            logger.info('writing %s' % name)
            f.write(','.join(values[0]._fields).encode('utf-8') + '\n')
            for value in values:
                f.write(','.join([x.replace(',', ':') for x in map(unicode, value)]).encode('utf-8') + '\n')


class VRSInfo(Routing):
    API_BASE = 'http://auskunft.vrsinfo.de/vrs/cgi/process/eingabeRoute'
    API_STOPS = 'http://auskunft.vrsinfo.de/vrs/cgi/service/objects'
    API_GEO = 'http://www.vrsinfo.de/index.php?eID=tx_mobi_fahrplan_geocoder&epsg=4326&q=%(name)s'
    payload = {
        'start': 'Frankenstr.',
        'startID': '81',
        'startTyp': 'Stop',
        'via': '',
        'viaID': '',
        'viaTyp': '',
        'viaZeitAufenthalt': '0',
        'ziel': 'Andreaskloster',
        'zielID': '417',
        'zielTyp': 'Stop',
        'datum': '24.02.2013',
        'zeit': '14:32',
        'suchrichtung': 'ab',
        'barriere': '',
        'zuschlagfrei': '1',
        # 'verkehrsmittel': 'Underground,LightRail,Bus,CommunityBus'
        'verkehrsmittel': 'LongDistanceTrains,RegionalTrains,SuburbanTrains,Underground,LightRail,Bus,CommunityBus,OnDemandServices'
    }
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
        'Accept-Encoding': 'gzip,deflate,sdch',
        'Accept-Language': 'en-US,en;q=0.8',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Host': 'auskunft.vrsinfo.de',
        'Origin': 'http://auskunft.vrsinfo.de',
        'Pragma': 'no-cache',
        'Referer': 'http://auskunft.vrsinfo.de/vrs/cgi/page/eingabeRoute',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_2) AppleWebKit/535.18 (KHTML, like Gecko) Chrome/18.0.1010.0 Safari/535.18'
    }
    city = u'Köln'
    stop_name_cache = {}
    stop_cache = {}
    trip_cache = {}
    stops = []
    agency = [Agency('VRS', 'http://www.vrsinfo.de', 'Europe/Berlin')]
    calendar = [Calendar('0', '1', '0', '0', '0', '0', '0', '0', '20130224', '20130601')]

    def __init__(self, routes):
        self.routes = []
        for filename in glob.glob('vrs/*.data'):
            with file(filename) as f:
                logger.info('now loading %s' % filename)
                filename = filename.split('/')[-1]
                long_name = filename.split('(')[1].split(')')[0]
                linedir = filename.split('_')[0].split('-')
                line = linedir[0]
                direction = '0'
                if len(linedir) > 1:
                    direction = linedir[1]
                stations = [l.decode('utf-8').strip() for l in f if l.strip()]
                if not stations:
                    continue
                first = stations[0]
                last = stations[-1]
                if line.startswith('0'):
                    route_type = '0'
                else:
                    route_type = '3'
                self.routes.append(Route('%s_%s' % (line, direction), line,
                                        long_name, route_type, first, last))

    def get_trips(self, service_id, route_id, from_station, to_station, time):
        payload = dict(self.payload)
        payload['start'] = from_station
        payload['startID'] = self.get_stop_id(payload['start'])
        payload['ziel'] = to_station
        payload['zielID'] = self.get_stop_id(payload['ziel'])
        payload['datum'] = time.strftime('%d.%m.%Y')
        payload['zeit'] = time.strftime('%H:%M')
        req = requests.post(VRSInfo.API_BASE,
                data=payload,
                headers=self.headers)
        with file('current.html', 'w') as f:
            f.write(req.content)
        parser = etree.HTMLParser()
        tree = etree.parse(StringIO(req.content), parser)
        trips = []
        line_id = route_id.split('_')[0]
        while line_id.startswith('0'):
            line_id = line_id[1:]
        for rideno in range(1, 6):
            print "rideno: ", rideno
            check_line = tree.xpath("//div[@id='fahrt-%d']//table//tr/td[2]/table//tr/td[2]//b" % rideno)
            if not check_line:
                continue
            if check_line[0].text.strip() != line_id:
                logger.info('Skipping trip with bad line: %s (expected %s)' % (check_line[0].text, line_id))
                continue
            trs = tree.xpath("//div[@id='fahrt-%d']//table//tr/td[2]/table//tr/td[1]/div/table//tr" % rideno)
            stop_times = []
            first_time = None
            trip_id = None
            print "trs len: ", len(trs)            
            for i, tr in enumerate(trs):
                stations = tr.xpath("td[1]//table//tr[1]/td[2]")
                print "scraped stations: ", i, stations[0]
                station = stations[0].text.strip()
                print "scraped station name: ", i, station.encode('utf-8')                
                station_times = tr.xpath("td[2]//table//tr/td[2]")
                station_time = station_times[0].text
                print "scraped stations imes: ", i, station_times[0]
                station_time = [int(x) for x in station_time.split(':')]
                formatted_time = convert_time(*station_time, first_time=first_time)
                station_time = station_time[0] * 3600 + station_time[1] * 60
                if i == 0:
                    first_time = station_time
                    trip_id = "%s_%s_%s" % (route_id, service_id, station_time)
                    if trip_id in self.trip_cache:
                        logger.info('Skipping %s' % trip_id)
                        trip_id = None
                        break
                    self.trip_cache[trip_id] = True
                stop_id = self.get_stop_id(station)
                logger.info('Just got to %s (%s) at %s' % (station, stop_id, formatted_time))
                stop_times.append(StopTime(trip_id, formatted_time, formatted_time, stop_id, str(i)))
            else:
                if trip_id is not None:
                    # trip did not exist before
                    trips.append((trip_id, stop_times))
        return trips

    def get_stop_id(self, name):
        if name in self.stop_name_cache:
            return self.stop_name_cache[name]
        logger.info("Querying '%s'" % name)
        headers = dict(self.headers)
        headers['Content-Type'] = 'text/xml; charset=UTF-8;'
        payload = u'<?xml version="1.0" encoding="ISO-8859-1"?><Request><ObjectInfo><ObjectSearch><String>%(name)s</String><Classes><Stop/><Address/><POI/></Classes></ObjectSearch><Options><Output><SRSName>urn:adv:crs:ETRS89_UTM32</SRSName></Output></Options></ObjectInfo></Request>'
        payload = payload % {'name': u'%s, %s' % (name, self.city)}
        while True:
            try:
                req = requests.post(self.API_STOPS,
                        data=payload.encode('utf-8'),
                        headers=headers)
                tree = etree.parse(StringIO(req.content))
                stop_id = tree.xpath('//ID[1]')[0].text
                stop_name = tree.xpath('//Value[1]')[0].text
                break
            except requests.exceptions.ConnectionError:
                logger.info('Connection error! Retrying in 10 seconds...')
                time.sleep(10)
                continue
            except:
                print req.content
                import pdb; pdb.set_trace()
                break
        self.stop_name_cache[name] = stop_id
        logger.info("Got %s for %s" % (stop_id, stop_name))
        while True:
            try:
                req = requests.get((self.API_GEO % {'name': stop_name}).encode('utf-8'))
                break
            except requests.exceptions.ConnectionError:
                logger.info('Connection error! Retrying in 10 seconds...')
                time.sleep(10)
                continue
            else:
                if not '503 Service Unavailable' in req.content:
                    break
            logger.info('Service is down! Retrying in 5 seconds...')
            time.sleep(5)
        plist = req.content
        #plist_utf8 = 'NULL'
        #plist = unicode(plist)
        #print "Type of PLIST:", type(plist)
        #try:
        #    plist_utf8 = plist.encode('utf-8')
        #except UnicodeDecodeError:
        #    print "Not encoded ERROR: " + plist
        try:
            plist = plistlib.readPlistFromString(plist)
        except:
            print plist
            raise
        stop = Stop(stop_id, stop_name, plist[0].lat, plist[0].lon)
        if stop_id not in self.stop_cache:
            self.stop_cache[stop_id] = stop
            self.stops.append(stop)
        return self.stop_name_cache[name]


if __name__ == '__main__':
    info = VRSInfo('blub')
    try:
        info.start()
    except Exception as e:
        info.save()
        raise
    except KeyboardInterrupt:
        print "KeyboardInterrupt"
    info.save()
    import ipdb; ipdb.set_trace()
    # r = requests.post(VRSInfo.API_BASE, data=VRSInfo.payload)
    # file("test.html", 'w').write(r.content)
    # print r.content
    # parser = etree.HTMLParser()
    # tree = etree.parse(StringIO(r.content), parser)
    # trs = tree.xpath("//div[@id='fahrt-1']//table/tr/td[2]/table/tr/td[1]/div/table/tr")
    # import pdb; pdb.set_trace()