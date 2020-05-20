#!/usr/bin/python3

import argparse
import json
import os
import pytz
from datetime import datetime
from datetime import timedelta

from curwmysqladapter import MySQLAdapter, Station
from db_adapter.constants import CURW_OBS_HOST, CURW_OBS_PORT, CURW_OBS_USERNAME, CURW_OBS_PASSWORD, CURW_OBS_DATABASE
from db_adapter.base import get_Pool, destroy_Pool
from db_adapter.curw_obs.timeseries import Timeseries
from Utils import \
    generate_curw_obs_hash_id, \
    extract_n_push_waterlevel

def utc_to_sl(utc_dt):
    sl_timezone = pytz.timezone('Asia/Colombo')
    return utc_dt.replace(tzinfo=pytz.utc).astimezone(tz=sl_timezone)

try:
    pool = get_Pool(host=CURW_OBS_HOST, port=CURW_OBS_PORT, user=CURW_OBS_USERNAME, password=CURW_OBS_PASSWORD, db=CURW_OBS_DATABASE)
    ROOT_DIR = os.path.dirname(os.path.realpath(__file__))
    COMMON_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
    COMMON_DATE_FORMATSTRT = '%Y-%m-%d %H:%M:00'
    COMMON_DATE_FORMATEND = '%Y-%m-%d %H:%M:00'
    forceInsert = False

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config',
                        help='Configuration file that includes db configs and stations. Default is ./CONFIG.dist.json.')
    parser.add_argument('-f', '--force', action='store_true', help='Enables force insert.')
    args = parser.parse_args()

    print('\n\nCommandline Options:', args)

    if args.config:
        CONFIG = json.loads(open(os.path.join(ROOT_DIR, args.config)).read())
    else:
        CONFIG = json.loads(open(os.path.join(ROOT_DIR, './CONFIG.dist.json')).read())
    forceInsert = args.force


    water_level_stations = CONFIG['water_level_stations']
    stations = water_level_stations


    extract_from_db = CONFIG['extract_from']

    extract_adapter = MySQLAdapter(
        host=extract_from_db['MYSQL_HOST'],
        user=extract_from_db['MYSQL_USER'],
        password=extract_from_db['MYSQL_PASSWORD'],
        db=extract_from_db['MYSQL_DB'])

    # Prepare start and date times.
    now_date = utc_to_sl(datetime.now())
    # now_date = datetime.now()
    start_datetime_obj = now_date - timedelta(hours=2)
    end_datetime_obj = now_date

    start_datetime = start_datetime_obj.strftime(COMMON_DATE_FORMATSTRT)
    end_datetime = end_datetime_obj.strftime(COMMON_DATE_FORMATEND)

    # start_datetime = '2018-07-04 00:00:00'
    # end_datetime = '2018-07-31 00:00:00'

    for station in stations:
        print("**************** Station: %s, start_date: %s, end_date: %s **************"
              % (station['name'], start_datetime, end_datetime))

        variables = station['variables']
        if not isinstance(variables, list) or not len(variables) > 0:
            print("Station's variable list is not valid.", variables)
            continue

        station_name = station['name']
        latitude = station['station_meta'][2]
        longitude = station['station_meta'][3]
        units = station['units']
        unit_types = station['unit_type']
        description = station['description']

        for variable, unit, unit_type in zip(variables, units, unit_types):

            variable_vir = 'Waterlevel'
            unit_vir = 'm'
            unit_type_vir = 'Instantaneous'
            latitude_vir = 6.9569179
            longitude_vir = 79.8780352
            station_name_vir = 'Sedawatta Bridge DS'
            description_vir = 'Leecom water level guage, Leecom communication box'

            obs_hash_id_vir = generate_curw_obs_hash_id(pool, variable=variable_vir, unit=unit_vir, unit_type=unit_type_vir,
                                                          latitude=latitude_vir, longitude=longitude_vir,
                                                          station_name=station_name_vir, description=description_vir)

            obs_hash_id = generate_curw_obs_hash_id(pool, variable=variable, unit=unit, unit_type=unit_type,
                                                    latitude=latitude, longitude=longitude, station_name=station_name, description=description)
            TS = Timeseries(pool=pool)
            prev_end_date = TS.get_end_date(obs_hash_id)

            if prev_end_date is not None:
                start_datetime = (prev_end_date - timedelta(hours=2)).strftime(COMMON_DATE_FORMAT)

            if variable == 'Waterlevel':
                try:
                    extract_n_push_waterlevel(extract_adapter, station, start_datetime, end_datetime, pool, obs_hash_id, obs_hash_id_vir)
                except Exception as ex:
                    print("Error occured while pushing water-level", ex)


            else:
                print("Unknown variable type: %s" %variable)

except Exception as ex:
    print('Error occurred while extracting and pushing data:', ex)

finally:
    destroy_Pool(pool=pool)
