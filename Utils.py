import copy
import decimal
import traceback
import pandas as pd
from datetime import datetime, timedelta
import pandas as pd


from db_adapter.curw_obs.station import StationEnum, get_station_id, add_station, update_description
from db_adapter.curw_obs.variable import get_variable_id, add_variable
from db_adapter.curw_obs.unit import get_unit_id, add_unit, UnitType
from db_adapter.curw_obs.timeseries import Timeseries

from curwmysqladapter import TimeseriesGroupOperation, Station, Data

CURW_WEATHER_STATION = 'CUrW_WeatherStation'
CURW_WATER_LEVEL_STATION = 'CUrW_WaterLevelGauge'
CURW_CROSS_SECTION = 'CUrW_CrossSection'


timeseries_meta_struct = {
    'station': '',
    'variable': '',
    'unit': '',
    'type': '',
    'source': '',
    'name': ''
}

def _waterlevel_timeseries_processor(timeseries, mean_sea_level=None, waterLevel_min=None, waterLevel_max=None):
    # print("**_waterlevel_timeseries_processor**")
    # print(timeseries)
    if timeseries is None or len(timeseries) <= 0:
        return []

    if mean_sea_level is None or not isinstance(mean_sea_level, (float, int)):
        raise ValueError('Invalid mean_sea_level. Should be a real number.')

    new_timeseries = []
    if decimal.Decimal(mean_sea_level) > 30.00:
        for tms_step in timeseries:
            wl = decimal.Decimal(mean_sea_level) - tms_step[1]
            # Waterlevel should be in between -1 and 3
            if decimal.Decimal(waterLevel_min) <= wl <= decimal.Decimal(waterLevel_max):
                new_timeseries.append([tms_step[0], wl])
    else:
        for tms_step in timeseries:
            wl = decimal.Decimal(mean_sea_level) - tms_step[1]
            # Waterlevel should be in between -1 and 3
            if decimal.Decimal(waterLevel_min) <= wl <= decimal.Decimal(waterLevel_max):
                new_timeseries.append([tms_step[0], wl])
    # print("New Timeseries:")
    # print(new_timeseries)
    return new_timeseries


def _extract_n_push(extract_adapter, station, start_date, end_date, pool, obs_hash_id, obs_hash_id_vir,
                        timeseries_meta, group_operation,
                        timeseries_processor=None, **timeseries_processor_kwargs):
    # If there is no timeseries-id in the extracting DB then just return without doing anything.

    timeseries_id = extract_adapter.get_event_id(timeseries_meta)
    # print("*****************")
    # print(timeseries_id)
    # print(start_date)
    # print(end_date)
    # print(group_operation)
    # print("*****************")
    if timeseries_id is None:
        print("No timeseries for the Precipitation of station_Id: %s in the extracting DB."
              % station['stationId'])
        return False

    timeseries = []
    if timeseries_processor is not None:
        # print("1")
        timeseries = timeseries_processor(
            extract_adapter.extract_grouped_time_series(timeseries_id, start_date, end_date, group_operation),
            **timeseries_processor_kwargs
        )
        # print(timeseries)
    else:
        # print("2")
        timeseries = extract_adapter.extract_grouped_time_series(timeseries_id, start_date, end_date, group_operation)
        # print(timeseries)

    # print(timeseries,list)
    if not isinstance(timeseries, list) or len(timeseries) <= 0:
        print("No value in the timeseries for the %s of station_Id: %s in the extracting DB."
              % (timeseries_meta['variable'], station['stationId']))
        return False

    if station['stationId'] == 'curw_wl_test':
        obs_hash_id = obs_hash_id_vir

    #insert extracted time series to the curwobs db
    inserted_rows = insert_timeseries(pool=pool, timeseries=timeseries, tms_id=obs_hash_id)
    print("Inserted tmieseries length {} values successfully...".format(len(timeseries)))



def extract_n_push_waterlevel(extract_adapter, station, start_date, end_date, pool, obs_hash_id, obs_hash_id_vir):
    if 'mean_sea_level' not in station.keys():
        raise AttributeError('Attribute mean_sea_level is required.')
    msl = 5.676
    wl_min = -1.00
    wl_max = 3.00

    #if station['stationId'] == 'curw_wl_test':
        #msl = 5.676
        #wl_min = -1.00
        #wl_max = 3.00

    # Create even metadata. Event metadata is used to create timeseries id (event_id) for the timeseries.
    timeseries_meta = copy.deepcopy(timeseries_meta_struct)
    timeseries_meta['station'] = station['name']
    timeseries_meta['variable'] = 'Waterlevel'
    timeseries_meta['unit'] = 'm'
    timeseries_meta['type'] = station['type']
    timeseries_meta['source'] = station['source']
    timeseries_meta['name'] = station['run_name']

    print("#############Extracting and water level of Station: %s###############" % station['name'])

    _extract_n_push(
        extract_adapter,
        station,
        start_date,
        end_date, pool, obs_hash_id, obs_hash_id_vir,
        timeseries_meta,
        TimeseriesGroupOperation.mysql_5min_avg,
        timeseries_processor=_waterlevel_timeseries_processor, mean_sea_level=msl, waterLevel_min=wl_min, waterLevel_max=wl_max)

def generate_curw_obs_hash_id(pool, variable, unit, unit_type, latitude, longitude, station_type=None,
                              station_name=None, description=None, append_description=False, start_date=None):

    """
    Generate corresponding curw_obs hash id for a given curw observational station
    :param pool: databse connection pool
    :param variable: str: e.g. "Precipitation"
    :param unit: str: e.g. "mm"
    :param unit_type: str: e.g. "Accumulative"
    :param latitude: float: e.g. 6.865576
    :param longitude: float: e.g. 79.958181
    :param station_type: str: enum:  'CUrW_WeatherStation' | 'CUrW_WaterLevelGauge' | 'CUrW_CrossSection'
    :param station_name: str: "Urumewella"
    :param description: str: "A&T Communication Box, Texas Standard Rain Gauge"
    :param append_description: bool:
    :param start_date: str: e.g."2019-07-01 00:00:00" ; the timestamp of the very first entry of the timeseries

    :return: new curw_obs hash id
    """
    # if run_name not in ('A&T Labs', 'Leecom', 'CUrW IoT'):
    #     print("This function is dedicated for generating curw_obs hash ids only for 'A&T Labs', 'Leecom', 'CUrW IoT' "
    #           "weather stations")
    #     exit(1)

    try:

        lat = '%.6f' % float(latitude)
        lon = '%.6f' % float(longitude)
        meta_data = {
                'unit': unit, 'unit_type': unit_type,
                'latitude': lat, 'longitude': lon
                }

        if variable == "Waterlevel":
            variable = "WaterLevel"

        meta_data['variable'] = variable

        if station_type and station_type in (CURW_WATER_LEVEL_STATION, CURW_WEATHER_STATION, CURW_CROSS_SECTION):
            station_type = StationEnum.getType(station_type)
        else:
            if variable=="WaterLevel":
                station_type = StationEnum.CUrW_WaterLevelGauge
            elif variable=="CrossSection":
                station_type = StationEnum.CUrW_CrossSection
            else:
                station_type = StationEnum.CUrW_WeatherStation

        meta_data['station_type'] = StationEnum.getTypeString(station_type)

        unit_id = get_unit_id(pool=pool, unit=unit, unit_type=UnitType.getType(unit_type))

        if unit_id is None:
            add_unit(pool=pool, unit=unit, unit_type=UnitType.getType(unit_type))
            unit_id = get_unit_id(pool=pool, unit=unit, unit_type=UnitType.getType(unit_type))

        variable_id = get_variable_id(pool=pool, variable=variable)

        if variable_id is None:
            add_variable(pool=pool, variable=variable)
            variable_id = get_variable_id(pool=pool, variable=variable)

        station_id = get_station_id(pool=pool, latitude=lat, longitude=lon, station_type=station_type)

        if station_id is None:
            add_station(pool=pool, name=station_name, latitude=lat, longitude=lon,
                    station_type=station_type)
            station_id = get_station_id(pool=pool, latitude=lat, longitude=lon,
                    station_type=station_type)
            if description:
                update_description(pool=pool, id_=station_id, description=description, append=False)

        elif append_description:
            if description:
                update_description(pool=pool, id_=station_id, description=description, append=True)

        TS = Timeseries(pool=pool)

        tms_id = TS.get_timeseries_id_if_exists(meta_data=meta_data)

        meta_data['station_id'] = station_id
        meta_data['variable_id'] = variable_id
        meta_data['unit_id'] = unit_id

        if tms_id is None:
            tms_id = TS.generate_timeseries_id(meta_data=meta_data)
            meta_data['tms_id'] = tms_id
            TS.insert_run(run_meta=meta_data)
            if start_date:
                TS.update_start_date(id_=tms_id, start_date=start_date)

        return tms_id

    except Exception:
        traceback.print_exc()
        print("Exception occurred while inserting run entries to curw_obs run table and making hash mapping")


def insert_timeseries(pool, timeseries, tms_id, end_date=None):

    """
    Insert timeseries to curw_obs database
    :param pool: database connection pool
    :param timeseries: list of [time, value] lists
    :param end_date: str: timestamp of the latest data
    :param tms_id: str: curw_obs timeseries (hash) id
    :return:
    """
    new_timeseries = []
    for t in [i for i in timeseries]:
        if len(t) > 1:
            # Insert EventId in front of timestamp, value list
            t.insert(0, tms_id)
            new_timeseries.append(t)
        else:
            print('Invalid timeseries data:: %s', t)

    if end_date is None:
        end_date = new_timeseries[-1][1]

    try:

        ts = Timeseries(pool=pool)

        ts.insert_data(timeseries=new_timeseries, upsert=True)
        ts.update_end_date(id_=tms_id, end_date=end_date)

    except Exception as e:
        traceback.print_exc()
        print("Exception occurred while pushing timeseries for tms_id {} to curw_obs".format(tms_id))


def update_station_description_by_id(pool, station_id, description, append_description=True):

    try:

        if append_description:
            update_description(pool=pool, id_=station_id, description=description, append=True)
        else:
            update_description(pool=pool, id_=station_id, description=description, append=False)

    except Exception as e:
        traceback.print_exc()
        print("Exception occurred while updating description for station id {}.".format(station_id))


def update_station_description(pool, latitude, longitude, station_type, description, append_description=True):

    lat = '%.6f' % float(latitude)
    lon = '%.6f' % float(longitude)

    try:

        if station_type and station_type in (CURW_WATER_LEVEL_STATION, CURW_WEATHER_STATION):
            station_type = StationEnum.getType(station_type)
        else:
            print("Station type cannot be recognized")
            exit(1)

        station_id = get_station_id(pool=pool, latitude=lat, longitude=lon, station_type=station_type)

        if append_description:
            update_description(pool=pool, id_=station_id, description=description, append=True)
        else:
            update_description(pool=pool, id_=station_id, description=description, append=False)

    except Exception as e:
        traceback.print_exc()
        print("Exception occurred while updating description for station id {}.".format(station_id))
