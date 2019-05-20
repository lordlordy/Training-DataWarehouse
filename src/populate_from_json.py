import json
import datetime
from datetime import date
from dateutil import parser
import sqlite3
import numpy as np
import pandas as pd
import math

JSON = 'json'
DB_COL = 'db_col'
TYPE = 'type'
FACTOR = 'factor'
REAL = 'REAL'
INTEGER = 'INTEGER'
BOOLEAN = 'BOOLEAN'
DEFAULT = 'DEFAULT'
AGGREGATION_METHOD = 'AggMethod'
SUM = 'Sum'
MEAN = 'Mean'
DAY = 'Day'
WEEK = 'Week'
MONTH = 'Month'

workout_map = [{JSON: 'km', DB_COL: 'km', TYPE: REAL, FACTOR: 1.0, DEFAULT: 0.0, AGGREGATION_METHOD: SUM},
               {JSON: 'km', DB_COL: 'miles', TYPE: REAL, FACTOR: 0.621371, DEFAULT: 0.0, AGGREGATION_METHOD: SUM},
               {JSON: 'tss', DB_COL: 'tss', TYPE: INTEGER, FACTOR: 1.0, DEFAULT: 0, AGGREGATION_METHOD: SUM},
               {JSON: 'rpe', DB_COL: 'rpe', TYPE: REAL, FACTOR: 1.0, DEFAULT: 0.0, AGGREGATION_METHOD: MEAN},
               {JSON: 'hr', DB_COL: 'hr', TYPE: INTEGER, FACTOR: 1.0, DEFAULT: 0, AGGREGATION_METHOD: MEAN},
               {JSON: 'watts', DB_COL: 'watts', TYPE: INTEGER, FACTOR: 1.0, DEFAULT: 0, AGGREGATION_METHOD: MEAN},
               {JSON: 'seconds', DB_COL: 'seconds', TYPE: INTEGER, FACTOR: 1.0, DEFAULT: 0, AGGREGATION_METHOD: SUM},
               {JSON: 'seconds', DB_COL: 'minutes', TYPE: INTEGER, FACTOR: 1.0 / 60.0, DEFAULT: 0, AGGREGATION_METHOD: SUM},
               {JSON: 'seconds', DB_COL: 'hours', TYPE: REAL, FACTOR: 1.0 / (60.0 * 60.0), DEFAULT: 0.0, AGGREGATION_METHOD: SUM},
               {JSON: 'ascentMetres', DB_COL: 'ascent_metres', TYPE: INTEGER, FACTOR: 1.0, DEFAULT: 0, AGGREGATION_METHOD: SUM},
               {JSON: 'ascentMetres', DB_COL: 'ascent_feet', TYPE: INTEGER, FACTOR: 3.28084, DEFAULT: 0, AGGREGATION_METHOD: SUM},
               {JSON: 'kj', DB_COL: 'kj', TYPE: INTEGER, FACTOR: 1.0, DEFAULT: 0, AGGREGATION_METHOD: SUM},
               {JSON: 'reps', DB_COL: 'reps', TYPE: INTEGER, FACTOR: 1.0, DEFAULT: 0, AGGREGATION_METHOD: SUM},
               {JSON: 'isRace', DB_COL: 'is_race', TYPE: BOOLEAN, FACTOR: 1.0, DEFAULT: 0, AGGREGATION_METHOD: SUM},
               {JSON: 'brick', DB_COL: 'brick', TYPE: BOOLEAN, FACTOR: 1.0, DEFAULT: 0, AGGREGATION_METHOD: SUM},
               {JSON: 'wattsEstimated', DB_COL: 'watts_estimated', TYPE: BOOLEAN, FACTOR: 1.0, DEFAULT: 0, AGGREGATION_METHOD: SUM},
               {JSON: 'cadence', DB_COL: 'cadence', TYPE: INTEGER, FACTOR: 1.0, DEFAULT: 0, AGGREGATION_METHOD: MEAN}]

workout_col_names = ','.join([m[DB_COL] for m in workout_map])
workout_zeroes = ','.join(['0' for _ in workout_map])
workout_col_creation = ','.join(f"{m[DB_COL]} {m[TYPE]} DEFAULT {m[DEFAULT]}" for m in workout_map)

day_map = [{JSON: 'fatigue', DB_COL: 'fatigue', TYPE: REAL, FACTOR: 1.0, DEFAULT: 0, AGGREGATION_METHOD: MEAN},
           {JSON: 'motivation', DB_COL: 'motivation', TYPE: REAL, FACTOR: 1.0, DEFAULT: 0, AGGREGATION_METHOD: MEAN},
           {JSON: 'sleep', DB_COL: 'sleep_seconds', TYPE: INTEGER, FACTOR: 60.0 * 60.0, DEFAULT: 0, AGGREGATION_METHOD: SUM},
           {JSON: 'sleep', DB_COL: 'sleep_minutes', TYPE: INTEGER, FACTOR: 60.0, DEFAULT: 0, AGGREGATION_METHOD: SUM},
           {JSON: 'sleep', DB_COL: 'sleep_hours', TYPE: REAL, FACTOR: 1.0, DEFAULT: 0.0, AGGREGATION_METHOD: SUM},
           {JSON: 'type', DB_COL: 'type', TYPE: 'VARCHAR(32)', FACTOR: 1.0, DEFAULT: "Normal"},
           {JSON: 'sleepQuality', DB_COL: 'sleep_quality', TYPE: 'VARCHAR(32)', FACTOR: 1.0, DEFAULT: 'Average'}]

day_col_names = ','.join(m[DB_COL] for m in day_map)
day_col_creation = ','.join(f"{m[DB_COL]} {m[TYPE]} DEFAULT {m[DEFAULT]}" for m in day_map)

calculated_map = [{DB_COL: 'ctl', TYPE: REAL, DEFAULT: 0.0, AGGREGATION_METHOD: MEAN},
                  {DB_COL: 'atl', TYPE: REAL, DEFAULT: 0.0, AGGREGATION_METHOD: MEAN},
                  {DB_COL: 'tsb', TYPE: REAL, DEFAULT: 0.0, AGGREGATION_METHOD: MEAN},
                  ]

calculated_col_creation = ','.join(f"{m[DB_COL]} {m[TYPE]} DEFAULT {m[DEFAULT]}" for m in calculated_map)

physiological_map = [{DB_COL: 'kg', TYPE: REAL},
                  {DB_COL: 'lbs', TYPE: REAL},
                  {DB_COL: 'fat_percentage', TYPE: REAL},
                  {DB_COL: 'resting_hr', TYPE: INTEGER},
                  {DB_COL: 'sdnn', TYPE: REAL},
                  {DB_COL: 'rmssd', TYPE: REAL},
                  ]

physiological_col_creation = ','.join(f"{m[DB_COL]} {m[TYPE]}" for m in physiological_map)

ACTIVITY = 'activityString'
ACTIVITY_TYPE = 'activityTypeString'
EQUIPMENT = 'equipmentName'
NOT_SET = 'Not Set'

CTL_DECAY_DAYS = 42
CTL_IMPACT_DAYS = 42
ATL_DECAY_DAYS = 7
ATL_IMPACT_DAYS = 7
CTL_DECAY = np.exp(-1 / CTL_DECAY_DAYS)
CTL_IMPACT = 1 - np.exp(-1 / CTL_IMPACT_DAYS)
ATL_DECAY = np.exp(-1 / ATL_DECAY_DAYS)
ATL_IMPACT = 1 - np.exp(-1 / ATL_IMPACT_DAYS)
table_names = set()

DB_NAME = 'training_data_warehouse.sqlite3'

def populate():

    conn = sqlite3.connect(DB_NAME)

    f = open('TrainingDiary.json')
    data = json.load(f)
    days = data['days']

    min_date = datetime.datetime.now().date()
    max_date = datetime.date(year=1, month=1, day=1)

    for d in days:
        date_time = parser.parse(d['iso8061DateString'])
        d_date = date(date_time.year, date_time.month, date_time.day)
        min_date = min(min_date, d_date)
        max_date = max(max_date, d_date)

        d_values = value_string_for_sql(d, day_map)

        if 'workouts' in d:
            save_workouts(conn, d_date, d['type'], d_values, d['workouts'])
        else:
            execute_day_sql(conn, d_date, d['type'], day_col_names, d_values,
                            activity='All', activity_type='All', equipment_name='All')

    #    fill in gaps
        for t in table_names:
            if not day_exists(d_date, t, conn):
                execute_day_sql(conn, d_date, d['type'], day_col_names, d_values, table_name=t)

    populate_kg_fat_percent(conn, data, min_date, max_date)
    populate_hr_sdnn_rmssd(conn, data, min_date, max_date)

    conn.commit()
    conn.close()


def populate_kg_fat_percent(conn, data, min_date, max_date):
    kg_dates = []
    kg_array = []
    fat_dates = []
    fat_percent = []
    if 'weights' in data:
        for d in data['weights']:
            date_time = parser.parse(d['iso8061DateString'])
            d_date = date(date_time.year, date_time.month, date_time.day)
            kg = round(float(d['kg']), 1)
            fat = round(float(d['fatPercent']), 1)
            if kg > 0:
                kg_dates.append(d_date)
                kg_array.append(kg)
            if fat > 0:
                fat_dates.append(d_date)
                fat_percent.append(fat)
    kg_series = pd.Series(kg_array, index=pd.to_datetime(kg_dates))
    kg_series = kg_series.reindex(index=pd.date_range(min_date, max_date)).interpolate(method='linear')
    fat_series = pd.Series(fat_percent, index=pd.to_datetime(fat_dates))
    fat_series = fat_series.reindex(index=pd.date_range(min_date, max_date)).interpolate(method='linear')

    for table in table_list(conn):
        for d, value in kg_series.iteritems():
            if math.isnan(value):
                value = 0
            lbs = round(value * 2.20462, 1)
            sql_str = f'UPDATE {table} SET kg={round(value,1)}, lbs={lbs} WHERE date="{str(d.date())}"'
            conn.cursor().execute(sql_str)
        for d, value in fat_series.iteritems():
            if math.isnan(value):
                value = 0
            sql_str = f'UPDATE {table} SET fat_percentage={round(value,1)} WHERE date="{str(d.date())}"'
            conn.cursor().execute(sql_str)


def populate_hr_sdnn_rmssd(conn, data, min_date, max_date):
    hr_dates = []
    hr_array = []
    sdnn_dates = []
    sdnn_array = []
    rmssd_dates = []
    rmssd_array = []
    if 'physiologicals' in data:
        for d in data['physiologicals']:
            date_time = parser.parse(d['iso8061DateString'])
            d_date = date(date_time.year, date_time.month, date_time.day)
            hr = sdnn = rmssd = 0
            if d['restingHR'] is not None:
                hr = int(d['restingHR'])
            if d['restingSDNN'] is not None:
                sdnn = round(float(d['restingSDNN']), 1)
            if d['restingRMSSD'] is not None:
                rmssd = round(float(d['restingRMSSD']), 1)
            if hr > 0:
                hr_dates.append(d_date)
                hr_array.append(hr)
            if sdnn > 0:
                sdnn_dates.append(d_date)
                sdnn_array.append(sdnn)
            if rmssd > 0:
                rmssd_dates.append(d_date)
                rmssd_array.append(sdnn)
    hr_series = pd.Series(hr_array, index=pd.to_datetime(hr_dates))
    hr_series = hr_series.reindex(index=pd.date_range(min_date, max_date)).interpolate(method='linear')
    sdnn_series = pd.Series(sdnn_array, index=pd.to_datetime(sdnn_dates))
    sdnn_series = sdnn_series.reindex(index=pd.date_range(min_date, max_date)).interpolate(method='linear')
    rmssd_series = pd.Series(sdnn_array, index=pd.to_datetime(rmssd_dates))
    rmssd_series = rmssd_series.reindex(index=pd.date_range(min_date, max_date)).interpolate(method='linear')

    for table in table_list(conn):
        for d, value in hr_series.iteritems():
            if value is None or math.isnan(value):
                value = 0
            sql_str = f'UPDATE {table} SET resting_hr={value} WHERE date="{str(d.date())}"'
            conn.cursor().execute(sql_str)
        for d, value in sdnn_series.iteritems():
            if value is None or math.isnan(value):
                value = 0
            sql_str = f'UPDATE {table} SET sdnn={round(value,1)} WHERE date="{str(d.date())}"'
            conn.cursor().execute(sql_str)
        for d, value in rmssd_series.iteritems():
            if value is None or math.isnan(value):
                value = 0
            sql_str = f'UPDATE {table} SET rmssd={round(value,1)} WHERE date="{str(d.date())}"'
            conn.cursor().execute(sql_str)



def table_list(conn):
    sql_str = f'SELECT table_name FROM Tables'
    results = conn.cursor().execute(sql_str)
    return [r[0] for r in results]


def calculate_all_tsb():
    conn = sqlite3.connect(DB_NAME)

    for r in table_list(conn):
        calculate_tsb(conn, r)

    conn.commit()


def calculate_tsb(conn, table_name):
    sql_str = f'SELECT id, tss FROM {table_name} ORDER BY date'
    atl = ctl = 0.0

    results = conn.cursor().execute(sql_str)
    for r in results:
        id = r[0]
        tss = r[1]
        ctl = tss * CTL_IMPACT + ctl * CTL_DECAY
        atl = tss * ATL_IMPACT + atl * ATL_DECAY
        tsb = ctl - atl
        sql_str = f'UPDATE {table_name} SET ctl={ctl}, atl={atl}, tsb={tsb} WHERE id={id}'
        conn.cursor().execute(sql_str)


def create_and_populate_agg_tables(period):
    agg_str, insert_str = create_agg_and_insert_str_for_sql()
    conn = sqlite3.connect(DB_NAME)
    sql_str = f'SELECT activity, activity_type, equipment FROM Tables WHERE period="{DAY}"'
    tables = conn.cursor().execute(sql_str)

    for t in tables:
        create_and_populate_agg_table(conn, period, t[0], t[1], t[2], agg_str, insert_str)

    conn.commit()


def create_and_populate_agg_table(conn, period, activity, activity_type, equipment_name, aggregation_str, insert_str):
    table_name = create_table(period, activity, activity_type, equipment_name, conn)
    day_table = f'{DAY}_{activity}_{activity_type}_{equipment_name}'

    if period == WEEK:
        agg = 'year_week'
    elif period == MONTH:
        agg = 'year_month'
    else:
        print(f'{period} unsupported')
        return

    sql_str = f'''
            SELECT {agg}, {aggregation_str}
            FROM {day_table}
            GROUP BY {agg}
    '''
    result = conn.cursor().execute(sql_str)
    for r in result:
        sql_str = f'''
            INSERT INTO {table_name}
            ({agg}, {insert_str})
            VALUES
            {r}
        '''
        conn.cursor().execute(sql_str)


def save_workouts(conn, d_date, d_type, d_values, workouts):
    aggregation_keys = [[ACTIVITY, ACTIVITY_TYPE, EQUIPMENT],
                        [ACTIVITY_TYPE, EQUIPMENT],
                        [ACTIVITY, EQUIPMENT],
                        [ACTIVITY, ACTIVITY_TYPE],
                        [EQUIPMENT],
                        [ACTIVITY],
                        [ACTIVITY_TYPE],
                        []
                        ]
    for a in aggregation_keys:
        agg_workouts = aggregate_workouts(workouts, a)
        for w in agg_workouts:
            save_workout(conn, d_date, d_type, d_values, w, a)


def save_workout(conn, d_date, d_type, d_values, workout, keys):

    a = 'All'
    at = 'All'
    e_name = 'All'

    if ACTIVITY in keys:
        a = workout[ACTIVITY]
    if ACTIVITY_TYPE in keys:
        at = workout[ACTIVITY_TYPE]
    if EQUIPMENT in keys:
        e_name = workout[EQUIPMENT].replace(' ', '')

    _ = create_table(DAY, a, at, e_name, conn)

    w_values = value_string_for_sql(workout, workout_map)

    col_names = f'{day_col_names}, {workout_col_names}'
    d_values = f'{d_values}, {w_values}'
    execute_day_sql(conn, d_date, d_type, col_names, d_values, activity=a, activity_type=at, equipment_name=e_name)


# take workouts and combine those that have same Activity:Type:Equipment
def aggregate_workouts(workouts, keys):
    agg_w = dict()
    for w in workouts:
        key = 'key'
        if len(keys) > 0:
            key = ':'.join([w[k] for k in keys])
        if EQUIPMENT in keys and (w[EQUIPMENT] == NOT_SET or w[EQUIPMENT] == ''):
            continue
        if key in agg_w:
            agg_w[key].append(dict(w))
        else:
            agg_w[key] = [dict(w)]
    result = []
    for w_array in agg_w.values():
        if len(w_array) == 1:
            result.append(w_array[0])
        else:
            d = dict()
            d[ACTIVITY] = w_array[0][ACTIVITY]
            d[ACTIVITY_TYPE] = w_array[0][ACTIVITY_TYPE]
            d[EQUIPMENT] = w_array[0][EQUIPMENT]
            for map in workout_map:
                r = 0
                for w in w_array:
                    if map[AGGREGATION_METHOD] == SUM:
                        r += w[map[JSON]]
                    else:
                        r += w[map[JSON]] * w['seconds']
                d[map[JSON]] = r
            for map in workout_map:
                if map[AGGREGATION_METHOD] == MEAN:
                    d[map[JSON]] = d[map[JSON]] / d['seconds']
                    if map[TYPE] == INTEGER:
                        d[map[JSON]] = int(d[map[JSON]])
            result.append(d)

    return result


def value_string_for_sql(dictionary, json_map):
    d_value_array = []
    for m in json_map:
        if m[TYPE] == INTEGER:
            d_value_array.append(str(int(round(float(dictionary[m[JSON]]) * m[FACTOR], 0))))
        elif m[TYPE] == REAL:
            d_value_array.append(str(round(float(dictionary[m[JSON]]) * m[FACTOR],2)))
        elif m[TYPE] == BOOLEAN:
            if dictionary[m[JSON]] == 0:
                d_value_array.append('0')
            else:
                d_value_array.append('1')
        else:
            d_value_array.append(f"'{dictionary[m[JSON]]}'")

    return ','.join([s for s in d_value_array])


def day_exists(d, table, conn):
    sql_str = f'''
            SELECT id FROM {table} WHERE date="{d}"
    '''
    result = conn.cursor().execute(sql_str)
    return len(result.fetchall()) > 0


def create_table(period, activity, activity_type, equipment_name, conn):

    table_name = f'{period}_{activity}_{activity_type}_{equipment_name}'

    sql_str = f'''
    
        CREATE TABLE {table_name}
        (id INTEGER PRIMARY KEY AUTOINCREMENT,
        date DATE UNIQUE,
        year_week VARCHAR(16),
        year_month VARCHAR(16),
        day_of_week VARCHAR(8),
        month VARCHAR(8),
        day_type VARCHAR(16),
        {day_col_creation},
        {workout_col_creation},
        {calculated_col_creation},
        {physiological_col_creation})
        '''

    try:
        conn.cursor().execute(sql_str)
        conn.commit()
        table_names.add(table_name)

        sql_str = f"""

            INSERT INTO Tables
            (period, activity, activity_type, equipment, table_name)
            VALUES
            ('{period}',
            '{activity}',
            '{activity_type}',
            '{equipment_name}',
            '{table_name}'
            )

        """
        conn.cursor().execute(sql_str)
        conn.commit()

    except Exception as e:
        pass

    return table_name


def execute_day_sql(conn, d_date, d_type, col_names, values, activity='All', activity_type='All', equipment_name='All', table_name=None):

    t_name = table_name
    if table_name is None:
        t_name = f'{DAY}_{activity}_{activity_type}_{equipment_name}'

    d_month = f'{d_date.year}-{d_date.strftime("%b")}'
    d_week = f'{d_date.year}-{d_date.isocalendar()[1]}'
    d_day = d_date.strftime('%a')
    month = d_date.strftime('%b')

    sql_str = f'''

        INSERT INTO {t_name}
        (date, year_week, year_month, day_of_week, month, day_type, {col_names})
        VALUES
        (
        '{d_date}',
        '{d_week}',
        '{d_month}',
        '{d_day}',
        '{month}',
        '{d_type}',
        {values}
        )
    '''

    try:
        conn.cursor().execute(sql_str)
        # conn.commit()
    except Exception as e:
        print(e)

def create_agg_and_insert_str_for_sql():
    aggregate_array = ['MAX(date)']
    insert_array = ['date']
    for m in (workout_map + day_map + calculated_map):
        if AGGREGATION_METHOD in m:
            if m[AGGREGATION_METHOD] == SUM:
                aggregate_array.append(f'SUM({m[DB_COL]})')
                insert_array.append(m[DB_COL])
            elif m[AGGREGATION_METHOD] == MEAN:
                aggregate_array.append(f'AVG({m[DB_COL]})')
                insert_array.append(m[DB_COL])

    return ','.join(aggregate_array), ','.join(insert_array)



if __name__ == '__main__':
    start = datetime.datetime.now()
    print('Basic day info...')
    populate()
    print(f'DONE in {datetime.datetime.now() - start}')

    start = datetime.datetime.now()
    print('Calculating TSB ...')
    calculate_all_tsb()
    print(f'DONE in {datetime.datetime.now() - start}')

    # start = datetime.datetime.now()
    # print('Creating weekly tables ...')
    # create_and_populate_agg_tables(WEEK)
    # print(f'DONE in {datetime.datetime.now() - start}')
    #
    # start = datetime.datetime.now()
    # print('Creating monthly tables ...')
    # create_and_populate_agg_tables(MONTH)
    # print(f'DONE in {datetime.datetime.now() - start}')

    # f = open('TrainingDiary.json')
    # data = json.load(f)
    # populate_kg_fat_percent(data)
