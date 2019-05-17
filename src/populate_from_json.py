import json
from datetime import date
from dateutil import parser
import sqlite3

JSON = 'json'
DB_COL = 'db_col'
TYPE = 'type'
FACTOR = 'factor'
REAL = 'REAL'
INTEGER = 'INTEGER'
BOOLEAN = 'VARCHAR(8)'      # storing Boolean as String in Warehouse
DEFAULT = 'DEFAULT'
AGGREGATION_METHOD = 'AggMethod'
SUM = 'Sum'
MEAN = 'Mean'

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
               {JSON: 'isRace', DB_COL: 'is_race', TYPE: BOOLEAN, FACTOR: 1.0, DEFAULT: 'False', AGGREGATION_METHOD: SUM},
               {JSON: 'brick', DB_COL: 'brick', TYPE: BOOLEAN, FACTOR: 1.0, DEFAULT: 'False', AGGREGATION_METHOD: SUM},
               {JSON: 'wattsEstimated', DB_COL: 'watts_estimated', TYPE: BOOLEAN, FACTOR: 1.0, DEFAULT: 'False', AGGREGATION_METHOD: SUM},
               {JSON: 'cadence', DB_COL: 'cadence', TYPE: INTEGER, FACTOR: 1.0, DEFAULT: 0, AGGREGATION_METHOD: MEAN}]

workout_col_names = ','.join([m[DB_COL] for m in workout_map])
workout_zeroes = ','.join(['0' for _ in workout_map])
workout_col_creation = ','.join(f"{m[DB_COL]} {m[TYPE]} DEFAULT {m[DEFAULT]}" for m in workout_map)

day_map = [{JSON: 'fatigue', DB_COL: 'fatigue', TYPE: REAL, FACTOR: 1.0, DEFAULT: 0},
           {JSON: 'motivation', DB_COL: 'motivation', TYPE: REAL, FACTOR: 1.0, DEFAULT: 0},
           {JSON: 'sleep', DB_COL: 'sleep_seconds', TYPE: INTEGER, FACTOR: 60.0 * 60.0, DEFAULT: 0},
           {JSON: 'sleep', DB_COL: 'sleep_minutes', TYPE: INTEGER, FACTOR: 60.0, DEFAULT: 0},
           {JSON: 'sleep', DB_COL: 'sleep_hours', TYPE: REAL, FACTOR: 1.0, DEFAULT: 0.0},
           {JSON: 'type', DB_COL: 'type', TYPE: 'VARCHAR(32)', FACTOR: 1.0, DEFAULT: "Normal"},
           {JSON: 'sleepQuality', DB_COL: 'sleep_quality', TYPE: 'VARCHAR(32)', FACTOR: 1.0, DEFAULT: 'Average'}]

day_col_names = ','.join(m[DB_COL] for m in day_map)
day_col_creation = ','.join(f"{m[DB_COL]} {m[TYPE]} DEFAULT {m[DEFAULT]}" for m in day_map)

ACTIVITY = 'activityString'
ACTIVITY_TYPE = 'activityTypeString'
EQUIPMENT = 'equipmentName'
NOT_SET = 'Not Set'

table_names = set()

def populate():

    conn = sqlite3.connect('training_data_warehouse.db')

    f = open('TrainingDiary.json')
    data = json.load(f)
    days = data['days']

    for d in days:
        date_time = parser.parse(d['iso8061DateString'])
        d_date = date(date_time.year, date_time.month, date_time.day)
        d_values = value_string_for_sql(d, day_map)

        if 'workouts' in d:
            save_workouts(conn, d_date, d_values, d['workouts'])
        else:
            sql_str = f'''
                
                INSERT INTO DAY_All_All_All
                (date, {day_col_names})
                VALUES
                (
                '{d_date}',
                {d_values}
                )
            '''
            try:
                conn.cursor().execute(sql_str)
            except Exception as e:
                print(e)
                pass

    #    fill in gaps
        for t in table_names:
            if not day_exists(d_date, t, conn):
                sql_str = f'''

                    INSERT INTO {t}
                    (date, {day_col_names})
                    VALUES
                    (
                    '{d_date}',
                    {d_values}
                    )
                '''
                try:
                    conn.cursor().execute(sql_str)
                except Exception as e:
                    print(e)
                    pass


    conn.commit()

def save_workouts(conn, d_date, d_values, workouts):
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
            save_workout(conn, d_date, d_values, w, a)


def save_workout(conn, d_date, d_values, workout, keys):
    c = conn.cursor()

    a = 'All'
    at = 'All'
    e_name = 'All'

    if ACTIVITY in keys:
        a = workout[ACTIVITY]
    if ACTIVITY_TYPE in keys:
        at = workout[ACTIVITY_TYPE]
    if EQUIPMENT in keys:
        e_name = workout[EQUIPMENT].replace(' ', '')

    table_name = f'DAY_{a}_{at}_{e_name}'
    day_key = f'{d_date}:{table_name}'

    _ = create_table(table_name, conn)

    if day_exists(d_date, table_name, conn):
        #  this needs to aggregate
        #  todo fix
        print(f'Day EXISTS: {d_date} {table_name}')

    values = value_string_for_sql(workout, workout_map)

    sql_str = f"""

        INSERT INTO {table_name}
        (date, {day_col_names},  {workout_col_names})
        VALUES
        ('{d_date}',
        {d_values},
        {values}
        )

    """

    try:
        c.execute(sql_str)
    except TypeError as te:
        print(te)
    except sqlite3.OperationalError as e:
        print(e)
        print(sql_str)
    except sqlite3.IntegrityError as e:
        print(e)
        print(day_key)

    if not table_exists(table_name, conn):

        sql_str = f"""
    
            INSERT INTO Tables
            (activity, activity_type, equipment, table_name)
            VALUES
            ('{a}',
            '{at}',
            '{e_name}',
            '{table_name}'
            )
    
        """
        try:
            c.execute(sql_str)
        except Exception as e:
            print(e)


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
                d_value_array.append("'False'")
            else:
                d_value_array.append("'True'")
        else:
            d_value_array.append(f"'{dictionary[m[JSON]]}'")

    return ','.join([s for s in d_value_array])


def day_exists(d, table, conn):
    sql_str = f'''
            SELECT id FROM {table} WHERE date="{d}"
    '''
    result = conn.cursor().execute(sql_str)
    return len(result.fetchall()) > 0


def table_exists(table_name, conn):
    sql_str = f'''
            SELECT id FROM Tables WHERE table_name="{table_name}"
    '''
    result = conn.cursor().execute(sql_str)
    return len(result.fetchall()) > 0


def create_table(table_name, conn):

    sql_str = f'''
    
        CREATE TABLE {table_name}
        (id INTEGER PRIMARY KEY AUTOINCREMENT,
        date DATE UNIQUE,
        {day_col_creation},
        {workout_col_creation})
    
        '''

    try:
        conn.cursor().execute(sql_str)
        conn.commit()
        table_names.add(table_name)
        return True
    except Exception as e:
        pass




if __name__ == '__main__':
    populate()
    # conn = sqlite3.connect('training_data_warehouse.db')
    # tables = f'''
    #     SELECT table_name FROM Tables
    #
    # '''
    # result = conn.cursor().execute(tables)
    #
    # for t in result.fetchall():
    #     sql_str = f'''
    #         SELECT * FROM {t[0]}
    #         WHERE date='2016-02-02'
    #     '''
    #     try:
    #         result = conn.cursor().execute(sql_str)
    #         data = result.fetchall()
    #         if len(data)>0:
    #             if data[0][14] is not None and data[0][14] > 0:
    #                 # print(data[0])
    #                 print(f'{data[0][1]}: {data[0][14]} \t~ {t[0]}')
    #     except Exception as e:
    #         print(e)
    #         pass
