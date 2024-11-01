
import json
import os

from tqdm import tqdm
from pymongo import MongoClient
import pandas as pd

collection_name = "BDC-4-02_bha_run1_20240725163759"
base_path = r"C:\Users\Jyu20\Downloads\test"

channel_names = [
    ('RTHydro.SPPSIM', 'Pa')
]

time_series_names = [
    "RTHydro.PressureProfile"
]

alarm_names = [
    "AbnormalHoleCleaningIndex"
]

db_name = "TransientHydraulics"
uri = 'mongodb://localhost:27017'

client = MongoClient(uri)
database = client.get_database(db_name)
collection = database.get_collection(collection_name)

results = collection.find({})

channel_datas = {}
for c,u in channel_names:
    channel_datas[c] = {
        'time_stamp': ['unitless'],
        c: [u]
    }

ts_dirs = {}
for ts in time_series_names:
    ts_dirs[ts] = os.path.join(base_path, ts)
    os.makedirs(ts_dirs[ts])

alarm_dirs = {}
for a in alarm_names:
    alarm_dirs[a] = os.path.join(base_path, a)
    os.makedirs(alarm_dirs[a])


for i, r in tqdm(enumerate(results)):
    if i > 10000:
        break

    value = json.loads(r["v"])
    t = r['time'].strftime("%Y%m%d%H%M%S%f")
    t_c = r['time'].strftime("%Y-%m-%dT%H:%M:%S.%f")
    
    for _c in channel_datas.keys():
        if _c in value.keys():
            channel_datas[_c]['time_stamp'].append(t_c)
            channel_datas[_c][_c].append(value[_c])

    for _ts in ts_dirs.keys():
        if _ts in value.keys():
            save_path = os.path.join(ts_dirs[_ts], f"{t}.json")
            with open(save_path, 'w') as f:
                json.dump(value[_ts], f)

    for _a in alarm_dirs.keys():
        if _a in value.keys():
            save_path = os.path.join(alarm_dirs[_a], f"{t}.json")
            with open(save_path, 'w') as f:
                json.dump(value[_a], f)

for k,v in channel_datas.items():
    save_path = os.path.join(base_path, f'{k}.csv')
    pd.DataFrame(v).to_csv(save_path, index=False)