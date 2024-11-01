

import pickle
import json
import statistics
from pymongo import MongoClient
from datetime import datetime
import pandas as pd
from tqdm import tqdm


def query_tnd_simulation(uri, db_name, well_name):
    client = MongoClient(uri)
    database = client.get_database(db_name)
    collection = database.get_collection(well_name)

    # legacy, add time field
    collection.update_many({},
        [{
          "$set": {
             "time": {
                 '$toDate': "$k"
             }
          }        
        }]
    )

    pipeline = [
        {
            "$sort":{
            "time": 1
            }
        }
    ]

    points = {
        'time_key': [],
        'pickup_hkld': [],
        'slackoff_hkld': [],
        'high_indicator': []
    }

    results = collection.aggregate(pipeline)
    for i, r in tqdm(enumerate(results)):
        time = r['time']
        value = json.loads(r['v'])

        points['time_key'].append(str(time))

        if 'TND.HKLD_SO_MODEL' in list(value.keys()):
            points['slackoff_hkld'].append(float(value['TND.HKLD_SO_MODEL']))
        else:
            points['slackoff_hkld'].append(None)
        

        if 'TND.HKLD_PU_MODEL' in list(value.keys()):
            points['pickup_hkld'].append(float(value['TND.HKLD_PU_MODEL']))
        else:
            points['pickup_hkld'].append(None)

        if 'TND.HKLD_HIGH_IND' in list(value.keys()):
            points['high_indicator'].append(float(value['TND.HKLD_HIGH_IND']))
        else:
            points['high_indicator'].append(None)


    return pd.DataFrame(points)
