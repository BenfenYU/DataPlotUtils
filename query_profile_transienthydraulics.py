
import pickle
import json
import statistics
from pymongo import MongoClient
from datetime import datetime
import pandas as pd
from tqdm import tqdm

well_name = '3s-617_Run3'
ecd_channel_data = fr'C:\NotOneDrive\Data\{well_name}\Channel_ECD.csv'
input_channel_data = fr'C:\NotOneDrive\Data\{well_name}\Channels.csv'
save_path = fr'C:\NotOneDrive\Data\merged_input_output_channels\{well_name}_bha_TransientHydraulics_profile.json'
db_name = 'TransientHydraulics'
uri = 'mongodb://localhost:27017'


def get_algo_output_channels():
   client = MongoClient(uri)
   database = client.get_database(db_name)
   collection = database.get_collection(well_name)

   # legacy, add time field
   # collection.update_many({},
   #     [{
   #       "$set": {
   #          "time": {
   #              '$toDate': "$k"
   #          }
   #       }        
   #     }]
   # )

   pipeline = [
      {
         "$sort":{
            "time": 1
         }
      }
   #    {
   #        '$match': {
   #            'time':{
   #             '$gt': datetime(2023, 11, 18)
   #            }
   #        }
   #    }
   ]

   points = {
      'time_key': [],
      'hci': [],
      'cutting_concentration': [],
      'cutting_bed_height': [],
      'md': []
   }

   results = collection.aggregate(pipeline)
   for i, r in tqdm(enumerate(results)):
      time = r['time']
      value = json.loads(r['v'])

      if 'RTHydro.HoleCleaningProfile' in list(value.keys()):
         hci_profile = json.loads(value['RTHydro.HoleCleaningProfile'])
         hcis = list(map(lambda x: float(x), hci_profile['HCI'] ))
         cbhs = list(map(lambda x: float(x), hci_profile['CuttingBedHeightP50'] ))
         ccs = list(map(lambda x: float(x), hci_profile['CuttingConcentrationP50'], ))
         mds = list(map(lambda x: float(x), hci_profile['MD'], ))
      else:
         hcis = []
         cbhs = []
         ccs = []
         mds = []

      points['time_key'].append(str(time))
      points['hci'].append(hcis)
      points['cutting_concentration'].append(ccs)
      points['cutting_bed_height'].append(cbhs)
      points['md'].append(mds)

   return points

profiles = get_algo_output_channels()
with open(save_path, 'w') as f:
   json.dump(profiles, f)