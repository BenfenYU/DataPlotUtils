
import json
import statistics
from pymongo import MongoClient
from datetime import datetime
import pandas as pd
from tqdm import tqdm
import os

well_name = 'BDC_Run1_bha'
time_stamp_str = '20240909143022'
is_merge_input = False
unique_well_name = f'{well_name}_{time_stamp_str}'
db_name = 'WellBalance'
ecd_channel_data = fr'C:\NotOneDrive\Data\{well_name}\Channel_ECD.csv'
input_channel_data = fr'C:\NotOneDrive\Data\{well_name}\Channels.csv'
save_path = os.path.join(r'C:\NotOneDrive\Data\merged_input_output_channels', f'{well_name}_{db_name}_{time_stamp_str}.csv')
uri = 'mongodb://localhost:27017'

# def parse_time(time_key):
#    datetime.strptime(time_key, '%Y-%M-%DT%H:%M:%S.  %a %B %d %H:%M:%S +0800 %Y')

def get_input_channels():
   channels_df = pd.read_csv(input_channel_data, skiprows=[1])
   channels_df = channels_df.rename(columns={
      'Time': 'time key',
      'TELESCOPE.ECD': 'ecd at bit',
      'DRILLING_VFLOW': 'flowrate',
      'DEPTH_BD': 'bit depth',
      'STANDPIPE_PRS': 'm spp'
   })

   return channels_df

def get_m_ecds():
   m_ecds = pd.read_csv(ecd_channel_data, names = ['time key', 'm ecd at bit'])
   return m_ecds

def get_algo_output_channels():
   client = MongoClient(uri)
   database = client.get_database(db_name)
   collection = database.get_collection(unique_well_name)

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
   #    {
   #        '$match': {
   #            'time':{
   #             '$gt': datetime(2023, 11, 18)
   #            }
   #        }
   #    }
   ]

   points = {
      'time key': [],
      'ecd at bit': [],
      'simulated spp': [],
      'drill string pressure loss': [],
   }

   results = collection.aggregate(pipeline)
   for i, r in tqdm(enumerate(results)):
      time = r['time']
      value = json.loads(r['v'])

      if 'WellBalance.ATBITECDSIM' in list(value.keys()):
         points['ecd at bit'].append(value['WellBalance.ATBITECDSIM'])
      else:
         points['ecd at bit'].append('')

      if 'WellBalance.ECD_RT_DEPTH' in list(value.keys()):
         pressure_drop_all = json.loads(value['WellBalance.ECD_RT_DEPTH'])['PressureDrop']
         if pressure_drop_all is None:
            points['drill string pressure loss'].append('')
         else:
            points['drill string pressure loss'].append(pressure_drop_all['DrillstringPressureDrop'])
      else:
         points['drill string pressure loss'].append('')
      
      if 'WellBalance.SPPSIM' in list(value.keys()):
         points['simulated spp'].append(value['WellBalance.SPPSIM'])
      else:
         points['simulated spp'].append('')
      
      # print(time.microsecond)
      points['time key'].append(time.strftime('%Y-%m-%dT%H:%M:%S') + f'.{time.microsecond:07d}Z')


   return pd.DataFrame(points)


output_channels = get_algo_output_channels()

if is_merge_input:
   input_channels = get_input_channels()
   ecd_channels = get_m_ecds()
   m = input_channels.merge(ecd_channels, how='outer', on='time key',)\
      .merge(output_channels, how='outer', on='time key',)

   m.to_csv(save_path)

else:
   output_channels.to_csv(save_path)