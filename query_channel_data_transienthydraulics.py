
import json
import statistics
from pymongo import MongoClient
from datetime import datetime
import pandas as pd
from tqdm import tqdm
import os

well_name = 'BDC-4-02_bha_run1_correct_mud'
time_stamp_str = '20240813175608'
unique_well_name = f'{well_name}_{time_stamp_str}'
ecd_channel_data = fr'C:\NotOneDrive\Data\{well_name}\Channel_ECD.csv'
input_channel_data = fr'C:\NotOneDrive\Data\{well_name}\Channels.csv'
db_name = 'TransientHydraulics'
uri = 'mongodb://localhost:27017'
save_path = os.path.join(r'C:\NotOneDrive\Data\merged_input_output_channels', f'{well_name}_{db_name}_{time_stamp_str}.csv')


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
      },
      # {
      #    "$set": {
      #       "time": {
      #           '$toDate': "$k"
      #       }
      #    }        
      # } 
   #    {
   #        '$match': {
   #            'time':{
   #             '$gt': datetime(2023, 11, 18)
   #            }
   #        }
   #    }
   ]

   points = {
      'time key': ['Date'],
      'max ecd': ['kg/m3'],
      'max hci': ['unitless'],
      'mean hci': ['unitless'],
      'max cutting concentration': ['unitless'],
      'mean cutting concentration': ['unitless'],
      'max cutting bed height': ['m'],
      'mean cutting bed height': ['m'],
      'ecd at bit': ['kg/m3'],
      'simulated spp': ['Pa'],
      'drill string pressure loss': ['Pa'],
      'ecd at bit no cuttings': ['kg/m3']
   }

   results = collection.aggregate(pipeline)
   for i, r in tqdm(enumerate(results)):
      time = r['time']
      value = json.loads(r['v'])

      if 'ECDAtBitNoCuttings' in value.keys():
         points['ecd at bit no cuttings'].append(value['ECDAtBitNoCuttings'])
      else:
         points['ecd at bit no cuttings'].append('')

      if 'RTHydro.PressureProfile' in list(value.keys()):
         pressure_profile = json.loads(value['RTHydro.PressureProfile'])['ProfilePoints']
         max_ecd = max(list(map(lambda x: float(x['Ecd']['P50']), pressure_profile))) 
      else:
         max_ecd = ''

      
      if 'RTHydro.HoleCleaningProfile' in list(value.keys()):
         hci_profile = json.loads(value['RTHydro.HoleCleaningProfile'])
         hcis = list(map(lambda x: float(x), hci_profile['HCI'] ))
         max_hci, mean_hci = ('','') if len(hcis) == 0 else (max(hcis), statistics.mean(hcis))
         cbhs = list(map(lambda x: float(x), hci_profile['CuttingBedHeightP50'] ))
         max_cbh, mean_cbh = ('', '') if len(cbhs) == 0 else (max(cbhs), statistics.mean(cbhs))
         ccs = list(map(lambda x: float(x), hci_profile['CuttingConcentrationP50'], ))
         max_cc, mean_cc = ('', '') if len(ccs) == 0 else (max(ccs), statistics.mean(ccs))
      else:
         max_hci = ''
         mean_hci = ''
         max_cbh = ''
         mean_cbh = ''
         max_cc = ''
         mean_cc = ''

      if 'RTHydro.ATBITECDSIM' in list(value.keys()):
         points['ecd at bit'].append(value['RTHydro.ATBITECDSIM'])
      else:
         points['ecd at bit'].append('')

      if 'RTHydro.PressureProfile' in list(value.keys()):
         pressure_drop_all = json.loads(value['RTHydro.PressureProfile'])['PressureDrop']
         if pressure_drop_all is None:
            points['drill string pressure loss'].append('')
         else:
            points['drill string pressure loss'].append(pressure_drop_all['DrillstringPressureDrop'])
      else:
         points['drill string pressure loss'].append('')
      
      if 'RTHydro.SPPSIM' in list(value.keys()):
         points['simulated spp'].append(value['RTHydro.SPPSIM'])
      else:
         points['simulated spp'].append('')
      
      # print(time.microsecond)
      points['time key'].append(time.strftime('%Y-%m-%dT%H:%M:%S') + f'.{time.microsecond:07d}Z')
      points['max ecd'].append(max_ecd)
      points['max hci'].append(max_hci)
      points['max cutting concentration'].append(max_cc)
      points['max cutting bed height'].append(max_cbh)
      points['mean hci'].append(mean_hci)
      points['mean cutting concentration'].append(mean_cc)
      points['mean cutting bed height'].append(mean_cbh)


   return pd.DataFrame(points)

# input_channels = get_input_channels()
# ecd_channels = get_m_ecds()
output_channels = get_algo_output_channels()

# m = input_channels.merge(ecd_channels, how='outer', on='time key',)\
#    .merge(output_channels, how='outer', on='time key',)

# m.to_csv(save_path)
output_channels.to_csv(save_path, index=False)