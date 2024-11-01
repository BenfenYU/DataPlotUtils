import os
import json
import pickle

well_name = '3s-624_Run3'
well_output_base_dir = r'C:\NotOneDrive\Data'

if '617_Run3' in well_name:
    time_key_display_max_limit = '2023-11-18T16:00:00Z'
    time_key_display_min_limit = '2023-11-17T12:00:00Z'
elif '624_Run3' in well_name:
    time_key_display_min_limit = '2023-12-30T12:00:00Z'
    time_key_display_max_limit = '2024-01-03T12:00:00Z'


well_output_path = os.path.join(well_output_base_dir, f'Output_{well_name}', 
                                'TransientHydraulics', 'merged_outputs.json')
well_trunked_save_path = os.path.join(well_output_base_dir, f'Output_{well_name}', 
                                      'TransientHydraulics', 'merged_outputs_trunked.list')

with open(well_output_path, 'r') as f:
    raw_data = json.load(f)

trunked_data = []
for data in raw_data:
    if data['Key'] > time_key_display_min_limit and data['Key'] < time_key_display_max_limit:
        trunked_data.append(data)

with open(well_trunked_save_path, "wb") as f:
    pickle.dump(trunked_data, f)