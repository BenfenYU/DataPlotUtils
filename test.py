import json
from pathlib import Path
import pandas as pd
from utils import ole2datetime, unit_convert
from tqdm import tqdm
import os
from datetime import datetime

def test():
    path = r'C:\Users\Jyu20\OneDrive - SLB\rhapsody\StuckPipe_investigation\code\3s-617_Run3_all_channels.csv'
    channels = pd.read_csv(path)
    print(channels.iloc[-1])

def extract_measure_ecd():
    path = r"C:\Users\Jyu20\OneDrive - SLB\rhapsody\requirements\2195295_validate_hci\well_Data\enable_rhapsody_BDC1B01-Offshore-RTHydraulics-V1_20231103_1.csv"
    save_path = r"C:\NotOneDrive\Data\algo_seperate_data\BDC_Run1_bha\Channel_ECD.csv"
    ecds = pd.read_csv(path)
    ecds = ecds[[
        'ECD_RT',
        'System Time'
    ]]
    ecds = ecds[ecds['ECD_RT'] != -9999]
    ecds = ecds.rename(columns={
        'ECD_RT': 'measure_ecd',
        'System Time': 'time_key'
    })
    ecds.to_csv(save_path, index=False)

def merge_ecds():
    ecd1_path = r"C:\Users\Jyu20\OneDrive - SLB\rhapsody\requirements\2195295_validate_hci\well_Data\enable_rhapsody_BDC1B01-Offshore-RTHydraulics-V1_20231103_1.csv"
    ecd2_path = r"C:\Users\Jyu20\OneDrive - SLB\rhapsody\requirements\2195295_validate_hci\well_Data\enable_rhapsody_BDC1B01-Offshore-RTHydraulics-V1_20231103_2.csv"

    ecd1 = pd.read_csv(ecd1_path)[['UtcTime', 'RTHydro.ATBITECDSIM']].rename(columns={'UtcTime': 'time_key', 'RTHydro.ATBITECDSIM': 'ecd_at_bit'}).dropna()
    ecd2 = pd.read_csv(ecd2_path)[['UtcTime', 'RTHydro.ATBITECDSIM']].rename(columns={'UtcTime': 'time_key', 'RTHydro.ATBITECDSIM': 'ecd_at_bit'}).dropna()

    ecds = pd.concat([ecd1, ecd2]).sort_values(by=['time_key'])
    ecds['ecd_at_bit'] *= unit_convert('kg/m3', 'lb/gal')
    ecds.to_csv(r"C:\Users\Jyu20\OneDrive - SLB\rhapsody\requirements\2195295_validate_hci\well_Data\BDC_ecds_202312_algo_version.csv", index=False)
    print(ecds)

def get_ecd_bmk():
    ecd_path = r"C:\Users\Jyu20\OneDrive - SLB\rhapsody\requirements\2195295_validate_hci\well_Data\SPPValidation_SDD201_2023911.bmk.csv"
    ecds = pd.read_csv(ecd_path)
    ecds['time_key'] = pd.to_datetime(ecds['Time '].apply(ole2datetime))
    ecds = ecds[['time_key', ' Calc ECDAtPWD']].rename(columns={' Calc ECDAtPWD': 'ecd_at_bit'})
    ecds['ecd_at_bit'] *= (1000*unit_convert('kg/m3', 'lb/gal') )
    ecds.to_csv(r'C:\Users\Jyu20\OneDrive - SLB\rhapsody\requirements\2195295_validate_hci\well_Data\SDD_CHE_ecd.csv', index=False)
    print(ecds)

def process_algo_output():
    ecd_path = r"C:\NotOneDrive\Data\merged_input_output_channels\SDD_Run1_bha_TransientHydraulics_20240703174642.csv"
    ecds = pd.read_csv(ecd_path,)
    ecds = ecds[['time key', 'ecd at bit']].rename(columns={'time key': 'time_key', 'ecd at bit': 'ecd_at_bit'})
    ecds['time_key'] = pd.to_datetime(ecds['time_key'])
    ecds['ecd_at_bit'] *= unit_convert('kg/m3', 'lb/gal')
    ecds.dropna(inplace=True)
    ecds.to_csv(r"C:\Users\Jyu20\OneDrive - SLB\rhapsody\requirements\2195295_validate_hci\well_Data\SDD_ecds_202407_algo_version.csv", index=False)
    print(ecds)

def merge_ecds_into_one_csv():
    engine_ecds_path = r"C:\Users\Jyu20\OneDrive - SLB\rhapsody\requirements\2195295_validate_hci\well_Data\BDC_clean_ecd_bmk.csv"
    old_algo_ecds_path = r"C:\Users\Jyu20\OneDrive - SLB\rhapsody\requirements\2195295_validate_hci\well_Data\BDC_ecds_202312_algo_version.csv"
    new_algo_ecds_path = r"C:\Users\Jyu20\OneDrive - SLB\rhapsody\requirements\2195295_validate_hci\well_Data\BDC_ecds_202407_algo_version.csv"

    engine_ecds = pd.read_csv(engine_ecds_path)
    old_algo_ecds = pd.read_csv(old_algo_ecds_path)
    new_algo_ecds = pd.read_csv(new_algo_ecds_path)

    engine_ecds['time_key'] = pd.to_datetime(engine_ecds['time_key'], utc=True)
    engine_ecds.rename(columns={'ecd_at_bit': 'engine_ecd_at_bit'}, inplace=True)
    old_algo_ecds['time_key'] = pd.to_datetime(old_algo_ecds['time_key'], utc=True)
    old_algo_ecds.rename(columns={'ecd_at_bit': 'old_algo_ecd_at_bit'}, inplace=True)
    new_algo_ecds['time_key'] = pd.to_datetime(new_algo_ecds['time_key'], utc=True)
    new_algo_ecds.rename(columns={'ecd_at_bit': 'new_algo_ecd_at_bit'}, inplace=True)

    m = pd.merge(engine_ecds, old_algo_ecds, on='time_key', how='outer')
    m = pd.merge(m, new_algo_ecds, on='time_key', how='outer')
    m.to_csv(r"C:\Users\Jyu20\OneDrive - SLB\rhapsody\requirements\2195295_validate_hci\well_Data\merged_ecds.csv", index=False)

def get_SDD_ecd():
    ecd_path = r"C:\Users\Jyu20\OneDrive - SLB\rhapsody\requirements\2195295_validate_hci\well_Data\enable-rhapsody_SDD-RTHydraulics-V1__2024-01-11T09_22_13.842Z_timedata_6_30_2014_7_4_2014.csv"
    ecds = pd.read_csv(ecd_path, skiprows=[1])
    ecds['UtcTime'] = pd.to_datetime(ecds['UtcTime'])
    ecds = ecds[['UtcTime', 'RTHydro.ATBITECDSIM']].rename(columns={'UtcTime': 'time_key', 'RTHydro.ATBITECDSIM': 'ecd_at_bit'})
    ecds['ecd_at_bit'] *= (1000*unit_convert('kg/m3', 'lb/gal') )
    ecds.to_csv(r'C:\Users\Jyu20\OneDrive - SLB\rhapsody\requirements\2195295_validate_hci\well_Data\SDD_ecds_202312_algo_version.csv', index=False)
    print(ecds)

def check_base_fluid():
    mapping_dfa_offer_path = r"C:\Users\Jyu20\Downloads\DFP Base Fluid to CHE Base Fluid-e499a03e-4233-4274-b159-a20b84ff9a60.json"
    che_bfs_path = r"C:\Users\Jyu20\OneDrive - SLB\rhapsody\CHE_base_fluid_type.txt"
    with open(mapping_dfa_offer_path, 'r') as f:
        mappings = json.load(f)

    che_bfs = []
    with open(che_bfs_path, 'r') as f:
        for line in f:
            che_bfs.append(line.strip())

    for k, v in mappings.items():
        che_name = v['CHEName']
        if che_name in che_bfs:
            print(True)
        else:
            print(False)

def save_base_fluid_mappine():

    mapping_dfa_offer_path = r"C:\Users\Jyu20\Downloads\DFP Base Fluid to CHE Base Fluid-e499a03e-4233-4274-b159-a20b84ff9a60.json"
    with open(mapping_dfa_offer_path, 'r') as f:
        mappings = json.load(f)
    for k, v in mappings.items():
        che_name = v['CHEName']
        mappings[k] = che_name

    with open('test.json', 'w') as f:
        json.dump(mappings, f)

def convert_time_to_string(df: pd.DataFrame, key):

    df[key] = df.dropna(subset=[key])[key].dt.strftime("%Y%m%d%H%M%S%f")
    # for i, r in tqdm(df.iterrows()):
    #     if pd.isna(r[key]):
    #         continue
    #     r[key] = r[key].strftime("%Y%m%d%H%M%S%f")

    return df

def insert_BV_to_engine_data():
    engine_data_path = r"C:\Users\Jyu20\OneDrive - SLB\Code\rhapsody_algorithm\CHE-RealtimeSimulatorSupport_2024.5\IntegrationTestData\RTSimulation\InputFiles\BDC-1B-01-CHE.csv"
    eng_data = pd.read_csv(engine_data_path)
    eng_data['Time'] = pd.to_datetime(eng_data['Time'], utc=True)

    channels_path = r"C:\NotOneDrive\Data\algo_seperate_data\BDC_Run1_bha\Channels.csv"
    channels = pd.read_csv(channels_path)
    channels['TIME'] = pd.to_datetime(channels['TIME'], utc=True)
    channels = channels[['TIME', 'AutoState.BVEL']]

    channels = convert_time_to_string(channels, 'TIME').set_index('TIME').to_dict()['AutoState.BVEL']
    # print(channels)
    eng_data['str_time'] = eng_data.dropna(subset=['Time'])['Time'].dt.strftime("%Y%m%d%H%M%S%f")

    # with open(r"C:\Users\Jyu20\Downloads\channels.json", 'w') as f:
    #     json.dump(channels, f)
    res = eng_data.copy()
    res.insert(0, 'pipe_velocity', pd.NA)
    for i, r in tqdm(eng_data.iterrows()):
        if r['str_time'] in channels:
            # print(r)
            res['pipe_velocity'][i] = channels[r['str_time']]

    res['pipe_velocity'] = res['pipe_velocity'].interpolate(method='nearest').fillna(0)
    res.to_csv(r"C:\Users\Jyu20\OneDrive - SLB\Code\rhapsody_algorithm\CHE-RealtimeSimulatorSupport_2024.5\IntegrationTestData\RTSimulation\InputFiles\BDC-1B-01-CHE_add_pipevelocity.csv",
                index=False)
    print(res)


def merge_engine_input():
    
    input_dir = r"C:\NotOneDrive\Data\EngineInput\BDC402_Run1"
    paths = sorted(list(Path(input_dir).glob("channel_*.json")))

    datas = {
        'SystemTime': [],
        'FlowRate': [],
        'BitDepth': [],
        'HoleDepth': [], 
        'ROP': [], 
        'PipeVelocity': [], 
        'RotationRate': [], 
        'Hookload': [],
        'WeightOnBit': []
    }

    for  p in tqdm(paths):
        with open(p, 'r') as f:
            content = json.load(f)
            for k, v in datas.items():
                if k in content.keys():
                    datas[k].append(content[k])
                else:
                    datas[k].append(None)

    with open(r"C:\NotOneDrive\Data\EngineInput\BDC402_Run1_merged_channel.json", 'w') as f:
        json.dump(datas, f)

            # for k, v in content.items():
            #     if k in datas.keys():
            #         datas[k].append(v)
            #     else:
            #         print(k)
            #         print(content)
            #         assert(False)

    # print(paths[0], paths[1])

def convert_json_channel_to_csv():
    json_path = r"C:\NotOneDrive\Data\EngineInput\BDC402_Run1_merged_channels\BDC402_Run1_merged_channel.json"
    with open(json_path, 'r') as f:
        content = json.load(f)

    csv_file = pd.DataFrame(content)
    csv_file.to_csv(r"C:\NotOneDrive\Data\EngineInput\BDC402_Run1_merged_channels\BDC402_Run1_merged_channel.csv")

def process_time_series_pressure_drop():
    path = r"C:\Users\Jyu20\Downloads\pressure_drop.json"
    with open(path, 'r') as f:
        all_data = json.load(f)['items']

    res = []
    for d in all_data:
        dv = json.loads(d['value'])['PressureDrop']
        if dv is None: continue
        dv['time'] = d['index']
        res.append(dv)

    with open(r"C:\Users\Jyu20\Downloads\lala_pressure_drop_processed.json", 'w') as f:
        json.dump(res, f)

def merge_output_td():
    base_dir = r"C:\Users\Jyu20\Downloads\test"

    m_data = {
        'time_key': [],
        'td': []
    }

    for f in tqdm(os.listdir(base_dir)):
        path = os.path.join(base_dir, f)
        if not os.path.isfile(path):
            continue
        date_str = f[:-4]
        date = datetime.strptime(date_str[:-4], "%Y%m%d-%H%M%S")
        milliseconds = int(date_str[-3:])
        parsed_datetime = date.replace(microsecond=milliseconds * 1000)
        with open(path, 'r') as f:
            td = float(f.read().strip())

        m_data['time_key'].append(
            parsed_datetime.strftime("%Y-%m-%dT%H:%M:%S.%f")
        )
        m_data['td'].append(td)

    m_data = pd.DataFrame(m_data)

    m_data.to_csv(r"C:\Users\Jyu20\Downloads\test\merged_tds.csv")



merge_output_td()

    