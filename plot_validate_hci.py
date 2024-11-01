
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import json
from math import isnan
import pandas as pd
from plotly_resampler import FigureResampler
import os
from datetime import datetime
import plotly.express as px
from tqdm import tqdm
import pickle
from copy import deepcopy
from utils import unit_convert
import statistics

def every_nth(nums, nth):
    # Use list slicing to return elements starting from the (nth-1) index, with a step of 'nth'.
    return nums[nth - 1::nth]

def filter_by_time_key(df, key_name, min_time, max_time):
    return df[(df[key_name] > min_time) & (df[key_name] < max_time)]

def get_ecd_with_threshold(df: pd.DataFrame, index, threshold):
    if not isnan(df.iloc[index]['ecd_at_bit']):
        return df.iloc[index]['ecd_at_bit']
    time_key = df.iloc[index]['time_key']

    def get_ecd(_index):
        _time_key = df.iloc[_index]['time_key']
        seconds = abs((time_key - _time_key).total_seconds())
        if seconds < threshold:
            _ecd = df.iloc[_index]['ecd_at_bit']
            if isnan(_ecd):
                return None
            return _ecd
        return None

    ecd = get_ecd(index-1)
    if ecd is not None or index == len(df)-1:
        return ecd
    ecd =  get_ecd(index+1)
    return ecd

def plot_hist_ecd_difference(ecds, well_name, algo_name):

    fig = go.Figure() 
    fig = px.histogram(
        ecds,
        x = 'difference',
    )
    fig.update_xaxes(tickfont=dict(
                                    size=20,
                                    ) )

    fig.update_yaxes(tickfont=dict(
                                    size=20,
                                    ) )
    fig.update_layout(
        title = dict(text = fr'{well_name} - {algo_name} - Measured minus Modeled ECD at bit', font = dict(size = 30)),
        yaxis=dict(title=dict(text="Count", font = dict(size = 20))),
        xaxis=dict(title=dict(text = "Ecd difference (lb/gal)", font = dict(size = 20))),
        legend = dict(font = dict(size = 20))
    )
    return fig

def plot_line_ecd_difference(ecds, well_name, algo_name):
    marker_size = 4
    line_width = 0.5

    fig = go.Figure() 
    fig.add_trace(
        go.Scattergl(
            mode = 'markers+lines',
            marker = dict(
                size = marker_size
            ),
            line={'width': line_width},
            x=ecds['time_key'], 
            y=ecds['difference'], 
            name="measured minus modeled ecd at bit",
            yaxis='y'),
    )

    fig.update_xaxes(tickfont=dict(
                                    size=20,
                                    ) )

    fig.update_yaxes(tickfont=dict(
                                    size=20,
                                    ) )
    fig.update_layout(
        title = dict(text=fr'{well_name} - {algo_name} - Measured minus Modeled ECD at bit', font = dict(size = 30)),
        yaxis=dict(title=dict(text = "Ecd (lb/gal)", font = dict(size = 20)),),
        xaxis=dict(title=dict(
            text = "Time",
            font = dict(size = 20)
            )),
        legend = dict(font = dict(size = 20))
    )

    return fig

def statistic(algo_outputs, measure_ecds, algo_name):

    algo_outputs['time_key'] = pd.to_datetime(algo_outputs['time_key'], utc=True)
    measure_ecds['time_key'] = pd.to_datetime(measure_ecds['time_key'], utc=True)
    merged_ecds = measure_ecds.merge(algo_outputs, how = 'outer', on = 'time_key')
    # o.to_csv(r"C:\Users\Jyu20\OneDrive - SLB\Code\rhapsody_algorithm\RunWellLocally\DataProcessAndVisualize\test.csv")
    clean_ecds = {
        'time_key': [],
        'measure_ecd': [],
        'model_ecd': [],
        'difference': [],
        'difference_abs': [],
    }
    i = 0
    j = 0
    for index, r in merged_ecds.iterrows():
        measure_ecd = r['measure_ecd']
        if isnan(measure_ecd):
            continue
        j += 1
        model_ecd = get_ecd_with_threshold(merged_ecds, index, 5)
        
        if model_ecd is not None:
            i += 1
            clean_ecds['time_key'].append(r['time_key'])
            clean_ecds['measure_ecd'].append(r['measure_ecd'])
            clean_ecds['model_ecd'].append(model_ecd)
            clean_ecds['difference'].append(r['measure_ecd'] - model_ecd)
            clean_ecds['difference_abs'].append(abs(r['measure_ecd'] - model_ecd))
    
    print(algo_name)
    print(f"Average: {statistics.mean(clean_ecds['difference_abs'])} lb/gal")
    print(f"Median: {statistics.median(clean_ecds['difference_abs'])} lb/gal")
    print(f"Variance: {statistics.variance(clean_ecds['difference_abs'])} lb/gal")
    print()

    return pd.DataFrame(clean_ecds)
    
def plot_line_both_ecd(algo_outputs, measure_ecds, well_name, algo_name):
    sub_data_ecd = algo_outputs[[
        'ecd_at_bit',
        'time_key',
    ]].dropna(how='any')

    measure_ecds['time_key'] = pd.to_datetime(measure_ecds['time_key'])
    range_max_ecd = max(sub_data_ecd['ecd_at_bit'].max(), measure_ecds['measure_ecd'].max())
    range_min_ecd = min(sub_data_ecd['ecd_at_bit'].min(), measure_ecds['measure_ecd'].min())

    '''
    measured ecd at bit, simulated ecd at bit
    '''

    marker_size = 4
    line_width = 0.5

    fig = go.Figure() 
    fig.add_trace(
        go.Scattergl(
            mode = 'markers+lines',
            marker = dict(
                size = marker_size
            ),
            line={'width': line_width},
            x=measure_ecds['time_key'], 
            y=measure_ecds['measure_ecd'], 
            name="measured ecd at bit",
            yaxis='y'),
    )

    fig.add_trace(
        go.Scattergl(
            mode = 'markers+lines',
            marker = dict(
                size = marker_size
            ),
            line={'width': line_width},
            x=sub_data_ecd['time_key'], 
            y=sub_data_ecd['ecd_at_bit'], 
            name="simulated ecd at bit"),
    )

    fig.update_xaxes(tickfont=dict(
                                    size=20,
                                    ) )

    fig.update_yaxes(tickfont=dict(
                                    size=20,
                                    ) )
    fig.update_layout(
        title = dict(
            text = fr'{well_name} - {algo_name} - ECD at bit',
            font = dict(size = 30)
        ),
        yaxis=dict(title=dict(
            text = "ECD (lb/gal)",
            font = dict(size = 20)
            ), 
            range = [range_min_ecd, range_max_ecd]
            )
            ,
        xaxis=dict(title=dict(
            text = "Time",
            font = dict(size = 20)
            )),
        legend = dict(font = dict(size = 20))
            )

    return fig
    # fig.show()

def plot_data_from_algo_output():

    well_name = 'SDD'
    time_stamp_str = '20240703174642'

    data_base_dir = rf'C:\NotOneDrive\Data\algo_seperate_data'
    output_base_dir = r'C:\NotOneDrive\Data\merged_input_output_channels'
    engine_output_channels_path = r"C:\Users\Jyu20\OneDrive - SLB\rhapsody\requirements\2195295_validate_hci\well_Data\SDD_ecds_202407_algo_version.csv"

    # measured
    channel_ecd_path = os.path.join(data_base_dir, well_name, 'Channel_ECD.csv')
    channel_ecds = pd.read_csv(channel_ecd_path).dropna()
    # channel_ecds['measure_ecd'] *= (1000*unit_convert('kg/m3', 'lb/gal') )
    channel_ecds['time_key'] = pd.to_datetime(channel_ecds['time_key']).apply(str)

    
    # end_time = r'2015-09-30 16:09:00+00:00'
    # trunc_ecds = channel_ecds[channel_ecds['time_key'] < end_time].copy(deep=True)
    trunc_ecds = channel_ecds.copy(deep=True)

    # algo output
    outputs = pd.read_csv(engine_output_channels_path)
    outputs['time_key'] = pd.to_datetime(outputs['time_key'])
    outputs[['ecd_at_bit']] = outputs[['ecd_at_bit']].apply(pd.to_numeric)
    # outputs['ecd_at_bit'] *= unit_convert('kg/m3', 'lb/gal')
    outputs = outputs[[
        'ecd_at_bit',
        'time_key',
    ]].dropna(how='any')
    # outputs = outputs.rename(columns={'ecd at bit': 'ecd_at_bit', 'time key': 'time_key'})

    version = 'TransientHydraulics algorithm (2024.7)'
    fig_both_ecd = plot_line_both_ecd(outputs, channel_ecds, well_name, version)
    fig_both_ecd.write_html(fr"C:\NotOneDrive\Data\temp_plots\validate_hci\{well_name}_TransientHydraulics_ECD_{time_stamp_str}.html")

    # fig_both_ecd_2 = plot_line_both_ecd(outputs, trunc_ecds, well_name, version)
    # fig_both_ecd_2.write_html(fr"C:\NotOneDrive\Data\temp_plots\validate_hci\{well_name}_TransientHydraulics_ECD_{time_stamp_str}_trunc.html")

    clean_ecds = statistic(outputs, channel_ecds, version)

    fig_ecd_diff = plot_line_ecd_difference(clean_ecds, well_name,version) 
    fig_ecd_diff.write_html(fr"C:\NotOneDrive\Data\temp_plots\validate_hci\{well_name}_TransientHydraulics_ECD_difference_{time_stamp_str}.html")
    fig_ecd_diff_hist = plot_hist_ecd_difference(clean_ecds, well_name, version)
    fig_ecd_diff_hist.write_html(fr"C:\NotOneDrive\Data\temp_plots\validate_hci\{well_name}_TransientHydraulics_ECD_difference_hist_{time_stamp_str}.html")

    return outputs['time_key'].iloc[-1]

def plot_data_from_CHE_result():

    well_name = 'SDD'
    time_stamp_str = '20240703174642'

    data_base_dir = rf'C:\NotOneDrive\Data\algo_seperate_data'

    channel_ecd_path = os.path.join(data_base_dir, well_name, 'Channel_ECD.csv')
    channel_ecds = pd.read_csv(channel_ecd_path).dropna()
    # channel_ecds['measure_ecd'] *= (1000*unit_convert('kg/m3', 'lb/gal') )
    channel_ecds['time_key'] = pd.to_datetime(channel_ecds['time_key']).apply(str)
    
    # end_time = r'2015-09-30 16:09:00+00:00'
    # trunc_ecds = channel_ecds[channel_ecds['time_key'] < end_time].copy(deep=True)
    trunc_ecds = channel_ecds.copy(deep=True)

    bmk_ecds_path = r"C:\Users\Jyu20\OneDrive - SLB\rhapsody\requirements\2195295_validate_hci\well_Data\SDD_CHE_ecd.csv"
    bmk_ecds = pd.read_csv(bmk_ecds_path)
    bmk_ecds['time_key'] = pd.to_datetime(bmk_ecds['time_key'])
    print(bmk_ecds)

    # bmk_ecds['time_key'] = pd.to_datetime(bmk_ecds['time_key'])
    # bmk_ecds = bmk_ecds[bmk_ecds['time_key'] < trunc_time]

    version = 'RT simulator (2023.9)'
    fig_both_ecd = plot_line_both_ecd(bmk_ecds, channel_ecds, well_name, version)
    fig_both_ecd.write_html(fr"C:\NotOneDrive\Data\temp_plots\validate_hci\{well_name}_RTCHE_ECD_{time_stamp_str}.html")

    # fig_both_ecd_2 = plot_line_both_ecd(bmk_ecds, trunc_ecds, well_name, version)
    # fig_both_ecd_2.write_html(fr"C:\NotOneDrive\Data\temp_plots\validate_hci\{well_name}_RTCHE_ECD_{time_stamp_str}_trunc.html")

    # clean_ecds = statistic(bmk_ecds, channel_ecds, version)

    # fig_ecd_diff = plot_line_ecd_difference(clean_ecds, well_name, version)
    # fig_ecd_diff.write_html(fr"C:\NotOneDrive\Data\temp_plots\validate_hci\{well_name}_RTCHE_ECD_difference_{time_stamp_str}.html")
    # fig_ecd_diff_hist = plot_hist_ecd_difference(clean_ecds, well_name, version)
    # fig_ecd_diff_hist.write_html(fr"C:\NotOneDrive\Data\temp_plots\validate_hci\{well_name}_RTCHE_ECD_difference_hist_{time_stamp_str}.html")

def plot_data_from_old_algo_result():

    well_name = 'SDD'

    data_base_dir = rf'C:\NotOneDrive\Data\algo_seperate_data'

    channel_ecd_path = os.path.join(data_base_dir, well_name, 'Channel_ECD.csv')
    channel_ecds = pd.read_csv(channel_ecd_path).dropna()
    # channel_ecds['measure_ecd'] *= (1000*unit_convert('kg/m3', 'lb/gal') )
    channel_ecds['time_key'] = pd.to_datetime(channel_ecds['time_key']).apply(str)
    
    # end_time = r'2015-09-30 16:09:00+00:00'
    # trunc_ecds = channel_ecds[channel_ecds['time_key'] < end_time].copy(deep=True)
    trunc_ecds = channel_ecds.copy(deep=True)

    # old algo version
    bmk_ecds_path = r"C:\Users\Jyu20\OneDrive - SLB\rhapsody\requirements\2195295_validate_hci\well_Data\SDD_ecds_202312_algo_version.csv"
    bmk_ecds = pd.read_csv(bmk_ecds_path).dropna()

    # bmk_ecds['time_key'] = pd.to_datetime(bmk_ecds['time_key'])
    # bmk_ecds = bmk_ecds[bmk_ecds['time_key'] < trunc_time]

    version = 'TransientHydraulics algorithm (2023.12)'
    fig_both_ecd = plot_line_both_ecd(bmk_ecds, channel_ecds, well_name, version)
    fig_both_ecd.write_html(fr"C:\NotOneDrive\Data\temp_plots\validate_hci\{well_name}_ECD_{version}.html")

    # fig_both_ecd_2 = plot_line_both_ecd(bmk_ecds, trunc_ecds, well_name, version)
    # fig_both_ecd_2.write_html(fr"C:\NotOneDrive\Data\temp_plots\validate_hci\{well_name}_ECD_{version}_trunc.html")

    clean_ecds = statistic(bmk_ecds, channel_ecds, version)

    fig_ecd_diff = plot_line_ecd_difference(clean_ecds, well_name, version)
    fig_ecd_diff.write_html(fr"C:\NotOneDrive\Data\temp_plots\validate_hci\{well_name}_ECD_difference_{version}.html")
    fig_ecd_diff_hist = plot_hist_ecd_difference(clean_ecds, well_name, version)
    fig_ecd_diff_hist.write_html(fr"C:\NotOneDrive\Data\temp_plots\validate_hci\{well_name}_ECD_difference_hist_{version}.html")

def analyze_():

    well_name = 'BDC_Run1_bha'
    time_stamp_str = '20240628125850'

    data_base_dir = rf'C:\NotOneDrive\Data\algo_seperate_data'
    raw_channels_path = rf"C:\NotOneDrive\Data\algo_seperate_data\BDC_Run1_bha\Channels.csv"
    raw_channels = pd.read_csv(raw_channels_path, skiprows=[1]).dropna().rename(
        columns={'TIME': 'time_key', 'DRILLING_VFLOW': 'flowrate', 'DEPTH_ROPINS': 'rop'})
    raw_channels['time_key'] = pd.to_datetime(raw_channels['time_key'], utc=True)

    channel_ecd_path = os.path.join(data_base_dir, well_name, 'Channel_ECD.csv')
    channel_ecds = pd.read_csv(channel_ecd_path).dropna()
    channel_ecds['measure_ecd'] *= (1000*unit_convert('kg/m3', 'lb/gal') )
    channel_ecds['time_key'] = pd.to_datetime(channel_ecds['time_key'], utc=True)
    
    # output of engine or algo 
    bmk_ecds_path = r"C:\Users\Jyu20\OneDrive - SLB\rhapsody\requirements\2195295_validate_hci\well_Data\BDC_ecds_202407_algo_version.csv"
    bmk_ecds = pd.read_csv(bmk_ecds_path)
    bmk_ecds['time_key'] = pd.to_datetime(bmk_ecds['time_key'], utc=True)

    merged = pd.merge(raw_channels, bmk_ecds, on = 'time_key', how='outer')
    merged = pd.merge(merged, channel_ecds, on='time_key', how='outer')
    merged = merged[['flowrate', 'time_key', 'ecd_at_bit', 'measure_ecd', 'rop']]
    merged['ecd_at_bit'] = merged['ecd_at_bit'].interpolate(method='linear', limit_direction='forward')
    merged['measure_ecd'] = merged['measure_ecd'].interpolate(method='linear', limit_direction='forward')
    # merged['rop'] = merged['rop'].interpolate(method='linear', limit_direction='forward')
    # merged.dropna(subset=['rop'], inplace=True)
    # merged = merged.fillna(0)
    merged = merged[merged['rop'] == 0]
    merged = merged[merged['flowrate'] > 0]
    # merged['flowrate'] = merged['flowrate'].interpolate(method='linear', limit_direction='forward')
    # merged = merged.dropna()
    merged['time_key'] = pd.to_datetime(merged['time_key'])
    # merged = merged[merged['flowrate'] == 0.0]
    merged = merged.assign(diff=abs(merged['ecd_at_bit'] - merged['measure_ecd'])) 

    print(f"Average: {statistics.mean(merged['diff'])} lb/gal")
    print(f"Median: {statistics.median(merged['diff'])} lb/gal")
    print(f"Variance: {statistics.variance(merged['diff'])} lb/gal")

    # merged.to_csv('test.csv')
    # version = 'RT simulator (2023.9)'
    # fig_both_ecd = plot_line_both_ecd(merged, merged, well_name, version)
    # fig_both_ecd.add_trace(
    #     go.Scattergl(
    #         x=merged['time_key'], 
    #         y=merged['flowrate'], 
    #         name="flowrate"),
    # )
    # fig_both_ecd.write_html(fr"temp.html")

    # fig_both_ecd_2 = plot_line_both_ecd(bmk_ecds, trunc_ecds, well_name, version)
    # fig_both_ecd_2.write_html(fr"C:\NotOneDrive\Data\temp_plots\validate_hci\{well_name}_RTCHE_ECD_{time_stamp_str}_trunc.html")

    # clean_ecds = statistic(bmk_ecds, channel_ecds, version)

    # fig_ecd_diff = plot_line_ecd_difference(clean_ecds, well_name, version)
    # fig_ecd_diff.write_html(fr"C:\NotOneDrive\Data\temp_plots\validate_hci\{well_name}_RTCHE_ECD_difference_{time_stamp_str}.html")
    # fig_ecd_diff_hist = plot_hist_ecd_difference(clean_ecds, well_name, version)
    # fig_ecd_diff_hist.write_html(fr"C:\NotOneDrive\Data\temp_plots\validate_hci\{well_name}_RTCHE_ECD_difference_hist_{time_stamp_str}.html")

def statistic_esd():

    well_name = 'BDC_Run1_bha'
    time_stamp_str = '20240628125850'

    data_base_dir = rf'C:\NotOneDrive\Data\algo_seperate_data'
    raw_channels_path = rf"C:\NotOneDrive\Data\algo_seperate_data\BDC_Run1_bha\Channels.csv"
    raw_channels = pd.read_csv(raw_channels_path, skiprows=[1]).dropna().rename(
        columns={'TIME': 'time_key', 'DRILLING_VFLOW': 'flowrate', 'DEPTH_ROPINS': 'rop'})
    raw_channels['time_key'] = pd.to_datetime(raw_channels['time_key'], utc=True)

    channel_ecd_path = os.path.join(data_base_dir, well_name, 'Channel_ECD.csv')
    channel_ecds = pd.read_csv(channel_ecd_path).dropna()
    channel_ecds['measure_ecd'] *= (1000*unit_convert('kg/m3', 'lb/gal') )
    channel_ecds['time_key'] = pd.to_datetime(channel_ecds['time_key'], utc=True)
    
    bmk_ecds_path = r"C:\Users\Jyu20\OneDrive - SLB\rhapsody\requirements\2195295_validate_hci\well_Data\BDC_ecds_202407_algo_version.csv"
    bmk_ecds = pd.read_csv(bmk_ecds_path)
    bmk_ecds['time_key'] = pd.to_datetime(bmk_ecds['time_key'], utc=True)

    merged = pd.merge(raw_channels, bmk_ecds, on = 'time_key', how='outer')
    merged = pd.merge(merged, channel_ecds, on='time_key', how='outer')
    merged = merged[['flowrate', 'time_key', 'ecd_at_bit', 'measure_ecd', 'rop']]
    merged['ecd_at_bit'] = merged['ecd_at_bit'].interpolate(method='linear', limit_direction='forward')
    merged['measure_ecd'] = merged['measure_ecd'].interpolate(method='linear', limit_direction='forward')
    # merged['rop'] = merged['rop'].interpolate(method='linear', limit_direction='forward')
    # merged.dropna(subset=['rop'], inplace=True)
    # merged = merged.fillna(0)
    merged = merged[merged['flowrate'] == 0]
    print(len(merged))
    # merged['flowrate'] = merged['flowrate'].interpolate(method='linear', limit_direction='forward')
    # merged = merged.dropna()
    merged['time_key'] = pd.to_datetime(merged['time_key'])
    merged = merged.assign(diff=abs(merged['ecd_at_bit'] - merged['measure_ecd'])) 

    print(f"Average: {statistics.mean(merged['diff'])} lb/gal")
    print(f"Median: {statistics.median(merged['diff'])} lb/gal")
    print(f"Variance: {statistics.variance(merged['diff'])} lb/gal")

    merged.to_csv('test.csv')
    version = 'RT simulator (2023.9)'
    fig_both_ecd = plot_line_both_ecd(merged, merged, well_name, version)
    fig_both_ecd.add_trace(
        go.Scattergl(
            x=merged['time_key'], 
            y=merged['flowrate'], 
            name="flowrate"),
    )
    fig_both_ecd.write_html(fr"temp.html")

    # fig_both_ecd_2 = plot_line_both_ecd(bmk_ecds, trunc_ecds, well_name, version)
    # fig_both_ecd_2.write_html(fr"C:\NotOneDrive\Data\temp_plots\validate_hci\{well_name}_RTCHE_ECD_{time_stamp_str}_trunc.html")

    # clean_ecds = statistic(bmk_ecds, channel_ecds, version)

    # fig_ecd_diff = plot_line_ecd_difference(clean_ecds, well_name, version)
    # fig_ecd_diff.write_html(fr"C:\NotOneDrive\Data\temp_plots\validate_hci\{well_name}_RTCHE_ECD_difference_{time_stamp_str}.html")
    # fig_ecd_diff_hist = plot_hist_ecd_difference(clean_ecds, well_name, version)
    # fig_ecd_diff_hist.write_html(fr"C:\NotOneDrive\Data\temp_plots\validate_hci\{well_name}_RTCHE_ECD_difference_hist_{time_stamp_str}.html")

def plot_ecd_after_full_mud_properties():
    measure_ecd_path = r"C:\NotOneDrive\Data\algo_seperate_data\BDC_Run1_bha\Channel_ECD.csv"
    measure_ecd = pd.read_csv(measure_ecd_path)
    measure_ecd['time_key'] = pd.to_datetime(measure_ecd['time_key'])
    measure_ecd['ecd_at_bit'] *= unit_convert('g/cm3', 'lb/gal')

    engine_ecd_path = r"C:\Users\Jyu20\OneDrive - SLB\rhapsody\requirements\2195295_validate_hci\well_Data\BDC_clean_ecd_bmk.csv"
    engine_ecd = pd.read_csv(engine_ecd_path)
    engine_ecd['time_key'] = pd.to_datetime(engine_ecd['time_key'])
    
    old_algo_path = r"C:\Users\Jyu20\OneDrive - SLB\rhapsody\requirements\2195295_validate_hci\well_Data\BDC_ecds_202312_algo_version.csv"
    old_algo_ecd = pd.read_csv(old_algo_path)
    old_algo_ecd['time_key'] = pd.to_datetime(old_algo_ecd['time_key'])

    new_algo_path = r"C:\NotOneDrive\Data\merged_input_output_channels\BDC_Run1_bha_TransientHydraulics_20240807091251_surfacecooling_10.csv"
    new_algo_ecd_surfacecooling_10 = pd.read_csv(new_algo_path, skiprows=[1])[['time key', 'ecd at bit']].rename(columns={'ecd at bit' : 'ecd_at_bit', 'time key': 'time_key'})
    new_algo_ecd_surfacecooling_10['time_key'] = pd.to_datetime(new_algo_ecd_surfacecooling_10['time_key'])
    new_algo_ecd_surfacecooling_10['ecd_at_bit'] *= unit_convert('kg/m3', 'lb/gal')

    poc_algo_path = r"C:\NotOneDrive\Data\merged_input_output_channels\BDC_Run1_bha_TransientHydraulics_20240628125850.csv"
    poc_algo_ecd = pd.read_csv(poc_algo_path, skiprows=[1])[['time key', 'ecd at bit']].rename(columns={'ecd at bit' : 'ecd_at_bit', 'time key': 'time_key'})
    poc_algo_ecd['time_key'] = pd.to_datetime(poc_algo_ecd['time_key'])
    poc_algo_ecd['ecd_at_bit'] *= unit_convert('kg/m3', 'lb/gal')

    new_algo_back_engine_algo_path = r"C:\NotOneDrive\Data\merged_input_output_channels\BDC_Run1_bha_TransientHydraulics_20240808143839_mudin_310.csv"
    new_algo_mudin_310 = pd.read_csv(new_algo_back_engine_algo_path, skiprows=[1])[['time key', 'ecd at bit']].rename(columns={'ecd at bit' : 'ecd_at_bit', 'time key': 'time_key'})
    new_algo_mudin_310['time_key'] = pd.to_datetime(new_algo_mudin_310['time_key'])
    new_algo_mudin_310['ecd_at_bit'] *= unit_convert('kg/m3', 'lb/gal')

    new_algo_surfacecooling_22_path = r"C:\NotOneDrive\Data\merged_input_output_channels\BDC_Run1_bha_TransientHydraulics_20240809103636_surfacecolling_22.csv"
    new_algo_ecd_surfacecooling_22 = pd.read_csv(new_algo_surfacecooling_22_path, skiprows=[1])[['time key', 'ecd at bit']].rename(columns={'ecd at bit' : 'ecd_at_bit', 'time key': 'time_key'})
    new_algo_ecd_surfacecooling_22['time_key'] = pd.to_datetime(new_algo_ecd_surfacecooling_22['time_key'])
    new_algo_ecd_surfacecooling_22['ecd_at_bit'] *= unit_convert('kg/m3', 'lb/gal')

    new_algo_surfacecooling_n22_path = r"C:\NotOneDrive\Data\merged_input_output_channels\BDC_Run1_bha_TransientHydraulics_20240809191032_surfacecoolingn22.csv"
    new_algo_ecd_surfacecooling_n22 = pd.read_csv(new_algo_surfacecooling_n22_path, skiprows=[1])[['time key', 'ecd at bit']].rename(columns={'ecd at bit' : 'ecd_at_bit', 'time key': 'time_key'})
    new_algo_ecd_surfacecooling_n22['time_key'] = pd.to_datetime(new_algo_ecd_surfacecooling_n22['time_key'])
    new_algo_ecd_surfacecooling_n22['ecd_at_bit'] *= unit_convert('kg/m3', 'lb/gal')

    new_algo_surfacecooling_n2_path = r"C:\NotOneDrive\Data\merged_input_output_channels\BDC_Run1_bha_TransientHydraulics_20240813123529_surfacecooling_2.csv"
    new_algo_ecd_surfacecooling_n2 = pd.read_csv(new_algo_surfacecooling_n2_path, skiprows=[1])[['time key', 'ecd at bit']].rename(columns={'ecd at bit' : 'ecd_at_bit', 'time key': 'time_key'})
    new_algo_ecd_surfacecooling_n2['time_key'] = pd.to_datetime(new_algo_ecd_surfacecooling_n22['time_key'])
    new_algo_ecd_surfacecooling_n2['ecd_at_bit'] *= unit_convert('kg/m3', 'lb/gal')

    marker_size = 4
    line_width = 0.5

    fig = go.Figure() 

    fig.add_trace(
        go.Scattergl(
            mode = 'markers+lines',
            marker = dict(
                size = marker_size
            ),
            line={'width': line_width},
            x=measure_ecd['time_key'], 
            y=measure_ecd['ecd_at_bit'], 
            name="measured ecd at bit",
            yaxis='y'),
    )

    fig.add_trace(
        go.Scattergl(
            mode = 'markers+lines',
            marker = dict(
                size = marker_size
            ),
            line={'width': line_width},
            x=engine_ecd['time_key'], 
            y=engine_ecd['ecd_at_bit'], 
            name="CHE ecd at bit",
            yaxis='y'),
    )

    fig.add_trace(
        go.Scattergl(
            mode = 'markers+lines',
            marker = dict(
                size = marker_size
            ),
            line={'width': line_width},
            x=old_algo_ecd['time_key'], 
            y=old_algo_ecd['ecd_at_bit'], 
            name="PA(2023.12) ecd at bit",
            yaxis='y'),
    )

    # fig.add_trace(
    #     go.Scattergl(
    #         mode = 'markers+lines',
    #         marker = dict(
    #             size = marker_size
    #         ),
    #         line={'width': line_width},
    #         x=new_algo_ecd_surfacecooling_10['time_key'], 
    #         y=new_algo_ecd_surfacecooling_10['ecd_at_bit'], 
    #         name="PA(2024.8 surface cooling 10) ecd at bit",
    #         yaxis='y'),
    # )

    # fig.add_trace(
    #     go.Scattergl(
    #         mode = 'markers+lines',
    #         marker = dict(
    #             size = marker_size
    #         ),
    #         line={'width': line_width},
    #         x=poc_algo_ecd['time_key'], 
    #         y=poc_algo_ecd['ecd_at_bit'], 
    #         name="PA(2024 POC) ecd at bit",
    #         yaxis='y'),
    # )

    fig.add_trace(
        go.Scattergl(
            mode = 'markers+lines',
            marker = dict(
                size = marker_size
            ),
            line={'width': line_width},
            x=new_algo_mudin_310['time_key'], 
            y=new_algo_mudin_310['ecd_at_bit'], 
            name="PA(2024.8 mudin 310 K) ecd at bit",
            yaxis='y'),
    )

    # fig.add_trace(
    #     go.Scattergl(
    #         mode = 'markers+lines',
    #         marker = dict(
    #             size = marker_size
    #         ),
    #         line={'width': line_width},
    #         x=new_algo_ecd_surfacecooling_22['time_key'], 
    #         y=new_algo_ecd_surfacecooling_22['ecd_at_bit'], 
    #         name="PA(2024.8 surface cooling 22) ecd at bit",
    #         yaxis='y'),
    # )

    # fig.add_trace(
    #     go.Scattergl(
    #         mode = 'markers+lines',
    #         marker = dict(
    #             size = marker_size
    #         ),
    #         line={'width': line_width},
    #         x=new_algo_ecd_surfacecooling_n22['time_key'], 
    #         y=new_algo_ecd_surfacecooling_n22['ecd_at_bit'], 
    #         name="PA(2024.8 surface cooling -22) ecd at bit",
    #         yaxis='y'),
    # )

    # fig.add_trace(
    #     go.Scattergl(
    #         mode = 'markers+lines',
    #         marker = dict(
    #             size = marker_size
    #         ),
    #         line={'width': line_width},
    #         x=new_algo_ecd_surfacecooling_n2['time_key'], 
    #         y=new_algo_ecd_surfacecooling_n2['ecd_at_bit'], 
    #         name="PA(2024.8 surface cooling 2) ecd at bit",
    #         yaxis='y'),
    # )

    fig.update_xaxes(tickfont=dict(
                                    size=20,
                                    ) )

    fig.update_yaxes(tickfont=dict(
                                    size=20,
                                    ) )
    fig.update_layout(
        title = dict(text=fr'Validate PA TransientHydraulics (BDC 1)', font = dict(size = 30)),
        yaxis=dict(title=dict(text = "Ecd (lb/gal)", font = dict(size = 20)),),
        xaxis=dict(title=dict(
            text = "Time",
            font = dict(size = 20)
            )),
        legend = dict(font = dict(size = 20))
    )

    fig.write_html("test.html")
# trunc_time = plot_data_from_algo_output()
# plot_data_from_CHE_result()
# plot_data_from_old_algo_result()

# analyze_()

plot_ecd_after_full_mud_properties()