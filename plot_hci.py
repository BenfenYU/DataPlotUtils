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

import numpy as np
from utils import unit_convert


def plot(datas: pd.DataFrame, algo_outputs: pd.DataFrame, measured_ecds: pd.DataFrame, mode: str, save_path: str, labels = None):
    fig = make_subplots(rows = 6, cols = 1,
                        specs=[
                            [{"secondary_y": True}],
                            [{"secondary_y": True}],
                            [{"secondary_y": True}],
                            [{"secondary_y": True}],
                            [{"secondary_y": True}],
                            [{"secondary_y": True}]],
                            shared_xaxes = True,
                            vertical_spacing = 0.09
                            )

    ''' row 1 '''
    fig.add_trace(
        go.Scatter(
            x=algo_outputs['time key'], 
            y=algo_outputs['max cutting concentration'], 
            name="max cutting concentration",
            mode = mode
        )
        , row=1, col=1,
        secondary_y = False
        )
    
    fig.add_trace(
        go.Scatter(
            x=algo_outputs['time key'], 
            y=algo_outputs['mean cutting concentration'], 
            name="average cutting concentration",
            mode = mode
        )
        , row=1, col=1,
        secondary_y = False
        )
    
    fig['layout']['yaxis']['title'] = 'Cutting concentration (%)'

    fig.add_trace(
        go.Scatter(
            x=algo_outputs['time key'], 
            y=algo_outputs['max cutting bed height'], 
            name="max cutting bed height",
            mode = mode
        )
        , row=1, col=1,
        secondary_y = True
        )
    
    fig.add_trace(
        go.Scatter(
            x=algo_outputs['time key'], 
            y=algo_outputs['mean cutting bed height'], 
            name="average cutting bed height",
            mode = mode
        )
        , row=1, col=1,
        secondary_y = True
        )

    fig['layout']['yaxis2']['title'] = 'Cutting bed height (m)'
    
    ''' row 2 '''

    fig.add_trace(
        go.Scatter(
            x=algo_outputs['time key'], 
            y=algo_outputs['mean hci'], 
            name="average hci",
            mode = mode
        )
        , row=2, col=1,
        )
    
    fig.add_trace(
        go.Scatter(
            x=algo_outputs['time key'], 
            y=algo_outputs['max hci'], 
            name="max hci",
            mode = mode
        )
        , row=2, col=1,
        )

    fig['layout']['yaxis3']['title'] = 'Hole cleaning index'

    ''' row 3 '''

    fig.add_trace(
        go.Scatter(
            x=datas['Time'], 
            y=datas['STANDPIPE_PRS'], 
            name="measured standpipe pressure",
            mode = mode
        )
        , row=3, col=1,
        )

    fig.add_trace(
        go.Scatter(
            x=algo_outputs['time key'], 
            y=algo_outputs['simulated spp'], 
            name="modeled standpipe pressure",
            mode = mode
        )
        , row=3, col=1,
        )
    
    fig['layout']['yaxis5']['title'] = 'Standpipe pressure (psi)'

    fig.add_trace(
        go.Scatter(
            x=algo_outputs['time key'], 
            y=algo_outputs['ecd at bit'], 
            name="modeled ecd at bit",
            mode = mode
        ), 
        row=3, col=1,
        secondary_y = True
        )
    
    fig.add_trace(
        go.Scatter(
            x=m_ecds['time_key'], 
            y=m_ecds['measured_ecd'], 
            name="measured ecd at bit",
            mode = 'markers',
            marker = dict(
                size = 2,
                opacity=0.5,
            )
        ), 
        row=3, col=1,
        secondary_y = True
        )

    fig['layout']['yaxis6']['title'] = 'ECD (lb/gal)'

    ''' row 4 '''

    fig.add_trace(
        go.Scatter(
            x=datas['Time'], 
            y=datas['DrilBHReam.ROP5'], 
            name="rop5",
            mode = mode
        )
        , row=4, col=1
        )

    fig['layout']['yaxis7']['title'] = 'Rop (m/h)'

    fig.add_trace(
        go.Scatter(
            x=datas['Time'], 
            y=datas['AutoState.BVEL'], 
            name="block velocity",
            mode = mode,
        )
        , row=4, col=1,
        secondary_y = True
        )

    fig['layout']['yaxis8']['title'] = 'Velocity (m/s)'

    ''' row 5 '''

    fig.add_trace(
        go.Scatter(
            x=datas['Time'], 
            y=datas['DRILLING_VFLOW'], 
            name="flowrate",
            mode = mode
        )
        , row=5, col=1
        )

    fig['layout']['yaxis9']['title'] = 'Flowrate (gal/min)'

    ''' row 6 '''

    fig.add_trace(
        go.Scatter(
            x=datas['Time'], 
            y=datas['DEPTH_BD'], 
            name="bit depth",
            mode = mode,
        )
        , row=6, col=1,
        secondary_y = False
        )

    fig['layout']['yaxis11']['title'] = 'Depth (ft)'

    fig.add_trace(
        go.Scatter(
            x=datas['Time'], 
            y=datas['DEPTH_TD'], 
            name="hole depth",
            mode = mode
        )
        , row=6, col=1,
        )

    if labels is not None:
        for (s,e) in labels:
            fig.add_vrect(
                x0=s, x1=e,
                y0=0, y1=1,
                fillcolor="Black",
                opacity = 0.5,
                layer="below", line_width=0,
            )
            print(s, e)

    fig.update_layout(title = fr'{well_name} - HCI',)
    fig.write_html(save_path)


well_names = ['3s-624_Run3_bha', '3s-617_Run3_bha']
well_labels = ['', r"C:\Users\Jyu20\Downloads\3S-617_Run3_bha_ECD.csv"]
time_keys = ['20240601000000', '']
for well_name, time_key, well_label in zip(well_names, time_keys, well_labels):
    input_base_dir = r'C:\NotOneDrive\Data\algo_seperate_data'
    algo_output_base_dir = r"C:\NotOneDrive\Data\merged_input_output_channels"
    output_base_dir = r'C:\NotOneDrive\Data\unified_plots'

    input_path = os.path.join(input_base_dir, well_name, 'Channels.csv')
    inputs = pd.read_csv(input_path, low_memory=False, skiprows=[1]) [[
        'Time',

        'STANDPIPE_PRS',

        'DrilBHReam.ROP5',
        'DRILLING_VFLOW',
        'AutoState.BVEL',

        'DEPTH_BD',
        'HOOK_HEIGHT',
        'DEPTH_TD'
    ]]
    print(len(inputs))
    inputs['DrilBHReam.ROP5'] *= unit_convert('m/s', 'm/h')
    inputs['DRILLING_VFLOW'] *= unit_convert('m3/s', 'gal/min')
    inputs['STANDPIPE_PRS'] *= unit_convert('pa', 'psi')

    if time_key:
        output_path = os.path.join(algo_output_base_dir, fr'{well_name}_TransientHydraulics_{time_key}.csv')
    else:
        output_path = os.path.join(algo_output_base_dir, fr'{well_name}_TransientHydraulics.csv')

    algo_outputs = pd.read_csv(output_path, low_memory=False)[[
        'time key',

        'max hci',
        'mean hci',

        'max cutting concentration',
        'mean cutting concentration',
        'max cutting bed height',
        'mean cutting bed height',

        'ecd at bit',
        'ecd at bit no cuttings',

        'simulated spp',
    ]]
    algo_outputs['max cutting concentration'] *= 100
    algo_outputs['mean cutting concentration'] *= 100
    algo_outputs['simulated spp'] *= unit_convert('pa', 'psi')
    algo_outputs['ecd at bit'] *= unit_convert('kg/m3', 'lb/gal')
    algo_outputs['ecd at bit no cuttings'] *= unit_convert('kg/m3', 'lb/gal')
    algo_outputs = algo_outputs[
        algo_outputs['time key'] < inputs['Time'].values[-1] 
        ]
    algo_outputs = algo_outputs[
        algo_outputs['time key'] > inputs['Time'].values[0]
        ]

    m_ecd_path = os.path.join(input_base_dir, f'{well_name}', 'Channel_ECD.csv')
    m_ecds = pd.read_csv(m_ecd_path, names=['time_key', 'measured_ecd'])
    m_ecds = m_ecds[
        m_ecds['time_key'] < inputs['Time'].values[-1] 
        ]
    m_ecds = m_ecds[
        m_ecds['time_key'] > inputs['Time'].values[0]
        ]
    m_ecds['measured_ecd'] *= unit_convert('kg/m3', 'lb/gal')

    inputs[['DEPTH_BD', 'HOOK_HEIGHT', 'DEPTH_TD']] = inputs[['DEPTH_BD', 'HOOK_HEIGHT', 'DEPTH_TD']].apply(lambda x: x * unit_convert('m', 'ft'))

    labels = None
    if well_label:
        labels_raw = pd.read_csv(well_label)
        labels = []
        for index,l in labels_raw.iterrows():
            labels.append(
                (l['start time'], l['end time'])
            )

    save_path = os.path.join(output_base_dir, f'{well_name}_HCI.html')
    plot(inputs[::], algo_outputs[::], m_ecds[::], 'lines', save_path, labels)
    sample = 50
    save_path = os.path.join(output_base_dir, f'{well_name}_HCI_sample{sample}.html')
    plot(inputs[::sample], algo_outputs[::sample], m_ecds[::sample], 'lines', save_path, labels)
    sample = 100
    save_path = os.path.join(output_base_dir, f'{well_name}_HCI_sample{sample}.html')
    plot(inputs[::sample], algo_outputs[::sample], m_ecds[::sample], 'lines', save_path, labels)