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


def plot(datas: pd.DataFrame, m_ecds: pd.DataFrame, mode: str, save_path: str, labels = None):
    fig = make_subplots(rows = 5, cols = 1,
                        specs=[
                            [{"secondary_y": True}],
                            [{"secondary_y": True}],
                            [{"secondary_y": True}],
                            [{"secondary_y": True}],
                            [{"secondary_y": True}]],
                            shared_xaxes = True
                            )

    ''' row 1 '''

    fig.add_trace(
        go.Scatter(
            x=datas['Time'], 
            y=datas['STANDPIPE_PRS'], 
            name="measured standpipe pressure",
            mode = mode
        )
        , row=1, col=1,
        secondary_y = False
        )
    
    fig['layout']['yaxis']['title'] = 'Standpipe pressure (psi)'

    fig.add_trace(
        go.Scatter(
            x=m_ecds['time_key'], 
            y=m_ecds['measured_ecd'], 
            name="measured ecd",
            mode = 'markers',
            marker = dict(
                size = 5,
                opacity=0.5,
            )
        )
        , row=1, col=1,
        secondary_y = True
        )

    fig['layout']['yaxis2']['title'] = 'ECD (lb/gal)'

    ''' row 2 '''

    fig.add_trace(
        go.Scatter(
            x=datas['Time'], 
            y=datas['DRILLING_VFLOW'], 
            name="flowrate",
            mode = mode
        )
        , row=2, col=1,
        secondary_y = False
        )

    fig['layout']['yaxis3']['title'] = 'Flowrate (gal/min)'

    ''' row 3 '''

    fig.add_trace(
        go.Scatter(
            x=datas['Time'], 
            y=datas['DOWNHOLE_WOB'], 
            name="downhole wob",
            mode = mode
        )
        , row=3, col=1
        )

    fig['layout']['yaxis5']['title'] = 'WOB (klbf)'

    fig.add_trace(
        go.Scatter(
            x=datas['Time'], 
            y=datas['SURFACE_TRQ'], 
            name="surface torque",
            mode = mode,
        )
        , row=3, col=1,
        secondary_y = True
        )

    fig['layout']['yaxis6']['title'] = 'Torque (ft.lbf)'

    ''' row 4 '''

    fig.add_trace(
        go.Scatter(
            x=datas['Time'], 
            y=datas['Surface.RPM'], 
            name="surface rpm",
            mode = mode
        )
        , row=4, col=1,
        )

    fig['layout']['yaxis7']['title'] = 'Rpm (c/min)'

    ''' row 5 '''

    fig.add_trace(
        go.Scatter(
            x=datas['Time'], 
            y=datas['DEPTH_BD'], 
            name="bit depth",
            mode = mode
        )
        , row=5, col=1
        )

    fig['layout']['yaxis9']['title'] = 'Depth (ft)'

    fig.add_trace(
        go.Scatter(
            x=datas['Time'], 
            y=datas['HOOK_HEIGHT'], 
            name="hook height",
            mode = mode,
        )
        , row=5, col=1,
        secondary_y = True
        )

    fig['layout']['yaxis10']['title'] = 'Hook height (ft)'

    fig.add_trace(
        go.Scatter(
            x=datas['Time'], 
            y=datas['DEPTH_TD'], 
            name="hole depth",
            mode = mode
        )
        , row=5, col=1,
        # secondary_y = True
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

    fig.update_layout(title = fr'{well_name} - APD',)
    fig.write_html(save_path)


well_names = ['3s-617_Run3_bha',]
well_labels = [ r"C:\Users\Jyu20\Downloads\3S-617_Run3_APD.csv"]
input_base_dir = r'C:\NotOneDrive\Data\algo_seperate_data'
output_base_dir = r'C:\NotOneDrive\Data\unified_plots'

for well_name, well_label in  zip(well_names, well_labels):

    input_path = os.path.join(input_base_dir, well_name, 'Channels.csv')
    inputs = pd.read_csv(input_path, low_memory=False, skiprows=[1]) [[
        'Time',

        'STANDPIPE_PRS',
        'DRILLING_VFLOW',

        'DOWNHOLE_WOB',
        'SURFACE_TRQ',
        'Surface.RPM',

        'DEPTH_BD',
        'HOOK_HEIGHT',
        'DEPTH_TD'
    ]]
    print(len(inputs))
    inputs['STANDPIPE_PRS'] *= unit_convert('pa', 'psi')
    inputs['DRILLING_VFLOW'] *= unit_convert('m3/s', 'gal/min')
    inputs['DOWNHOLE_WOB'] *= unit_convert('n', 'klbf')
    inputs['SURFACE_TRQ'] *= unit_convert('n.m', 'ft.lbf')
    inputs['Surface.RPM'] *= unit_convert('rad/s', 'c/min')

    inputs[['DEPTH_BD', 'HOOK_HEIGHT', 'DEPTH_TD']] = inputs[['DEPTH_BD', 'HOOK_HEIGHT', 'DEPTH_TD']].apply(lambda x: x * unit_convert('m', 'ft'))

    # apd_output_path = os.path.join(input_base_dir, well_name, 'apd_output.csv')
    # apd_output = pd.read_csv(apd_output_path, low_memory=False, skiprows=[1])[[
    #     'UtcTime',
    #     'AbnormalPressure.DSPP'
    # ]]

    m_ecd_path = os.path.join(input_base_dir, f'{well_name}', 'Channel_ECD.csv')
    m_ecds = pd.read_csv(m_ecd_path, names=['time_key', 'measured_ecd'])
    m_ecds = m_ecds[
        m_ecds['time_key'] < inputs['Time'].values[-1] 
        ]
    m_ecds = m_ecds[
        m_ecds['time_key'] > inputs['Time'].values[0]
        ]
    m_ecds['measured_ecd'] *= 0.00834549445

    labels = None
    if well_label:
        labels_raw = pd.read_csv(well_label)
        labels = []
        for index,l in labels_raw.iterrows():
            labels.append(
                (l['Start Time'], l['End Time'])
            )

    save_path = os.path.join(output_base_dir, f'{well_name}_APD.html')
    plot(inputs[::], m_ecds, 'lines', save_path, labels)
    sample = 50
    save_path = os.path.join(output_base_dir, f'{well_name}_APD_sample{sample}.html')
    plot(inputs[::sample], m_ecds, 'lines', save_path, labels)
    sample = 100
    save_path = os.path.join(output_base_dir, f'{well_name}_APD_sample{sample}.html')
    plot(inputs[::sample], m_ecds, 'lines', save_path, labels)