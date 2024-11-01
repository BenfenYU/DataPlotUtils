import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import json
from math import isnan
import pandas as pd
from plotly_resampler import FigureResampler
import os
from utils import unit_convert

def plot(datas: pd.DataFrame, mode: str, save_path: str, labels = None):
    fig = make_subplots(rows = 2, cols = 1,
                        specs=[[{"secondary_y": True}],
                            [{"secondary_y": True}]],
                        shared_xaxes = True)

    fig.add_trace(
        go.Scatter(
            x=datas['Time'], 
            y=datas['HOOK_LOAD'], 
            name="hookload",
            mode = mode
        )
        , row=1, col=1,
        secondary_y = False
        )

    fig['layout']['yaxis']['title'] = 'Hookload (klbf)'

    fig.add_trace(
        go.Scatter(
            x=datas['Time'], 
            y=datas['SURFACE_TRQ'], 
            name="surface torque",
            mode = mode
        )
        , row=1, col=1,
        secondary_y = True
        )

    fig['layout']['yaxis2']['title'] = 'Surface torque (ft.lbf)'

    fig.add_trace(
        go.Scatter(
            x=datas['Time'], 
            y=datas['DEPTH_BD'], 
            name="bit depth",
            mode = mode
        )
        , row=2, col=1
        )

    fig.add_trace(
        go.Scatter(
            x=datas['Time'], 
            y=datas['DEPTH_TD'], 
            name="hole depth",
            mode = mode,
        )
        , row=2, col=1
        )

    fig['layout']['yaxis3']['title'] = 'Depth (ft)'

    fig.add_trace(
        go.Scatter(
            x=datas['Time'], 
            y=datas['HOOK_HEIGHT'], 
            name="hook height",
            mode = mode
        )
        , row=2, col=1,
        secondary_y = True
        )

    fig['layout']['yaxis4']['title'] = 'Hook height (ft)'

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

    fig.update_layout(title = fr'{well_name} - HookLoad',)
    fig.write_html(save_path)


well_names = ['3s-617_Run3_bha']
well_labels = [r"C:\Users\Jyu20\Downloads\3S-617_Run3_TnD_label.csv"]
input_base_dir = r'C:\NotOneDrive\Data\algo_seperate_data'
output_base_dir = r'C:\NotOneDrive\Data\unified_plots'

for well_name, well_label in zip(well_names, well_labels):
    input_path = os.path.join(input_base_dir, well_name, 'Channels.csv')
    inputs = pd.read_csv(input_path, low_memory=False, skiprows=[1]) [[
        'Time',
        'HOOK_LOAD',
        'SURFACE_TRQ',
        'DEPTH_BD',
        'HOOK_HEIGHT',
        'DEPTH_TD'
    ]]

    inputs['HOOK_LOAD'] *= unit_convert('n', 'klbf')
    inputs['SURFACE_TRQ'] *= unit_convert('n.m', 'ft.lbf')
    inputs[['DEPTH_BD', 'HOOK_HEIGHT', 'DEPTH_TD']] = inputs[['DEPTH_BD', 'HOOK_HEIGHT', 'DEPTH_TD']].apply(lambda x: x * unit_convert('m', 'ft'))
    print(len(inputs))

    labels = None
    if well_label:
        labels_raw = pd.read_csv(well_label)
        labels = []
        for index,l in labels_raw.iterrows():
            labels.append(
                (l['StartTime'], l['EndTime'])
            )

    save_path = os.path.join(output_base_dir, f'{well_name}_Hookload.html')
    plot(inputs[::], 'lines', save_path, labels)