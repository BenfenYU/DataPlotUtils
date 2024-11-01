
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

def every_nth(nums, nth):
    # Use list slicing to return elements starting from the (nth-1) index, with a step of 'nth'.
    return nums[nth - 1::nth]

def filter_by_time_key(df, key_name, min_time, max_time):
    return df[(df[key_name] > min_time) & (df[key_name] < max_time)]


'''
sspp, m_spp, drill_string_pressure_loss, bit depth, flowrate
'''
def plot_spp():
    sub_spp_data = outputs[[
        'drill string pressure loss',
        'simulated spp',
        'time key',
    ]].dropna()
    m_spp = channels[['m spp', 'time key']].dropna()
    m_flowrate = channels[['time key', 'flowrate']].dropna()
    bit_depth = channels[['time key', 'bit depth']].dropna()

    pressure_loss_max = max(
        sub_spp_data['drill string pressure loss'].max(),
        sub_spp_data['simulated spp'].max(),
        m_spp['m spp'].max()
    )
    pressure_loss_min = min(
        sub_spp_data['drill string pressure loss'].min(),
        sub_spp_data['simulated spp'].min(),
        m_spp['m spp'].min()
    )

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=sub_spp_data['time key'], 
            y=sub_spp_data['drill string pressure loss'], 
            name='drill string pressure loss'
        ),
    ) 

    fig.add_trace(
        go.Scatter(
            x=m_flowrate['time key'], 
            y=m_flowrate['flowrate'], 
            name="flowrate",
            yaxis='y2'),
    )

    fig.add_trace(
        go.Scatter(
            x=sub_spp_data['time key'], 
            y=sub_spp_data['simulated spp'], 
            name="Simulated SPP",
            yaxis='y3'),
    )

    fig.add_trace(
        go.Scatter(
            x=bit_depth['time key'], 
            y=bit_depth['bit depth'], 
            name="bit depth",
            yaxis='y4'),
    )

    fig.add_trace(
        go.Scatter(
            x=m_spp['time key'], 
            y=m_spp['m spp'], 
            name="measured spp",
            yaxis='y5'),
    )

    fig.update_layout(
        title = fr'{well_name} - RT CHE - Standpipe pressure',
        yaxis=dict(title="drill string pressure loss (pa)",
            range = [pressure_loss_min, pressure_loss_max]
                ),
        yaxis2=dict(title="flowrate m3/s",        
            anchor="free",
            overlaying="y",
            side="left",
            autoshift = True),
        yaxis3=dict(title="Simulated spp (pa)",
                    anchor="free",
            overlaying="y",
            autoshift = True,
            range = [pressure_loss_min, pressure_loss_max]
            ),
        yaxis4=dict(title="bit depth (m)", anchor="free",
            overlaying="y",
            side="right",
            autoshift = True
            ),
        yaxis5=dict(title="standpipe pressure (Pa)", anchor="free",
            overlaying="y",
            # side="right",
            autoshift = True,
            range = [pressure_loss_min, pressure_loss_max]
            )
        )

    if packoff_time_key:
        fig.add_vline(x=packoff_time_key, line_width=3, line_dash="dash", line_color="green", name = 'Stuckpipe detected')
    fig.write_html(fr"images\{well_name}_TransientHydraulics_SPP_{time_stamp_str}.html")

def plot_ecd():
    sub_data_ecd = outputs[[
        'ecd at bit',
        'time key',
    ]].dropna(how='any')

    bit_depth_sub = channels[[
        'time key',
        'bit depth'
    ]].dropna()

    if 'ecd at bit no cuttings' in outputs.keys():
        ecd_nocutting_sub = outputs[[
            'time key',
            'ecd at bit no cuttings',
        ]].dropna()
    else:
        ecd_nocutting_sub = None


    m_ecd = channel_ecds[['time key', 'm ecd at bit']].dropna()
    m_ecd = m_ecd[m_ecd['time key'] < bit_depth_sub['time key'].values[-1]]
    m_ecd = m_ecd[m_ecd['time key'] > bit_depth_sub['time key'].values[0]]

    ecd_max = max(
        m_ecd['m ecd at bit'].max(),
        sub_data_ecd['ecd at bit'].max(),
    )
    ecd_min = min(
        m_ecd['m ecd at bit'].min(),
        sub_data_ecd['ecd at bit'].min(),
    )
    '''
    measured ecd at bit, simulated ecd at bit
    '''

    fig = go.Figure()

    # fig.add_trace(
    #     go.Scattergl(
    #         x=sub_data_ecd['time key'], 
    #         y=sub_data_ecd['ecd at bit'], 
    #         name='simulated ecd at bit'
    #     ),
    # ) 

    fig.add_trace(
        go.Scattergl(
            # mode = 'markers',
            # marker = dict(
            #     size = 2
            # ),
            x=m_ecd['time key'], 
            y=m_ecd['m ecd at bit'], 
            name="measured ecd at bit",
            yaxis='y2'),
    )

    # fig.add_trace(
    #     go.Scatter(
    #         # mode = 'markers',
    #         # marker = dict(
    #         #     size = 5
    #         # ),
    #         x=result['time_key'], 
    #         y=result['ecd_difference'], 
    #         name="ecd difference",
    #         yaxis='y3'),
    # )

    fig.add_trace(
        go.Scattergl(
            x=bit_depth_sub['time key'], 
            y=bit_depth_sub['bit depth'], 
            name="bit depth",
            yaxis='y3'),
    )

    # if ecd_nocutting_sub is not None:
    #     fig.add_trace(
    #         go.Scattergl(
    #             x=ecd_nocutting_sub['time key'], 
    #             y=ecd_nocutting_sub['ecd at bit no cuttings'], 
    #             name="ecd at bit no cuttings",
    #             yaxis='y4',
    #             opacity = 0.9,),
    #     )

    fig.add_trace(
        go.Scatter(
            x=m_flowrate['time key'], 
            y=m_flowrate['flowrate'], 
            name="flowrate",
            yaxis='y5'),
    )

    rpm = channels[[
        'rpm',
        'time key',
    ]].dropna()
    hookload = channels[[
        'hookload',
        'time key',
    ]].dropna()
    fig.add_trace(
        go.Scatter(
            x=rpm['time key'], 
            y=rpm['rpm'], 
            name="rpm",
            yaxis='y6'),
    )
    fig.add_trace(
        go.Scatter(
            x=hookload['time key'], 
            y=hookload['hookload'], 
            name="hookload",
            yaxis='y7'),
    )

    fig.update_layout(
        title = fr'{well_name} - RT CHE - ECD at bit',
        yaxis=dict(title="simulated ecd at bit",
                range = [ecd_min, ecd_max]),
        yaxis2=dict(title="measured ecd at bit ",        
            anchor="free",
            overlaying="y",
            side="left",
            autoshift = True,
            range = [ecd_min, ecd_max]),
        # yaxis3=dict(title="ecd difference",        
        #     anchor="free",
        #     overlaying="y",
        #     side="left",
        #     autoshift = True),
        yaxis3=dict(title="bit depth",        
            anchor="free",
            overlaying="y",
            side="left",
            autoshift = True),
        yaxis4=dict(title="ecd at bit no cuttings",        
            anchor="free",
            overlaying="y",
            side="left",
            autoshift = True,
            range = [ecd_min, ecd_max]),
        yaxis5=dict(title="flowrate",        
            anchor="free",
            overlaying="y",
            side="left",
            autoshift = True),
        yaxis6=dict(title="rpm",        
            anchor="free",
            overlaying="y",
            side="left",
            autoshift = True),
        yaxis7=dict(title="hookload",        
            anchor="free",
            overlaying="y",
            side="left",
            autoshift = True),
        )

    fig.add_vline(x=packoff_time_key, line_width=3, line_dash="dash", line_color="green", name = 'Stuckpipe detected')
    fig.write_html(fr"images\{well_name}_TransientHydraulics_ECD_{time_stamp_str}.html")
    # fig.show()

def plot_ecd_cutomized_lei_label_ecd(algo_outputs, ecd, algo_input_channels, sample_rate = 5):

    # print(algo_input_channels['flowrate'].dropna())
    # algo_outputs = algo_outputs.iloc[::sample_rate]
    # algo_input_channels = algo_input_channels.iloc[::sample_rate]

    sub_data_ecd = algo_outputs[[
        'ecd at bit',
        'time key',
    ]].dropna(how='any')

    sub_data_flowrate = algo_input_channels[[
        'flowrate',
        'time key',
    ]].dropna(how='any')

    bit_depth_sub = algo_input_channels[[
        'time key',
        'bit depth'
    ]].dropna().iloc[::sample_rate]

    # if 'ecd at bit no cuttings' in algo_outputs.keys():
    #     ecd_nocutting_sub = algo_outputs[[
    #         'time key',
    #         'ecd at bit no cuttings',
    #     ]].dropna()
    # else:
    #     ecd_nocutting_sub = None


    m_ecd = ecd[['time key', 'm ecd at bit']].dropna()
    m_ecd = m_ecd[m_ecd['time key'] < algo_input_channels['time key'].values[-1]]
    m_ecd = m_ecd[m_ecd['time key'] > algo_input_channels['time key'].values[0]]
    # m_ecd['time key'] = pd.to_datetime(m_ecd['time key']) - pd.Timedelta(hours=1)

    ecd_max = max(
        # m_ecd['m ecd at bit'].max(),
        sub_data_ecd['ecd at bit'].max(),
        0
    )
    ecd_min = min(
        # m_ecd['m ecd at bit'].min(),
        sub_data_ecd['ecd at bit'].min(),
        0
    )
    '''
    measured ecd at bit, simulated ecd at bit
    '''

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            mode = 'markers',
            marker = dict(
                size = 2
            ),
            x=m_ecd['time key'], 
            y=m_ecd['m ecd at bit'], 
            name="measured ecd at bit",
            yaxis='y'),
    )

    fig.add_trace(
        go.Scatter(
            x=sub_data_ecd['time key'], 
            y=sub_data_ecd['ecd at bit'], 
            name="modeled ecd at bit",
            yaxis='y'),
    )

    # if ecd_nocutting_sub is not None:
    #     fig.add_trace(
    #         go.Scatter(
    #             x=ecd_nocutting_sub['time key'], 
    #             y=ecd_nocutting_sub['ecd at bit no cuttings'], 
    #             name="modeled ecd at bit without cuttings",
    #             yaxis='y'),)

    fig.update_layout(
        yaxis=dict(
            title="ecd",        
            # anchor="free",
            side="left",
            autoshift = True,
            range = [1200, 1700]),
    )

    fig.add_trace(
        go.Scatter(
            x=sub_data_flowrate['time key'], 
            y=sub_data_flowrate['flowrate'], 
            name="flowrate",
            yaxis='y2'),
    )
    # print(algo_input_channels['flowrate'].dropna())

    fig.update_layout(
        yaxis2=dict(
            title="flowrate",        
            anchor="free",
            side="left",
            autoshift = True,
            overlaying="y",
            ),
    )

    # rpm = algo_input_channels[[
    #     'rpm',
    #     'time key',
    # ]].dropna()
    # hookload = algo_input_channels[[
    #     'hookload',
    #     'time key',
    # ]].dropna()

    # fig.add_trace(
    #     go.Scatter(
    #         x=rpm['time key'], 
    #         y=rpm['rpm'], 
    #         name="rpm",
    #         yaxis='y3'),
    # )

    # fig.update_layout(
    #     yaxis3=dict(
    #         title="rpm",        
    #         anchor="free",
    #         side="left",
    #         autoshift = True,
    #         overlaying="y",
    #         ),
    # )



    fig.add_trace(
        go.Scatter(
            x=bit_depth_sub['time key'], 
            y=bit_depth_sub['bit depth'], 
            name="bit depth",
            yaxis='y3'),
    )

    fig.update_layout(
        yaxis3=dict(
            title="depth",        
            anchor="free",
            side="right",
            autoshift = True,
            overlaying="y",
            ),
    )


    hookloads = algo_input_channels[[
        'hookload',
        'time key',
    ]].dropna(how='any')
    fig.add_trace(
        go.Scatter(
            x=hookloads['time key'], 
            y=hookloads['hookload'], 
            name="hookload",
            yaxis='y4'),
    )

    fig.update_layout(
        yaxis4=dict(
            title="hookload",        
            anchor="free",
            side="left",
            autoshift = True,
            overlaying="y",
            range = [hookloads['hookload'].min(), hookloads['hookload'].max() * 3]
            ),
    )

    # fig.add_trace(
    #     go.Scatter(
    #         x=algo_input_channels['time key'], 
    #         y=algo_input_channels['AutoState.BVEL'], 
    #         name="block velocity",
    #         yaxis='y6'),
    # )

    # fig.update_layout(
    #     yaxis6=dict(
    #         title="block velocity",        
    #         anchor="free",
    #         side="right",
    #         autoshift = True,
    #         overlaying="y",
    #         ),
    # )

    # fig.add_trace(
    #     go.Scatter(
    #         x=algo_input_channels['time key'], 
    #         y=algo_input_channels['SURFACE_TRQ'], 
    #         name="surface torque",
    #         yaxis='y4',
    #         ),
    # )

    # fig.update_layout(
    #     yaxis4=dict(
    #         title="torque",        
    #         anchor="free",
    #         side="right",
    #         autoshift = True,
    #         overlaying="y",
    #         range = [algo_input_channels['SURFACE_TRQ'].min(), algo_input_channels['SURFACE_TRQ'].max() * 3]
    #         ),
    # )

    # fig.add_trace(
    #     go.Scatter(
    #         x=algo_input_channels['time key'], 
    #         y=algo_input_channels['m_spp'], 
    #         name="measured spp",
    #         yaxis='y8',
    #         ),
    # )

    # fig.update_layout(
    #     yaxis8=dict(
    #         title="measured spp",        
    #         anchor="free",
    #         side="right",
    #         autoshift = True,
    #         overlaying="y",
    #         ),
    # )

    fig.update_layout(
        title = fr'{well_name} - RT CHE - ECD at bit',
        )

    # fig.add_vline(x=packoff_time_key, line_width=3, line_dash="dash", line_color="green", name = 'Stuckpipe detected')
    fig.write_html(os.path.join(r"C:\NotOneDrive\Data\for_hci", fr"{well_name}_TransientHydraulics_ECD_{time_stamp_str}.html"))
    # fig.show()

def plot_ecd_cutomized_mario_compare_ecd(algo_outputs, ecd, algo_input_channels):

    sub_data_ecd = algo_outputs[[
        'ecd at bit',
        'time key',
    ]].dropna(how='any')

    bit_depth_sub = algo_input_channels[[
        'time key',
        'bit depth'
    ]].dropna()

    if 'ecd at bit no cuttings' in algo_outputs.keys():
        ecd_nocutting_sub = algo_outputs[[
            'time key',
            'ecd at bit no cuttings',
        ]].dropna()
    else:
        ecd_nocutting_sub = None


    m_ecd = ecd[['time key', 'm ecd at bit']].dropna()
    m_ecd = m_ecd[m_ecd['time key'] < bit_depth_sub['time key'].values[-1]]
    m_ecd = m_ecd[m_ecd['time key'] > bit_depth_sub['time key'].values[0]]
    # m_ecd['time key'] = pd.to_datetime(m_ecd['time key']) - pd.Timedelta(hours=1)

    ecd_max = max(
        m_ecd['m ecd at bit'].max(),
        sub_data_ecd['ecd at bit'].max(),
    )
    ecd_min = min(
        m_ecd['m ecd at bit'].min(),
        sub_data_ecd['ecd at bit'].min(),
    )
    '''
    measured ecd at bit, simulated ecd at bit
    '''

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=m_ecd['time key'], 
            y=m_ecd['m ecd at bit'], 
            name="measured ecd at bit",
            yaxis='y'),
    )

    fig.add_trace(
        go.Scatter(
            x=sub_data_ecd['time key'], 
            y=sub_data_ecd['ecd at bit'], 
            name="modeled ecd at bit",
            yaxis='y'),
    )

    if ecd_nocutting_sub is not None:
        fig.add_trace(
            go.Scatter(
                x=ecd_nocutting_sub['time key'], 
                y=ecd_nocutting_sub['ecd at bit no cuttings'], 
                name="measured ecd at bit without cuttings",
                yaxis='y'),)

    fig.update_layout(
        yaxis=dict(
            title="ecd",        
            # anchor="free",
            side="left",
            autoshift = True,
            range = [ecd_min, ecd_max]),
    )

    fig.add_trace(
        go.Scatter(
            x=algo_input_channels['time key'], 
            y=algo_input_channels['flowrate'], 
            name="flowrate",
            yaxis='y2'),
    )

    fig.update_layout(
        yaxis2=dict(
            title="flowrate",        
            anchor="free",
            side="left",
            autoshift = True,
            overlaying="y",
            ),
    )

    fig.add_trace(
        go.Scatter(
            x=algo_input_channels['time key'], 
            y=algo_input_channels['bit depth'], 
            name="bit depth",
            yaxis='y5'),
    )

    fig.update_layout(
        yaxis5=dict(
            title="depth",        
            anchor="free",
            side="right",
            autoshift = True,
            overlaying="y",
            ),
    )

    fig.update_layout(
        title = fr'{well_name} - RT CHE - ECD at bit',
        )

    # fig.add_vline(x=packoff_time_key, line_width=3, line_dash="dash", line_color="green", name = 'Stuckpipe detected')
    fig.write_html(fr"cutomized_ecd_for_Lei\{well_name}_TransientHydraulics_ECD_{time_stamp_str}.html")
    # fig.show()


well_names = [
    'BDC-4-02_bha_run1_correct_mud_TransientHydraulics',
    ]
time_stamp_strs = [
    '20240813175608_surface_cool',
    ]

channel_ecd_path = r"C:\NotOneDrive\Data\algo_data_nas_copy\BDC 4-02\RawData\Channel_ECD_ESD.csv"
channel_ecds = pd.read_csv(channel_ecd_path, skiprows=[1]).rename(
    columns={
        'Time': 'time key',
        'ECD_ARC_RT': 'm ecd at bit'
    }
)
channel_ecds['m ecd at bit'] *= unit_convert('g/cm3', 'kg/m3')
channel_ecds['time key'] = pd.to_datetime(channel_ecds['time key'], utc=False)

for well_name, time_stamp_str in zip(well_names, time_stamp_strs): 
    data_base_dir = rf'C:\NotOneDrive\Data\algo_seperate_data'
    output_base_dir = r'C:\NotOneDrive\Data\merged_input_output_channels'
    engine_output_channels_path = os.path.join(output_base_dir, f'{well_name}' + (f'_{time_stamp_str}' if time_stamp_str else '') + '.csv')
    # engine_output_channels_path = r"C:\NotOneDrive\Data\algo_seperate_data\BDC-4-02_bha_run1\Channels.csv"

    # channel_ecd_path = os.path.join(data_base_dir, well_name, 'Channel_ECD.csv')
    # channel_ecds = pd.read_csv(channel_ecd_path, names = ['time key', 'm ecd at bit']).dropna()

    # channels_path = os.path.join(data_base_dir, well_name, 'Channels.csv')
    channels_path = r"C:\NotOneDrive\Data\algo_seperate_data\BDC-4-02_bha_run1\Channels.csv"
    channels = pd.read_csv(channels_path, skiprows=[1])
    channels = channels.rename(columns={
        'Time': 'time key',
        'DRILLING_VFLOW': 'flowrate',
        'DEPTH_BD': 'bit depth',
        'STANDPIPE_PRS': 'm_spp',
        'HOOK_LOAD': 'hookload',
        'Surface.RPM': 'rpm'
    })
    channels['time key'] = pd.to_datetime(channels['time key'], utc=False)

    outputs = pd.read_csv(engine_output_channels_path, skiprows=[1])
    outputs[['ecd at bit']] = outputs[['ecd at bit']].apply(pd.to_numeric)
    outputs[['simulated spp']] = outputs[['simulated spp']].apply(pd.to_numeric)
    outputs[['drill string pressure loss']] = outputs[['drill string pressure loss']].apply(pd.to_numeric)
    outputs['time key'] = pd.to_datetime(outputs['time key'], utc=False)


    plot_ecd_cutomized_lei_label_ecd(outputs, channel_ecds, channels)