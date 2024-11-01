
import datetime
import json


unit_pairs = {
    ('kkgf', 'n') : 9806.65,
    ('n', 'klbf') : 0.0002248089431,

    ('n.m', 'kn.m') : 0.001,
    ('n.m', 'ft.lbf') : 0.7375621493,

    ('pa', 'psi') : 0.000145037738,
    ('pa', 'kpa') : 0.001,

    ('kg/m3', 'lb/gal') : 0.00834540445,
    ('g/cm3', 'kg/m3'): 1000,
    ('g/cm3', 'lb/gal'): 8.34540445,

    ('m3/s', 'l/min') : 60000,
    ('m3/s', 'gal/min') : 15850.3231,
    
    ('c/min', 'rad/s') : 0.1047198,

    ('m/s', 'm/h') : 1/60,

    ('m', 'ft') : 3.2808399,

    ('rad', 'degree'): 57.2957795,

    # grms, unit of acceleration
    ('ft/s2', 'm/s2'): 0.3048
}

def unit_convert(start_unit: str, end_unit: str):
    unit_pair = (start_unit.lower(), end_unit.lower())
    if unit_pair in unit_pairs.keys():
        return unit_pairs[unit_pair]
    
    unit_pair = (end_unit.lower(), start_unit.lower())
    if unit_pair in unit_pairs.keys():
        return 1/unit_pairs[unit_pair]
    
    raise Exception()

OLE_TIME_ZERO = datetime.datetime(1899, 12, 30, 0, 0, 0)

def ole2datetime(oledt):
    return OLE_TIME_ZERO + datetime.timedelta(days=float(oledt))

def load_trajectory(path):
    with open(path, 'r') as f:
        traj = json.load(f)

    stations = traj['trajectoryStation']

    res = {
        'mds': [],
        'tvds': [],
        'incls': []
    }

    for station in stations:
        res['tvds'].append(float(station['tvd']['value']))
        res['incls'].append(float(station['incl']['value']) * unit_convert('rad', 'degree'))
        res['mds'].append(float(station['md']['value']))

    return res