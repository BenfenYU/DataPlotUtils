import pandas as pd
from copy import deepcopy
import os, json

demo_alarm_path = ""
csv_path = r"C:\Users\Jyu20\Downloads\Pack_Off_Alarm.csv"
save_dir = r'C:\Users\Jyu20\OneDrive - SLB\Code\rhapsody_algorithm\DataProcessAndVisualize\test'

_type = 'PackoffOverallRisk'
_title = 'Pack off Overall Risk Alarm'

alarm_format =   {
    "type": "PackOffRisk",
    "severity": "",
    "title": _title,
    "message": "",
    "convertibleMessage": {
      "expression": "",
      "values": None
    },
    "reason": "",
    "notificationClass": "",
    "notificationLevel": 0,
    "WorkOrderId": "",
    "Timestamp": ""
  }

alarms = pd.read_csv(csv_path)


_alarms = []
for i, r in alarms.iterrows():


    t = pd.to_datetime(r['Start Time'])
    _a = deepcopy(alarm_format)
    _a['severity'] = r['Severity']
    _a['Timestamp'] = str(t)
    file_path = os.path.join(save_dir, t.strftime("%Y%m%d%H%M%S%f") + '.json')
    with open(file_path, 'w') as f:
        json.dump(_a, f)


    t = pd.to_datetime(r['End Time'])
    _a = deepcopy(alarm_format)
    _a['severity'] = 'Inactive'
    _a['Timestamp'] = str(t)
    file_path = os.path.join(save_dir, t.strftime("%Y%m%d%H%M%S%f") + '.json')
    with open(file_path, 'w') as f:
        json.dump(_a, f)
    