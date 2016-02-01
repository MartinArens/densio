#!/usr/bin/python3

import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
import requests
import json

docId = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'
title = 'title'
subTitle = 'sub title'
unit = '€'
timeslice = 'M'
reportdate = '2016-01-01'
username = 'user@domain.tld'
password = 'password'
header = {'content-type': 'application/json'}
url = 'https://portal.densio.com/api/documents/{0}/update/'.format(docId)
engine = create_engine('mysql+mysqlconnector://username:password@host/database')

sqlQuery = sqlalchemy.text("""select month, dimension, values
                            from table
                            where month = :month
                            group by month
                            order by month;""")

dataFrame = pd.read_sql(sqlQuery, engine, params={'month': '2016-01-01'})

dataPivot = dataFrame.pivot(index='dimension', columns='month', values='values')

dataCsv = dataPivot.to_csv(sep=',', encoding='utf-8')

data = {}
data['username'] = username
data['password'] = password
data['csv'] = {}
data['csv']['title'] = title
data['csv']['subtitle'] = subTitle
data['csv']['unit'] = unit
data['csv']['timeslice'] = timeslice
data['csv']['reportdate'] = reportdate
data['csv']['content'] = dataCsv
dataApi = json.dumps(data)

d = requests.post(url, data=dataApi, headers=header, verify=False)
print(d.status_code)
print(d.json())
