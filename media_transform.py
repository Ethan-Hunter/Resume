#just making sure im commiting to ethan branch
#i will start with comment blocks, that will set up order of operations
#figure out what, if any, helper files may be needed
#I need to ask how we are getting the params in the real code, and in R code. 
    #it is read in from user as a file in R
#THE PLAN: do ad length analysis first (no file uploads for config, just data to start). Compare to R script with Matt (or someone)
from doctest import OutputChecker
import io
from multiprocessing.pool import ApplyResult
import re
import json
import os
from enum import Enum
from datetime import datetime
from sqlite3 import Row
import sys
from threading import local
import traceback
from turtle import left
from unicodedata import name
from urllib.parse import DefragResultBytes
import zipfile
from decimal import Decimal
#special imports
import math
import json
import numpy as np
import pandas
import statsmodels.api as sm
from statsmodels.stats.outliers_influence import variance_inflation_factor
from scipy import stats

import json_logger
def media_transform():
    db_entry = {}
     
    #Grab Revlevant Files
    TVadDataFrame = pandas.read_csv('TVadDATA.csv',encoding = 'unicode_escape')
    KPIdataFrame = pandas.read_csv('KPIDATA.csv',encoding = 'unicode_escape')
    MediaFrame = pandas.read_csv('media.csv',encoding = 'unicode_escape')
    #Attempt to load in config file
    f = open('configs.txt','r')
    fileConfigs = json.loads(f.read())

    #WOULD CHECK here for date formating matching, ignored for now
    
    airing_cost_temp = []
    for i in range(TVadDataFrame.shape[0]):
        if TVadDataFrame['Airing Cost'][i] == 0:
            airing_cost_temp.append(TVadDataFrame['Airing Estimated Value'][i])
        else:
            airing_cost_temp.append(TVadDataFrame['Airing Cost'][i])
    TVadDataFrame['Temp Cost'] = airing_cost_temp
    #Grab config file vars
    configs = {}
    configs["mediaCategoryIndependentVariable"] = 'cost'
    configs["dependentVariable"]= 'Sales'
    configs["includeDayOfWeekVariable"]= True
    for item in configs:
        if item in fileConfigs:
            configs[item] = fileConfigs[item]
    
    #this is where you would choose what type of regression we are doing
    #we will mimic total and cost type
    #always done, more is filtered if cut is not total

    if configs['mediaCategoryIndependentVariable'] == 'impressions':
        aggCol = 'Impressions'
    else:
        aggCol = 'Temp Cost'
    
    local_data = TVadDataFrame[TVadDataFrame['Local Cover Up'] == 1]
    local_data = local_data.groupby('Airing Date').sum().reset_index()
    local_data = local_data.rename(columns={'Airing Date':'Date',aggCol:'Local Cover Up Raw'})
    temp = local_data[local_data.columns.difference(['Date','Local Cover Up Raw'])].columns
    local_data = local_data.drop(columns=temp)
    
    program_data = TVadDataFrame[TVadDataFrame['Program Analysis'] == 1]
    program_data = program_data.groupby('Airing Date').sum().reset_index()
    program_data = program_data.rename(columns={'Airing Date':'Date',aggCol:'Program Raw'})
    temp = program_data[program_data.columns.difference(['Date','Program Raw'])].columns
    program_data = program_data.drop(columns=temp)
    
    sat_data = TVadDataFrame[TVadDataFrame['Placement Type'] == 'Satellite']
    sat_data = sat_data[sat_data['Program Analysis']==0]
    sat_data = sat_data.groupby('Airing Date').sum().reset_index()
    sat_data = sat_data.rename(columns={'Airing Date':'Date',aggCol:'Satellite Variable Raw'})
    temp = sat_data[sat_data.columns.difference(['Date','Satellite Variable Raw'])].columns
    sat_data = sat_data.drop(columns=temp)

    nat_data= TVadDataFrame[(TVadDataFrame['Local Cover Up'] == 0) & (TVadDataFrame['Program Analysis'] == 0) 
        & (TVadDataFrame['Placement Type'] != 'Satellite') & (TVadDataFrame['Placement Type'] != 'Broadcast - Local')
        & (TVadDataFrame['Placement Type'] != 'Cable - Local') & (TVadDataFrame['Placement Type'] != 'PI')
        & (TVadDataFrame['Placement Type'] != 'Streaming') & (TVadDataFrame['Placement Type'] != 'VOD')
        & (TVadDataFrame['Placement Type'] != 'Cinema')].groupby('Airing Date').sum().reset_index()
    nat_data = nat_data.rename(columns={'Airing Date':'Date',aggCol:'National Raw'})
    temp = nat_data[nat_data.columns.difference(['Date','National Raw'])].columns
    nat_data = nat_data.drop(columns=temp)

    dates = KPIdataFrame['Date']
    daynum = []
    minDate = TVadDataFrame['Airing Date'][0]
    removing = True
    i = 0
    while removing:
        if dates[i] == minDate:
            removing = False
        else:
            dates = dates.drop(i)
        i = i + 1
    dates = dates.reset_index()['Date']
    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
    dayOfWeekCol = pandas.DataFrame()
    if configs['includeDayOfWeekVariable'] == True:
        for i in range(dates.shape[0]):
            d = {'Monday':0, 'Tuesday':0, 'Wednesday':0, 'Thursday':0, 'Friday':0, 'Saturday':0, 'Date': dates[i]}
            rowToAdd = pandas.DataFrame(data=d, index=[0])
            temp = datetime.strptime(dates[i], "%m/%d/%Y").weekday()
            if temp != 6:
                rowToAdd[days[temp]] = 1
            if i ==1:
                dayOfWeekCol = rowToAdd
            else:
                dayOfWeekCol = dayOfWeekCol.append(rowToAdd)
    dayOfWeekCol.merge(dates,on='Date')  
    
    outputFrame = dayOfWeekCol.merge(local_data,on='Date', how='left')
    outputFrame = outputFrame.merge(program_data,on='Date', how='left')
    outputFrame = outputFrame.merge(sat_data,on='Date', how='left')
    outputFrame = outputFrame.merge(nat_data,on='Date', how='left')
    
    if configs['mediaCategoryIndependentVariable'] == 'impressions':
        mediaVar = 'Impressions'
    else:
        mediaVar = 'Spend'
    vodCol = MediaFrame
    temp = vodCol[vodCol.columns.difference(['Date','VOD '+mediaVar])].columns
    vodCol = vodCol.drop(columns=temp)

    ottCol = MediaFrame
    temp = ottCol[ottCol.columns.difference(['Date','OTT '+mediaVar])].columns
    ottCol = ottCol.drop(columns=temp)

    huluCol = MediaFrame
    temp = huluCol[huluCol.columns.difference(['Date','Hulu '+mediaVar])].columns
    huluCol = huluCol.drop(columns=temp)

    otherCol = MediaFrame
    temp = otherCol[otherCol.columns.difference(['Date','Other TV '+mediaVar])].columns
    otherCol = otherCol.drop(columns=temp)
    
    if vodCol['VOD '+mediaVar].sum() > 0:
        outputFrame = outputFrame.merge(vodCol,on='Date', how='left')

    if ottCol['OTT '+mediaVar].sum() > 0:
        outputFrame = outputFrame.merge(ottCol,on='Date', how='left')

    if huluCol['Hulu '+mediaVar].sum() > 0:
        outputFrame = outputFrame.merge(huluCol,on='Date', how='left')

    if otherCol['Other TV '+mediaVar].sum() > 0:
        outputFrame = outputFrame.merge(otherCol,on='Date', how='left')

    tempKPI = KPIdataFrame[['Date',configs['dependentVariable']]]
    tempKPI = tempKPI.rename(columns={configs['dependentVariable']:'KPI Raw'})
    outputFrame = outputFrame.merge(tempKPI, on='Date', how='left')

    colnames = outputFrame.columns
    colnames = [i for i in colnames if 'Raw' in i]
    for i in colnames:
        curLog = outputFrame[i]
        newLog = []
        print(curLog)
        for c in curLog:
            if c != 0:
                newLog.append(math.log(c))
            else:
                newLog.append(0)
        outputFrame[i.replace('Raw','Log')] = newLog
    print(outputFrame)
    os.makedirs('output',exist_ok=True)
    outputFrame.to_csv('output/media_transform.csv')
    #for i in range(dates.shape[0]):
    #    daynum.append((datetime.strptime(dates[i], "%m/%d/%Y")).weekday())
    #print(daynum)

    #tapeLengthAgg = filtered_data.groupby(['Airing Date', 'Ad Length']).size().reset_index(name='Indvar')
    #tapeLengthAgg = tapeLengthAgg.groupby(['Airing Date', 'Ad Length']).sum().reset_index()
    #Make it a csv file and send it out
    os.makedirs('output',exist_ok=True)
    #outputDataFrame.to_csv('output/ad_length_'+configs['cut']+'_transform.csv')

def costs(Row):
    act_value = []
    for item in Row:
        if item == 0:
            act_value.append(0)
        else:
            act_value.append(item)
    answer = pandas.DataFrame(act_value)
    return answer

if __name__ == "__main__":
    media_transform()
