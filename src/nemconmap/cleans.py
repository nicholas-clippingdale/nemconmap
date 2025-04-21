#!/usr/bin/env python3

import pandas as pd
import geopandas as gpd
from requests import Request
from owslib.wfs import WebFeatureService
from nemosis import static_table
from fuzzywuzzy import process


def clean_stations(raw_data_cache):
    
    # Extract geo data from WFS server
    url = 'https://services.ga.gov.au/gis/services/National_Electricity_Infrastructure/MapServer/WFSServer'
    params = dict(service = 'WFS', version = "2.0.0", request = 'GetFeature', typeName = 'National_Electricity_Infrastructure:Major_Power_Stations')
    wfs_request_url = Request('GET', url, params = params).prepare().url
    df_geoservice = gpd.read_file(wfs_request_url)
    
    df_geoservice = df_geoservice.loc[~df_geoservice['STATE'].isin(['Western Australia', 'Northern Territory'])]

    
    # Extract AEMO station data through nemosis
    df_aemo = static_table('Generators and Scheduled Loads', raw_data_cache)


    # Find exact matches
    df_matched = df_aemo.merge(df_geoservice, how = 'inner', left_on = 'Station Name', right_on = 'POWERSTATION_NAME')
    # Remove matched stations from source tables to avoid matching again
    df_aemo_tmp = df_aemo.loc[~df_aemo['Station Name'].isin(df_matched['Station Name'])]
    df_geoservice_tmp = df_geoservice.loc[~df_geoservice['POWERSTATION_NAME'].isin(df_matched['POWERSTATION_NAME'])]


    # Remove the string 'Power Station' then find exact matches
    df_aemo_tmp.insert(0, 'Station Name - No Power Station', df_aemo_tmp['Station Name'].str.replace(' Power Station', ''))
    df_geoservice_tmp.insert(0, 'POWERSTATION_NAME - No Power Station', df_geoservice_tmp['POWERSTATION_NAME'].str.replace(' Power Station', ''))

    df_tmp = df_aemo_tmp.merge(df_geoservice_tmp, how = 'inner', left_on = 'Station Name - No Power Station', right_on = 'POWERSTATION_NAME - No Power Station')
    df_tmp = df_tmp.drop(columns = ['Station Name - No Power Station', 'POWERSTATION_NAME - No Power Station'])

    df_matched = pd.concat([df_matched, df_tmp])

    df_aemo_tmp = df_aemo_tmp.loc[~df_aemo_tmp['Station Name'].isin(df_matched['Station Name'])]
    df_geoservice_tmp = df_geoservice_tmp.loc[~df_geoservice_tmp['POWERSTATION_NAME'].isin(df_matched['POWERSTATION_NAME'])]


    # Find the best fuzzywuzzy matches
    # Use the name without the words Power Station to reduce the number of false positives
    choices = list(df_geoservice_tmp['POWERSTATION_NAME - No Power Station'])

    for i in df_aemo_tmp['DUID']:

        query = df_aemo_tmp[df_aemo_tmp['DUID'] == i]['Station Name - No Power Station'].item()
        best_match = process.extractOne(query, choices)

        duid_skip_man = ['CALL_B_1', 'CALL_B_2', '-', 'TIB1', 'TORRB1', 'TORRB2', 'TORRB3', 'TORRB4', 'GB01', 'HBESSG1', 'HBESSL1', 'HBESS1', 'KABANWF1', 'UPPTUMUT', 'WDBESS1', 'BHB1']
    
        if best_match[1] >= 90:
        
            if i not in duid_skip_man:
            
                #print(i, " - ", best_match)
            
                df_aemo_tmp_row = df_aemo_tmp[df_aemo_tmp['DUID'] == i]
                df_geoservice_tmp_row = df_geoservice_tmp[df_geoservice_tmp['POWERSTATION_NAME - No Power Station'] == best_match[0]]
            
                df_aemo_tmp_row.insert(0, 'TEMP_JOIN', 'JOINVALUE', allow_duplicates = True)
                df_geoservice_tmp_row.insert(0, 'TEMP_JOIN', 'JOINVALUE', allow_duplicates = True)
                df_tmp = df_aemo_tmp_row.merge(df_geoservice_tmp_row, how = 'inner', on = 'TEMP_JOIN')
                df_tmp = df_tmp.drop(columns = ['Station Name - No Power Station', 'POWERSTATION_NAME - No Power Station', 'TEMP_JOIN'])
            
                df_matched = pd.concat([df_matched, df_tmp])
        
            #else:
                #print('Skipping ', i, ' due to manual adjustment')

    df_aemo_tmp = df_aemo_tmp.loc[~df_aemo_tmp['Station Name'].isin(df_matched['Station Name'])]
    df_geoservice_tmp = df_geoservice_tmp.loc[~df_geoservice_tmp['POWERSTATION_NAME'].isin(df_matched['POWERSTATION_NAME'])]


    # Remove all geoservice columns except the geo data
    df_matched = df_matched.drop(columns = df_geoservice.columns.drop(['gml_id', 'LOCALITY', 'X_COORDINATE', 'Y_COORDINATE', 'geometry']))


    df_matched.to_csv(raw_data_cache + 'Stations with Geo Data.csv', index = False)

    return(df_matched)
