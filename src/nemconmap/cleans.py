#!/usr/bin/env python3

import geopandas as gpd
from nemosis import static_table


def clean_duid_w_geo(df_aemo, df_geo, raw_data_cache):

    df_geo = df_geo[df_geo['Region'].isin(['VIC1', 'NSW1', 'QLD1', 'SA1', 'TAS1'])]
    df_geo = df_geo.drop(columns = ['Region', 'Dispatch Type'])
    
    df_aemo = df_aemo[df_aemo['Classification'] != 'Non-Scheduled']

    # Join on DUID - this will miss some DUIDs, especially where there are multiple DUIDs for a single station
    df_matched = df_aemo.merge(df_geo, how = 'left', on = 'DUID')
    df_matched = df_matched.drop_duplicates(subset = ['DUID'], keep = 'last')


    # Some stations with multiple DUIDs only populate one DUID with geometry data
    # Copy the geometry data from the DUID which was populated
    # Only capture those with identical station names
    df_duid_missed = df_matched[df_matched['geometry'].isna()]
    for i_station in df_duid_missed['Station Name'].unique():
    
        df_tmp = df_matched[df_matched['Station Name'] == i_station]
        index_populated = df_tmp[df_tmp['Site Name'].notna()].index
    
        if not df_tmp.loc[index_populated].empty:
        
            if len(index_populated) > 1:
                #print('More than one index with Station Name ' + i_station + ' has a non-empty Site Name')
                index_populated = index_populated[0]
        
            for i_index in df_tmp.index.drop(index_populated):
                df_matched.loc[i_index] = df_tmp.loc[i_index].fillna(df_tmp.loc[index_populated].squeeze())


    # Manually copy geometry data where stations are different, but close enough
    l_unpop = ('BOWWPV1', 'BOWWBA1', 'SNOWSTH1', 'STUBSF1', 'SNOWYP', 'WLWLSF2', 'WARWSF2')
    l_pop = ('BOLIVPS1', 'BOLIVPS1', 'SNOWNTH1', 'STUBSF2', 'TUMUT3', 'WLWLSF1', 'WARWSF1')
    if len(l_pop) != len(l_unpop):
        print('Manual copy lists are of unequal length')

    for i in range(0, len(l_unpop)):

        i_pop = df_matched[df_matched['DUID'] == l_pop[i]].index.item()
        i_unpop = df_matched[df_matched['DUID'] == l_unpop[i]].index.item()

        df_matched.loc[i_unpop] = df_matched.loc[i_unpop].fillna(df_matched.loc[i_pop].squeeze())


    df_matched = df_matched.drop(columns = df_geo.columns.drop(['DUID', 'geometry']))
    df_matched = gpd.GeoDataFrame(df_matched)


    # Export matched CSV for manual checking
    df_matched.to_csv(raw_data_cache + 'Stations with Geo Data - Cleaned.csv', index = False)

    return(df_matched)
