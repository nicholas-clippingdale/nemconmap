#!/usr/bin/env python3

#TODO: Also breakdown the RHS - looks more complex, it's not as simple as adding DUIDs, even ez2view doesn't do this - the table is GENERICCONSTRAINTRHS, file is PUBLIC_ARCHIVE#GENERICCONSTRAINTRHS#FILE01#YYYYMMDD0000.zip
#TODO: Import pre-dispach generator/IC/region data too, to infer about future behaviour
#TODO: Add FCAS constraints
#TODO: Have data in a PostgreSQL db


def extract_cons_info(start_date, end_date, raw_data_cache):

    from nemosis import dynamic_data_compiler
    import pandas as pd


    ## Create a data frame with details of all active constraints
    df_cons_info = dynamic_data_compiler(start_date, end_date, 'GENCONDATA', raw_data_cache)

    df_cons_info = df_cons_info.sort_values(by = ['GENCONID', 'VERSIONNO', 'LASTCHANGED']).drop_duplicates(subset = ['GENCONID'], keep = 'last')


    return(df_cons_info)


def extract_cons_coef(start_date, end_date, raw_data_cache):

    from nemosis import dynamic_data_compiler
    import pandas as pd


    ## Create a data frame with coefficients of all effective constraints
    df_cons_region= dynamic_data_compiler(start_date, end_date, 'SPDREGIONCONSTRAINT', raw_data_cache, filter_cols = ['BIDTYPE'], filter_values = (['ENERGY'],))
    df_cons_conn = dynamic_data_compiler(start_date, end_date, 'SPDCONNECTIONPOINTCONSTRAINT', raw_data_cache, filter_cols = ['BIDTYPE'], filter_values = (['ENERGY'],))
    df_cons_ic = dynamic_data_compiler(start_date, end_date, 'SPDINTERCONNECTORCONSTRAINT', raw_data_cache)

    df_cons_ic.insert(6, 'BIDTYPE', None, allow_duplicates = False)

    df_cons_region.insert(0, 'TERMTYPE', 'REGION', allow_duplicates = False)
    df_cons_conn.insert(0, 'TERMTYPE', 'CONNECTIONPOINT', allow_duplicates = False)
    df_cons_ic.insert(0, 'TERMTYPE', 'INTERCONNECTOR', allow_duplicates = False)

    df_cons_region.rename(columns = {'REGIONID':'OBJECTID'}, inplace = True)
    df_cons_conn.rename(columns = {'CONNECTIONPOINTID':'OBJECTID'}, inplace = True)
    df_cons_ic.rename(columns = {'INTERCONNECTORID':'OBJECTID'}, inplace = True)

    df_cons_coef = pd.concat([df_cons_region, df_cons_ic, df_cons_conn])


    return(df_cons_coef)


def extract_duid_info(nem_raw_data_cache, geo_raw_data_cache, station_mapping_path):

    from nemosis import static_table
    import pandas as pd
    import geopandas as gp


    df_geo_stations = gp.read_file('Major_Power_Stations_v4.shp')
    
    df_geo_stations = df_geo_stations[~df_geo_stations['STATE'].isin(['Western Australia', 'Northern Territory'])]
    df_geo_stations = df_geo_stations[df_geo_stations['OPERATIONA'] == 'Operational']
    

    df_aemo_duids = static_table('Generators and Scheduled Loads', raw_data_cache)
    
    df_aemo_duids = df_aemo_duids.replace(['Scheduled*', 'Non-Scheduled*', 'Non-Scheduled**'], ['Scheduled', 'Semi-Scheduled', 'Scheduled'])
    df_aemo_duids = df_aemo_duids[df_aemo_duids['Classification'].isin(['Scheduled', 'Semi-Scheduled'])]

    
    df_station_mapping = pd.read_csv(station_mapping_path)

    df_aemo_duids = df_aemo_duids.merge(df_station_mapping, how = 'left', on = ['Station Name', 'DUID'])


    df_duids = df_geo_stations.merge(df_aemo_duids, how = 'right', on = 'NAME')


    return(df_duids)
