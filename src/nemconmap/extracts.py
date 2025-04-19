#!/usr/bin/env python3

#TODO: Also breakdown the RHS - looks more complex, it's not as simple as adding DUIDs, even ez2view doesn't do this - the table is GENERICCONSTRAINTRHS, file is PUBLIC_ARCHIVE#GENERICCONSTRAINTRHS#FILE01#YYYYMMDD0000.zip
#TODO: Import pre-dispach generator/IC/region data too, to infer about future behaviour
#TODO: Add FCAS constraints

import pandas
from nemosis import dynamic_data_compiler


def extract_cons_info(start_date, end_date, raw_data_cache):

    ## Create a data frame with details of all active constraints
    df_con_info = nemosis.dynamic_data_compiler(start_date, end_date, 'GENCONDATA', raw_data_cache)

    # Only keep the latest version of each constraint
    df_con_info = df_con_info.sort_values(by = ['GENCONID', 'VERSIONNO', 'LASTCHANGED']).drop_duplicates(subset = ['GENCONID'], keep = 'last')


    return(df_con_info)


def extract_cons_coef(start_date, end_date, raw_data_cache):

    ## Create a data frame with coefficients of all effective constraints
    df_con_region= dynamic_data_compiler(start_date, end_date, 'SPDREGIONCONSTRAINT', raw_data_cache, filter_cols = ['BIDTYPE'], filter_values = (['ENERGY'],))
    df_con_conn = dynamic_data_compiler(start_date, end_date, 'SPDCONNECTIONPOINTCONSTRAINT', raw_data_cache, filter_cols = ['BIDTYPE'], filter_values = (['ENERGY'],))
    df_con_ic = dynamic_data_compiler(start_date, end_date, 'SPDINTERCONNECTORCONSTRAINT', raw_data_cache)

    # Create an empty column in the interconnector dataframe so columns are consistent across the three dataframes
    df_con_ic.insert(6, 'BIDTYPE', None, allow_duplicates = False)

    # Create a column in each dataframe specifying the type of object its describing
    df_con_region.insert(0, 'TERMTYPE', 'REGION', allow_duplicates = False)
    df_con_conn.insert(0, 'TERMTYPE', 'CONNECTIONPOINT', allow_duplicates = False)
    df_con_ic.insert(0, 'TERMTYPE', 'INTERCONNECTOR', allow_duplicates = False)

    df_con_region.rename(columns = {'REGIONID':'OBJECTID'}, inplace = True)
    df_con_conn.rename(columns = {'CONNECTIONPOINTID':'OBJECTID'}, inplace = True)
    df_con_ic.rename(columns = {'INTERCONNECTORID':'OBJECTID'}, inplace = True)

    # Concatenate all three dataframes into one
    df_con_coef = pd.concat([df_con_region, df_con_ic, df_con_conn])


    return(df_con_coef)
