#!/usr/bin/env python3

import pandas as pd
from nemosis import static_table, dynamic_data_compiler


def extract_cons_coef(start_date, end_date, raw_data_cache):

    ## Create a data frame with coefficients of all effective constraints
    df_con_region= dynamic_data_compiler(start_date, end_date, 'SPDREGIONCONSTRAINT', raw_data_cache)
    df_con_conn = dynamic_data_compiler(start_date, end_date, 'SPDCONNECTIONPOINTCONSTRAINT', raw_data_cache)
    df_con_ic = dynamic_data_compiler(start_date, end_date, 'SPDINTERCONNECTORCONSTRAINT', raw_data_cache)

    # Create an empty column in the interconnector dataframe so columns are consistent across the three dataframes
    df_con_ic.insert(len(df_con_ic.columns), 'BIDTYPE', 'None', allow_duplicates = False)

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
