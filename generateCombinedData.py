import os
import pandas as pd
import numpy as np

filepath = r'Q:\Projects\224008\DESIGN\ANALYSIS\00_PV\Power Demand Graph\Excel Files'


def get_files(folder_filepath):
    """
    Takes in filepath to folder of files to be processed

    RETURNS: list of csv files to process, list of section titles
    """
    file_list = os.listdir(folder_filepath)
    section_list = [file.split(" ")[2].split('_')[0] for file in file_list]
    return file_list, section_list


def preprocess_dataframe(dataframe):
    """
    Cleans dataframe by coercing non-numeric values to nan and then dropping rows with nan
    :param dataframe: dataframe to be processed
    :return: processed dataframe with only numeric values
    """
    df_columns = ['EArray', 'EOutInv', 'PR']
    for column in df_columns:
        dataframe[column] = pd.to_numeric(dataframe[column], errors='coerce')
    dataframe = dataframe.dropna()
    return dataframe


def calc_sums(list_of_df, position):
    """
    Calculates sums and averages of data on the given row (position)
    :param list_of_df: list of dataframes to perform calculations on
    :param position: integer of which row to calculate
    :return: list of rounded sums and averages to be used as a row in the dataframe to export
    """
    sum_earray = 0
    sum_eoutinv = 0
    sum_pr = 0

    for df in list_of_df:
        sum_earray += df.loc[position, 'EArray']
        sum_eoutinv += df.loc[position, 'EOutInv']
        sum_pr += df.loc[position, 'PR']

    avg_pr = sum_pr / len(list_of_df)
    row_sum = [sum_earray, sum_eoutinv, avg_pr]
    row_sum = [round(num, 4) for num in row_sum]

    return row_sum


# get list of filepaths to process
files, sections = get_files(filepath)
files = [filepath + '\\' + f for f in files]

# collect list of dataframes and clean them
list_of_dataframes = [pd.read_csv(f, skiprows=10) for f in files]
list_of_dataframes = [preprocess_dataframe(df) for df in list_of_dataframes]

# copy first dataframe to use as template for one to export
export_dataframe = list_of_dataframes[0].copy()

# calculate sums and averages for each row and populate database to export with them
for index, row in export_dataframe.iterrows():
    new_row_vals = calc_sums(list_of_dataframes, index)
    export_dataframe.loc[index, 'EArray'] = new_row_vals[0]
    export_dataframe.loc[index, 'EOutInv'] = new_row_vals[1]
    export_dataframe.loc[index, 'PR'] = new_row_vals[2]

export_dataframe.rename(columns={'EArray': 'EArray Sum', 'EOutInv': 'EOutInv Sum', 'PR': 'PR Avg'}, inplace=True)

# export the combined data
output_filepath = r'Q:\Projects\224008\DESIGN\ANALYSIS\00_PV\Power Demand Graph' + '\\' + 'CombinedPowerData.csv'
try:
    export_dataframe.to_csv(output_filepath, index=False)
    print("\nFile output to: \n" + output_filepath)
except PermissionError:
    print("\nCannot write to file, try closing " + output_filepath + " and rerunning")
