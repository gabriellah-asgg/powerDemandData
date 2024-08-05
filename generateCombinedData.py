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


def process_file(csv_filepath, sect, dataframe):
    """
    Takes in filepath to given csv to process, the corresponding section title, and the dataframe to concat the data
    to. Calculates sums and averages of specific column titles and adds them to the input dataframe with the
    corresponding section title.

    RETURNS: Dataframe with sums and averages of given section added.
    """
    df = pd.read_csv(csv_filepath, skiprows=10)

    # ensure inputs are numeric
    df = df[['EArray', 'EOutInv', 'PR']]
    for column in df.columns:
        df[column] = pd.to_numeric(df[column], errors='coerce')
    df.fillna(0, inplace=True)

    # calc sums
    earray_sum = np.nansum(df['EArray'])
    eoutinv_sum = np.nansum(df['EOutInv'])

    # calc avg
    pr_avg = round(np.nansum(df['PR']) / len(df['PR']), 4)

    result_df = pd.DataFrame(
        {'Section': [sect], 'EArray Sum': [earray_sum], 'EOutInv Sum': [eoutinv_sum], 'PR Average': [pr_avg]})

    dataframe = pd.concat([dataframe, result_df])

    return dataframe


files, sections = get_files(filepath)

# initialize dataframe to export
column_names = ['Section', 'EArray Sum', 'EOutInv Sum', 'PR Average']
export_dataframe = pd.DataFrame(columns=column_names)

# process data in each file
for index, f in enumerate(files):
    export_dataframe = process_file(filepath + '\\' + f, sections[index], export_dataframe)

# export the combined data
output_filepath = r'Q:\Projects\224008\DESIGN\ANALYSIS\00_PV\Power Demand Graph' + '\\' + 'CombinedPowerData.csv'
try:
    export_dataframe.to_csv(output_filepath)
    print("\nFile output to: \n" + output_filepath)
except PermissionError:
    print("\nCannot write to file, try closing " + output_filepath + " and rerunning")
