#the basics...
import numpy as np
import pandas as pd

#used for moving files around and finding folders
import os as os

import warnings

#used to output pretty progress bars
from tqdm import tqdm

import gc


#if you have downloaded the ICPSR crosswalk file, which is used to associate state, county, and place fips codes, specify the path here;
#   the ICPSR crosswalk file can be found at: https://www.icpsr.umich.edu/web/ICPSR/studies/35158#
crosswalk_path = None

#suppress warning from pandas
from warnings import simplefilter 
simplefilter(action="ignore", category=pd.errors.PerformanceWarning)

#ensuring certain directories are present;
newpath1 = r'RETA_data'
newpath2 = r'output' 
if not os.path.exists(newpath1):
    os.makedirs(newpath1)

if not os.path.exists(newpath2):
    os.makedirs(newpath2)

if(crosswalk_path is None):
    warnings.warn("ICPSR Crosswalk not found; No STATE, COUNTY, or PLACE fips codes will be attached.")
    print("ICPSR Crosswalk file can be found at https://www.icpsr.umich.edu/web/ICPSR/studies/35158.")
    print("Once downloaded, replace \"None\"  at the top of the .py file with the path to the 35158-0001-Data.tsv file.")


def read_RETA_file(filepath, keyfile = 'RETURN-A_Keyfile.xlsx', crosswalk_path = None):
    
    #first, read the Key File
    if(not os.path.exists(keyfile)):
        print("keyfile not found: %s"%keyfile)
        raise Exception("Keyfile Not Found Error")


    key_file = pd.read_excel(keyfile, sheet_name = "New Variables")
    
    #getting a shortened 3-letter month string
    key_file['month_short'] = key_file['month'].str.lower().str[:3]

    #combining each column name with its month
    key_file['name'] = key_file['month_short'] + '||' + key_file['new_names_for_real_this_time']
    
    
    #reading the RETA file as a list of each line in the file;
    with open(filepath, 'r', encoding='latin') as file:
        orig_txt = file.readlines()
    
    #doing a tiny bit of cleanup;
    #  the \n was retained at the end of each line, so i am removing that
    for i in range(len(orig_txt)):
        orig_txt[i] = orig_txt[i].replace('\n','')

    #preparing the dataframe to parse the RETURN-A file into
    wide_df = pd.DataFrame()

    #creating a string column, where each row contains the entire line from the RETURN-A file.
    wide_df['origtxt'] = orig_txt
    
    #iterate through the different columns from the keyfile.
    curr_pos = 0 #tracking the position along the lines.
    for i in tqdm(range(len(key_file)), 'Collecting Column Information'):

        #identify the datatype;
        dtype_ind = key_file['Type_Length'].iloc[i][0] #"N is numeric, A is character"

        #identify how long this entry is.
        entry_length = int(key_file['Type_Length'].iloc[i][1:])

        #getting the df;
        #  this uses the pandas string accessor;
        #  makes a new column that is a subset of the 'origtxt' column
        #  names it based on the current column name
        wide_df[key_file['name'].iloc[i]] = wide_df['origtxt'].str[curr_pos:curr_pos + entry_length]

        #converts the column to numeric if they are marked as such
        if(dtype_ind == 'N'):
            wide_df[key_file['name'].iloc[i]] = pd.to_numeric(wide_df[key_file['name'].iloc[i]], errors = 'coerce')

        #updating the current position.
        curr_pos += entry_length
    
    
    #dropping the original text column;
    wide_df.drop(columns = ['origtxt'], inplace = True)
    
    #trying to aggregate this stuff;
    
    #first, get substrings corresponding to the relevant columns
    #   gets the last 31 columns in the list
    #   this corresponds to the columns for december.
    name_bits = pd.Series(key_file['name'][-31:]).copy()

    #removing the 'dec||' prefix from the column names that have it, and adding '_2' to these
    #   _2 corresponds to the actual offenses.
    name_bits.iloc[:-3] = name_bits.iloc[:-3].str[5:-2] + '_2'

    #removing the 'dec||' from the column names that have it
    name_bits.iloc[-3:] = name_bits.iloc[-3:].str[5:]

    #making a second array of names that correspond to the other columns that we want to drop.
    drop_bits = pd.concat([name_bits.copy().iloc[:-3].str[:-1] + '1',
                           name_bits.copy().iloc[:-3].str[:-1] + '3',
                           name_bits.copy().iloc[:-3].str[:-1] + '4'])
    
    #converting these from pandas Series to arrays of strings.
    name_bits = np.array(name_bits)
    drop_bits = np.array(drop_bits)
    
    #for each;
    #  find what rows contain that substring
    #  add all of those rows together into a new column
    #  drop those old columns from the overall dataframe.
    for i in tqdm(range(len(name_bits)), 'Aggregating Column Information'):

        #find what rows of the dataframe contain this 'name_bit' (something like murder_2)
        cols_mask = np.zeros(len(wide_df.columns), dtype = bool)
        for j in range(len(cols_mask)):
            cols_mask[j] = name_bits[i] in wide_df.columns[j]

        #making a dataframe that only contains the relevant columns
        cols_of_interest = wide_df.columns[cols_mask]

        #converting each of those columns to numeric;
        for j in range(len(cols_of_interest)):
            wide_df[cols_of_interest[j]] = pd.to_numeric(wide_df[cols_of_interest[j]], errors='coerce')

        #now, we want to add all those columns together
        wide_df[name_bits[i]] = wide_df[cols_of_interest].sum(axis = 1, skipna=True)

        #dropping these aggregated columns from the dataframe.
        wide_df.drop(columns = cols_of_interest, inplace = True)

        
    #dropping extra columns that correspond to other kinds of counts (unfounded arrests, cleared by arrests, arrests under 18)
    for i in range(len(drop_bits)):
        
        #find what rows of the dataframe contain this 'drop_bit' (something like 'murder_3', 'robbery_1')
        cols_mask = np.zeros(len(wide_df.columns), dtype = bool)
        for j in range(len(cols_mask)):
            cols_mask[j] = drop_bits[i] in wide_df.columns[j]

        #dropping these identified columns.
        wide_df.drop(columns = wide_df.columns[cols_mask], inplace = True)
        
    
    #dropping a set of columns that are otherwise of no use to us.
    #   they represent information about the columns we just dropped.
    
    #identify these columns.
    cols_mask = np.zeros(len(wide_df.columns), dtype = bool)
    for j in range(len(cols_mask)):
        if('card0' in wide_df.columns[j]):
            cols_mask[j] = True

        if('card2' in wide_df.columns[j]):
            cols_mask[j] = True

        if('card3' in wide_df.columns[j]):
            cols_mask[j] = True

    #dropping those columns.
    wide_df.drop(columns = wide_df.columns[cols_mask], inplace = True)
    
    #creating a new months_reported column based on the card1_type column for each month;
    
    #identifying the columns
    cols_mask = np.zeros(len(wide_df.columns), dtype = bool)
    for j in range(len(cols_mask)):
        if('card1_type' in wide_df.columns[j]):
            cols_mask[j] = True

    #paring to just these columns
    card1_cols = wide_df.columns[cols_mask]

    #preparing the new_months_reported column
    wide_df['manual_months_reported'] = np.zeros(len(wide_df))
    
    #for each column (month)
    for i in range(len(card1_cols)):
        
        #get a column of the numbers as integers.
        temp = pd.to_numeric(wide_df[card1_cols[i]], errors = 'coerce')

        #get a column of true/false, representing if that month has data
        col_bool = (temp == 5) | (temp == 2)

        #convert it to an array
        col_bool = np.array(col_bool)
        
        #this is the tricky part;
        #   when you convert a boolean array to an integer, it becomes either 0 (false) or 1 (true)
        #   here, I am adding that column to the new_months_reported; true = +1, false = +0.
        wide_df['manual_months_reported'] = np.array(wide_df['manual_months_reported']) + col_bool.astype(int)
    

    if(crosswalk_path != None):

        #reading the crosswalk file.
        crosswalk = pd.read_csv(crosswalk_path, sep = '	', dtype = 'string')

        #paring to what we want;
        crosswalk = crosswalk.loc[crosswalk.ORI7 != '-1',['ORI7', 'FIPS_ST','FIPS_COUNTY', 'FPLACE']]

        #renaming in preparation to merge;
        crosswalk.rename(columns = {'ORI7' : 'hea||ori',
                                    'FIPS_ST' : 'STATEFP',
                                    'FIPS_COUNTY' : 'COUNTYFP',
                                    'FPLACE' : 'PLACEFP'}, inplace = True)
        #mergin;
        wide_df = wide_df.merge(crosswalk, on='hea||ori', how='left')

    return(wide_df)



# =============================
#   Main Function Begins
# =============================

#Listing the identified RETURN-A files.
orig_RETA_files = os.listdir('RETA_data')
orig_RETA_files.sort()

print("I identified %s RETA_data files."%len(orig_RETA_files))

if(len(orig_RETA_files) == 0):
    print("No files found in RETA_data folder.")
    raise Exception("RETURNA File Not Found Error")


#for each identified file;
for i in range(len(orig_RETA_files)):
    
    #get this specific filename
    RETA_file = orig_RETA_files[i]
    
    #get the year from the filename;
    try:
        year = int(RETA_file[:4])
    except:
        print("Please rename each RETURNA file to have the year on the front (ex: RETA05.DAT --> 2005_RETA.DAT)")
        raise Exception("RETURNA File Naming Error")

    print('==================  %s  ======================='%year)
    print('filename: %s'%RETA_file)
    
    #using the function
    output_df = read_RETA_file('RETA_data/' + RETA_file, crosswalk_path=crosswalk_path)
    
    #setting the year to the actual, rather than two digits
    output_df['hea||year'] = year
    
    #saving;
    print('Writing result to output/RETA' + str(year) + '.csv')
    output_df.to_csv('output/' + 'RETA' + str(year) + '.csv', index = False)