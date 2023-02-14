import pandas as pd
import os
from tqdm import tqdm
import numpy as np

class cleanFile:
    """
    Containing all the methods and attributes related to cleaning the records.

    Attributes:
        file_root: the root of the files that need to be cleaned.
        column_alias: a check list that contains different aliases, e.g., Birth Date, Date of Birth.
        column_standard: the standard column list, e.g. Last Name, First Name, Date of Birth, etc.
            This will be used to create the dataFrame and also in the final excel file.
        column_checklist: a dict whose key is every aliase and the value is the standard column name
    """

    def __init__(self, file_root: str, entry_config: str) -> None:
        """
        Init the attributes of the class

        Args:
            entry_config: the file containing the standard entry name and all its aliases
            file_root: the file root
        """
        self.file_root = file_root


        self.column_standard, self.column_alias = self.resolveColumn(entry_config)
        self.column_checklist = self.unifyAlises()
        

        self.df_order = pd.DataFrame(columns=self.column_standard.values())
        # self.df_order.set_index(keys=[self.column_standard['tube-number']], inplace=True)
        self.df_info = pd.DataFrame(columns=self.column_standard.values())
        # self.df_info.set_index(keys=[self.column_standard['first-name'], self.column_standard['last-name']], inplace=True)


    def resolveColumn(self, entry_config: str):
        """
        Using the config file to set the standard and all the name list for columns
        """
        with open(entry_config) as f:
            lines = f.readlines()

            column_standard = {}
            column_aliases = {}
            for line in lines:
                entry = line.split(':')
                key = entry[0]
                vals = entry[1]
                vals = vals.split(',')
                vals = [*map(lambda x: x.strip(), vals)]

                column_standard[key] = vals[0]
                column_aliases[vals[0]] = vals

    
        return column_standard, column_aliases

    def unifyAlises(self):
        """
        Use the self.column_alias to create a str to str dict which is used to turn the aliases to standard
        column names.

        Args:
            self.column_alias

        Returns:
            self.column_dict: the string to string dict.
        """
        column_dict = {}

        for key, val in self.column_alias.items():
            if key not in val:
                val.append(key)

            for alias in val:
                column_dict[alias] = key

        return column_dict

    def checkColumn(self, df_keys: list[str]) -> dict[str, str]:
        """
        Check the column keys and return the modify dict, e.g., {'Date of Birth': 'Birth Date'} means
        we will change 'Date of Birth' to 'Birth Date'

        Args:
            df_keys: the column keys from te file we read.

        Returns:
            check_list: return the checklist dict as descibed in the description above
        """
        check_list = {}
        for key in df_keys:
            if key not in self.column_checklist.keys():
                continue
            check_list[key] = self.column_checklist[key]

        return check_list

    def __cleanDataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Make the dataframe standard-framed
        """
        check_list = self.checkColumn(df.keys())
        df.rename(columns=check_list, inplace=True)

        tube_entry = self.column_standard['tube-number'] # order id column
        first_name_entry = self.column_standard['first-name'] # first name column
        last_name_entry = self.column_standard['last-name'] # last name column

        # If this sheet does not contain any information about order id or name information,
        # this could not be a valid data sheet, and we can skip it
        if(tube_entry not in df.keys() and 
            first_name_entry not in df.keys() and last_name_entry not in df.keys()):
            return pd.DataFrame(columns=self.column_standard.values())

        # Drop all the columns that are not in the standard file columns
        column_diff = list(set(df.keys()).difference(set(self.column_standard.values())))
        df.drop(columns=column_diff, inplace=True)

        # make a template to fill the df_sheet, so that df_sheet becomes the 
        # same structure as the self.df
        template = pd.DataFrame(columns=self.column_standard.values()) 
        df = df.merge(template, how='left')

        # eliminate invalid records that don't have any information about names or order id
        df.dropna(how='all', subset=[first_name_entry, last_name_entry, tube_entry], inplace=True)

        return df

    def __aggregate(self):
        """
        Aggregate the two dataframes: self.data_order and self.data_info to self.df
        """
        first_name_entry = self.column_standard['first-name'] # first name column
        last_name_entry = self.column_standard['last-name'] # last name column
        dob_entry = self.column_standard['date-of-birth'] # date of birth column
        tube_entry = self.column_standard['tube-number'] # order id column

        # Aggregate the data of personal information and order id
        self.df_order.reset_index(inplace=True)
        self.df_order.set_index(keys=[first_name_entry, last_name_entry], inplace=True)
        self.df_info.set_index(keys=[first_name_entry, last_name_entry], inplace=True)
        self.df = self.df_order.combine_first(self.df_info)
        self.df.reset_index(inplace=True)

        self.df = self.df[self.column_standard.values()] # reset to standard column order
        self.df.dropna(subset=[tube_entry, first_name_entry, last_name_entry], how='any', inplace=True) # discard the data that still has no order id

        self.df = self.df.sort_values(by=[first_name_entry, last_name_entry]) # sort by names

    def processFile(self, file_name: str):
        """
        Process the file of any forms. This function will read all the 
        items by this way: if the item is totally new in the dataframe, which is containing totally new
        names or tube number

        Args:
            file_name: the excel file that needs to be processed

        Returns:
            Directly append the valid items to the class attribute self.df
        """
        df_file = pd.read_excel(file_name, sheet_name=None, dtype='string')

        for sheet in df_file.keys():
            df_sheet = df_file[sheet]
            tube_entry = self.column_standard['tube-number'] # order id column
            first_name_entry = self.column_standard['first-name'] # first name column
            last_name_entry = self.column_standard['last-name'] # last name column

            df_sheet = self.__cleanDataframe(df_sheet) # change to standard report form
            # clean names
            df_sheet[first_name_entry] = df_sheet[first_name_entry].fillna('') # fill nan first
            df_sheet[last_name_entry] = df_sheet[last_name_entry].fillna('') # fill nan first
            df_sheet[first_name_entry] = df_sheet[first_name_entry].map(lambda x: x.strip().capitalize(), na_action='ignore') # strip all the blank space
            df_sheet[last_name_entry] = df_sheet[last_name_entry].map(lambda x: x.strip().capitalize(), na_action='ignore') # strip all the blank space
            # print(df_sheet[first_name_entry].info())
            df_sheet[first_name_entry] = df_sheet[first_name_entry].replace('', None) # change to nan again
            df_sheet[last_name_entry] = df_sheet[last_name_entry].replace('', None) # change to nan again

            df_order = df_sheet.dropna(subset=[tube_entry]) # the dataframe that contrains order id
            df_info = df_sheet.loc[pd.isnull(df_sheet[tube_entry])] # the dataframe that contrains testant info

            # Add the dataframe with order id to the overall dataframe
            self.df_order.set_index(keys=[tube_entry], inplace=True)
            df_order.set_index(keys=[tube_entry], inplace=True)
            self.df_order = self.df_order.combine_first(df_order)
            self.df_order.reset_index(inplace=True)
            self.df_order.drop_duplicates(subset=[tube_entry], keep='first', inplace=True)

            # Find the names that already exist in the overall dataframe
            self.df_info.set_index(keys=[first_name_entry, last_name_entry], inplace=True)
            df_info.set_index(keys=[first_name_entry, last_name_entry], inplace=True)
            self.df_info = self.df_info.combine_first(df_info)
            self.df_info.reset_index(inplace=True)
            self.df_info.drop_duplicates(subset=[first_name_entry, last_name_entry], keep='first', inplace=True)
                    
            
    def walkthrough(self):
        """
        Walk through all the xlsx files in the file root
        """
        xlsx_files = []
        for (dirpath, dirnames, filenames) in os.walk(self.file_root):
            for filename in filenames:
                if(filename.endswith('.xlsx')):
                    filename = os.path.join(dirpath, filename)
                    # self.processFile(filename)
                    xlsx_files.append(filename)

        for xlsx_file in tqdm(xlsx_files):
            self.processFile(xlsx_file)

        
        self.__aggregate()
        
    
    @staticmethod
    def resolveNames(name_file):
        names = []
        with open(name_file) as f:
            lines = f.readlines()
            for name in lines:
                name = name.split()
                name = [*map(lambda x: x.strip(), name)]
                names.append(name)

        return names

    def searchByNames(self, name_file):
        """
        Considering that the form could have some dislocation, we may need to handle it manually
        """
        self.df_info = pd.DataFrame(columns=self.column_standard.values())
        self.df_order = pd.DataFrame(columns=self.column_standard.values())


        xlsx_files = []
        for (dirpath, dirnames, filenames) in os.walk(self.file_root):
            for filename in filenames:
                if(filename.endswith('.xlsx')):
                    filename = os.path.join(dirpath, filename)
                    # self.processFile(filename)
                    xlsx_files.append(filename)

        names = self.resolveNames(name_file)
        
        for xlsx_file in tqdm(xlsx_files):
            self.__searchByName(names, xlsx_file)

        self.__aggregate()
        
    def __searchByName(self, names, xlsx_file):
        """
        Search one file by first name and last name
        """
        df = pd.read_excel(xlsx_file, sheet_name=None, dtype="string")
        print(names)
        names = [*map(lambda x: x[0].capitalize() + ' ' + x[1].capitalize(), names)]
        
        for sheet in df.keys():
            df_sheet = df[sheet]

            tube_entry = self.column_standard['tube-number'] # order id column
            first_name_entry = self.column_standard['first-name'] # first name column
            last_name_entry = self.column_standard['last-name'] # last name column

            df_sheet = self.__cleanDataframe(df_sheet) # change to standard report form

            # clean names
            
            df_sheet[first_name_entry] = df_sheet[first_name_entry].fillna('') # fill nan first
            df_sheet[last_name_entry] = df_sheet[last_name_entry].fillna('') # fill nan first
            
            df_sheet[first_name_entry] = df_sheet[first_name_entry].map(lambda x: x.strip().capitalize(), na_action='ignore') # strip all the blank space
            df_sheet[last_name_entry] = df_sheet[last_name_entry].map(lambda x: x.strip().capitalize(), na_action='ignore') # strip all the blank space
            # print(df_sheet[first_name_entry].info())
            df_sheet[first_name_entry] = df_sheet[first_name_entry].replace('', None) # change to nan again
            df_sheet[last_name_entry] = df_sheet[last_name_entry].replace('', None) # change to nan again
            

            # keep two dataframes: 1. with 'full' information, which is order id and full name
            # 2. without order id but with a requested name
            
            df_order = df_sheet.dropna(subset=[tube_entry]) # the dataframe that contrains order id
            df_names = df_order[first_name_entry].fillna('') + ' ' + df_order[last_name_entry].fillna('')
            df_order = df_order.loc[df_names.isin(names)]

            df_info = df_sheet.loc[pd.isnull(df_sheet[tube_entry])] # the dataframe that contrains testant info
            df_names = df_info[first_name_entry].fillna('').apply(str) + ' ' + df_info[last_name_entry].fillna('').apply(str)
            df_info = df_info.loc[df_names.isin(names)]
            
            # Add the dataframe with order id to the overall dataframe
            self.df_order.set_index(keys=[tube_entry], inplace=True)
            df_order.set_index(keys=[tube_entry], inplace=True)
            self.df_order = self.df_order.combine_first(df_order)
            self.df_order.reset_index(inplace=True)
            self.df_order.drop_duplicates(subset=[tube_entry], keep='first', inplace=True)

            # Find the names that already exist in the overall dataframe
            self.df_info.set_index(keys=[first_name_entry, last_name_entry], inplace=True)
            df_info.set_index(keys=[first_name_entry, last_name_entry], inplace=True)
            self.df_info = self.df_info.combine_first(df_info)
            self.df_info.reset_index(inplace=True)
            self.df_info.drop_duplicates(subset=[first_name_entry, last_name_entry], keep='first', inplace=True)

            

            