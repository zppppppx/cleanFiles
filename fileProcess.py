import pandas as pd
import numpy as np
import os
from tqdm import tqdm

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

            # change the column names to standard column names
            check_list = self.checkColumn(df_sheet.keys())
            df_sheet.rename(columns=check_list, inplace=True)

            tube_entry = self.column_standard['tube-number'] # order id column
            first_name_entry = self.column_standard['first-name'] # first name column
            last_name_entry = self.column_standard['last-name'] # last name column

            # If this sheet does not contain any information about order id or name information,
            # this could not be a valid data sheet, and we can skip it
            if(tube_entry not in df_sheet.keys() and 
                first_name_entry not in df_sheet.keys() and last_name_entry not in df_sheet.keys()):
                continue

            # Drop all the columns that are not in the standard file columns
            column_diff = list(set(df_sheet.keys()).difference(set(self.column_standard.values())))
            df_sheet.drop(columns=column_diff, inplace=True)

            # make a template to fill the df_sheet, so that df_sheet becomes the 
            # same structure as the self.df
            template = pd.DataFrame(columns=self.column_standard.values()) 
            df_sheet = df_sheet.merge(template, how='left')
            # print(df_sheet.keys())

            # eliminate invalid records that don't have any information about names or order id
            df_sheet.dropna(how='all', subset=[first_name_entry, last_name_entry, tube_entry], inplace=True)

            df_order = df_sheet.dropna(subset=[tube_entry]) # the dataframe that contrains order id
            df_info = df_sheet.loc[pd.isnull(df_sheet[tube_entry])] # the dataframe that contrains testant info

            # Add the dataframe with order id to the overall dataframe
            self.df_order.set_index(keys=[tube_entry], inplace=True)
            df_order.set_index(keys=[tube_entry], inplace=True)
            self.df_order = self.df_order.combine_first(df_order)
            self.df_order.reset_index(inplace=True)
            self.df_order.drop_duplicates(subset=[tube_entry], keep='first', inplace=True)

            
            # self.df.set_index(keys=[tube_entry], inplace=True)
            # df_order_id.set_index(keys=[tube_entry], inplace=True)
            # self.df = self.df.combine_first(df_order_id)
            # self.df.reset_index(inplace=True)
            # print(self.df)


            # Find the names that already exist in the overall dataframe
            self.df_info.set_index(keys=[first_name_entry, last_name_entry], inplace=True)
            df_info.set_index(keys=[first_name_entry, last_name_entry], inplace=True)
            self.df_info = self.df_info.combine_first(df_info)
            self.df_info.reset_index(inplace=True)
            self.df_info.drop_duplicates(subset=[first_name_entry, last_name_entry], keep='first', inplace=True)
            # df_info = pd.concat([df_info, self.df])
            # df_info.drop_duplicates(subset=[first_name_entry, last_name_entry], keep='first', inplace=True)

            # Use the new information to fill the overall dataframe

            # print("The memroy has been used with {} for info and {} for order"
            #       .format(self.df_info.info(verbose=False, memory_usage='deep'), self.df_order.info(verbose=False, memory_usage='deep')))


            # self.df.set_index(keys=[first_name_entry, last_name_entry], inplace=True)
            # df_info.set_index(keys=[first_name_entry, last_name_entry], inplace=True)
            # print(df_info.info())
            # print(self.df.info())
            # self.df = self.df.combine_first(df_info)
            # self.df.reset_index(inplace=True)
                    
            
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

        # Clean the forms of the data
        self.df[first_name_entry] = self.df[first_name_entry].map(lambda x: x.strip()) # strip all the blank space
        self.df[last_name_entry] = self.df[last_name_entry].map(lambda x: x.strip()) # strip all the blank space

        # self.df[dob_entry] = pd.to_datetime(self.df[dob_entry], errors='coerce') # standardlize the date time representation
        # self.df[dob_entry] = pd.PeriodIndex(self.df[dob_entry], freq='D')
        # self.df[dob_entry] = self.df[dob_entry]

        self.df = self.df.sort_values(by=[first_name_entry, last_name_entry]) # sort by names
    
            