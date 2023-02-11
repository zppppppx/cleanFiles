# Logistics
Every time we read a `xlsx` file, we adopt following steps to modify the overall data store:
1. Change the name of the columns of the file read to standard representations (no trimming yet).
2. After standarlizing the column names, we check the `Order ID`, `First Name` and `Last Name` entries. If they don't exist, it means this file is not valid for obtaining valuable information.
3. After the check, we trim all the columns that are not required in the standard report file. And we fill the lacking columns to fit with the standard data form.
4. We separate the new data to two parts: a. that with `Order ID`: `df_order`, b. that without `Order ID` but with `First Name` and `Last Name`: `df_info`, and follow following steps:
   1. Aggregate the data with `Order ID`, and use new data to directly fill `N/A` items in the overall dataframe `self.df_order`, and eliminate all the duplicated entries;
   2. Aggregate the data without `Order ID`, and use new data to directly fill `N/A` items in the overall dataframe `self.df_info`, and eliminate all the duplicated entries.

After walking through all the files in the directory, we aggregate the two dataframes `self.df_order` and `self.df_info` by entries `Last Name` and `First Name`. 