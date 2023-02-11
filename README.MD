# Logistics
Every time we read a `xlsx` file, we adopt following steps to modify the overall data store:
1. Change the name of the columns of the file read to standard representations (no trimming yet).
2. After standarlizing the column names, we check the `Order ID`, `First Name` and `Last Name` entries. If they don't exist, it means this file is not valid for obtaining valuable information.
3. After the check, we trim all the columns that are not required in the standard report file. And we fill the lacking columns to fit with the standard data form.
4. We separate the new data to two parts: a. that with `Order ID`: `df_order`, b. that without `Order ID` but with `First Name` and `Last Name`: `df_info`, and follow following steps:
   1. Aggregate the data with `Order ID`, and use new data to directly fill `N/A` items in the overall dataframe `self.df_order`, and eliminate all the duplicated entries;
   2. Aggregate the data without `Order ID`, and use new data to directly fill `N/A` items in the overall dataframe `self.df_info`, and eliminate all the duplicated entries.

After walking through all the files in the directory, we aggregate the two dataframes `self.df_order` and `self.df_info` by entries `Last Name` and `First Name`. 


# Usage
Now, this is just a **very naive** demo.

The file tree should be like:

      |-- root
         |-- fileProcess.py
         |-- mainPorcess.py
         |-- files
            |-- *.xlsx
            |-- subdir
               |-- *.xlsx

Some key params in `mainProcess.py`:

`config`: the file path of `config.txt`, this file contains all the names of entries, each line stands for one column names. In each line, the first word is a marker used by the program, please don't change it. The following words are the standard name and aliases of the column. The first word should and must be the standard name. Each name is separated by `,`. The more aliases provided, the more accurate and cleaner it is to sweep over the files.

`file_root`: the root of all the `xlsx` files.

`output_file`: the output path for the file.


> **PAY ATTENTION:** 
> 
> 1. due to the limitation of the `pandas`, which leads to obvious slowing down when reading extremly large files, please separate the files into smaller pieces as much as possible.
> 2. After reading in a file, `pandas` would consume more storage than the size of the file, one estimation is that the size consumed by `pandas` is 6x times greater than the file's original file, e.g. 10 MB -> 70 MB in RAM.
> 3. Notice that this demo does not have any functions to correct the content of the file. If there is a mistake in the file, e.g., dislocation of the contents (phone number in the DOB), this could be a disaster.