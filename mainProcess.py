import pandas as pd
import fileProcess as fp

config = './config.txt'
file_root = './files'
cf = fp.cleanFile(file_root, config)

output_file = './report.xlsx'
cf.walkthrough()
cf.df.to_excel(output_file, index=False)
