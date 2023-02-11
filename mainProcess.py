import pandas as pd
import fileProcess as fp

config = './config.txt'
cf = fp.cleanFile('./files', config)

file_name = './templates/test.xlsx'
df = pd.read_excel(file_name)
# print(df.info())

# cf.processFile(file_name)
# cf.df_info.to_excel('./templates/info_test.xlsx')
# cf.df_order.to_excel('./templates/order_test.xlsx')

cf.walkthrough()
cf.df.to_excel('./report.xlsx', index=False)
