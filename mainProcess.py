import fileProcess as fp

manual_code = input("Input the execution code:\n 1: clean all the files;\n 2: search by name files\n")
manual_code = manual_code.strip()

config = input('Please input the configure file path, default: ./config.txt')
config = config.strip()
if(len(config) == 0):
    config = './config.txt'
file_root = input('Please input your xlsx files\' root directory, default: ./files')
file_root = file_root.strip()
if(len(file_root) == 0):
    file_root = './files'


# output_file = './report.xlsx'
output_file = input('Please input your output file name, default: ./report.xlsx')
output_file = output_file.strip()
if(len(output_file) == 0):
    output_file = './report.xlsx'


cf = fp.cleanFile(file_root, config)


if manual_code == "1":
    cf.walkthrough()

elif manual_code == "2":
    name_file = input('Please input the file that contains name request, default: ./names.txt')
    name_file = name_file.strip()
    if(len(name_file) == 0):
        name_file = './names.txt'
    cf.searchByNames(name_file)

cf.df.to_excel(output_file, index=False)
