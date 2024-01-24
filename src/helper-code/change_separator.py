import csv
import sys
import os

#It is assumed that the structure will look something like this
#├── new_folder
#│   ├── hint_and_eplain
#│   ├── query1
#│   ├── query2
#│   ├── query3
#│   └── query4
#└── old_folder
#    ├── hint_and_eplain
#    ├── query1
#    ├── query2
#    ├── query3
#    └── query4
results_dir = '/path/to/original/results/folder'
results_comma_directory = '/path/to/updated/results/folder'
old_folder = '/old_folder_name'
new_folder = '/new_folder_name'

folders_list = [
	'query1',
	'query2',
	'query3',
	'query4'
]


for folder in folders_list:
	for subdir, dirs, files in os.walk(results_dir+folder):
		for file in files:
			if file.endswith('.csv'):
				input_file_path = os.path.join(subdir, file)
				output_file_path = os.path.join(subdir.replace(old_folder, new_folder), file)
				#print("output_file_path is:" ,output_file_path)
				reader = csv.reader(open(input_file_path, newline = None), delimiter='|')
				writer = csv.writer(open(output_file_path,  'w'), delimiter=',')
				writer.writerows(reader)
				print("Delimiter successfully changed")
