import csv
import sys
import os

input_dir = '/Users/dimitris.mitr/Desktop/Σχολή/11ο εξάμηνο/advanced DB/updated_results/'
output_dir = '/Users/dimitris.mitr/Desktop/Σχολή/11ο εξάμηνο/advanced DB/updated_results_comma/'

folders_list = [
	'query1',
	'query2',
	'query3',
	'query4'
]


for folder in folders_list:
	for subdir, dirs, files in os.walk(input_dir+folder):
		for file in files:
			if file.endswith('.csv'):
				input_file_path = os.path.join(subdir, file)
				output_file_path = os.path.join(subdir.replace('/updated_results', '/updated_results_comma'), file)
				#print("output_file_path is:" ,output_file_path)
				reader = csv.reader(open(input_file_path, newline = None), delimiter='|')
				writer = csv.writer(open(output_file_path,  'w', encoding="utf-16"), delimiter=',')
				writer.writerows(reader)
				print("Delimiter successfully changed")
