import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd

#declare the path where the result files are located
folder_path = '/home/user/opt/results/'
file_list = os.listdir(folder_path)


def process_query_1_2 (file_name):
	'''
	This is a function that creates a dataframe, used for the first two queries. Then it calculates
	the average for each of the first three columns and returns the dataframe. 

	Note: the fourth column is the number of executors, which is constant for the first
	two queries so for the plots we do not take it into account 

	:file_name = a list of the file names with the time measurements for each query

	'''
	dfs = [pd.read_csv(folder_path+file) for file in file_name]
	#print(dfs)
	means = [df.iloc[:, :3].mean() for df in dfs]
	return pd.concat(means, axis=1).T

def process_query_3 (file_name):
	'''
	This is a function that creates one dataframe for the third. Since, in this 
	query we have different number of executors, we group by this number and
	then we take the average of the first three columns for each number of executors

	:file_name = a list of the file names with the time measurements for each query

	'''
	df = pd.read_csv(folder_path+file_name)
	#print(df)
	means = df.groupby('Number of executors').mean().reset_index()
	return means



def create_grouped_bar_chart(data, labels, title, queries):
	#print(data)
	ax = data.plot(kind='bar', width=0.5, figsize=(10, 6), color = ['#dc143c', '#523c50', '#efface'])
	ax.set_xticklabels(labels, rotation = 0)
	ax.set_ylabel('Mean time')
	ax.set_title(title)

	#Show only two axis
	ax.spines['right'].set_visible(False)
	ax.spines['top'].set_visible(False)
	ax.spines['left'].set_visible(True)
	ax.spines['bottom'].set_visible(True)

	#Change axis config
	xlab = ax.xaxis.get_label()
	ylab = ax.yaxis.get_label()
	xlab.set_style('italic')
	xlab.set_size(10)
	ylab.set_style('italic')
	ylab.set_size(10)

	#Change title config
	ttl = ax.title
	ttl.set_weight('bold')

	if queries == '3':
		ax.legend(loc='upper right', ncol = 1, facecolor = 'white', framealpha = 0.5)
	else:
		ax.legend(loc='upper left', ncol = 1, facecolor = 'white', framealpha = 0.5)

	#Add values on top of each bar
	for p in ax.patches:
		ax.annotate(f'{p.get_height():.4f}', (p.get_x() + p.get_width() / 2, p.get_height() * 1.005),
                ha='center', va='bottom')
	
	plt.tight_layout()
	plt.grid(axis='y')
	ax.set_axisbelow(True)
	save_path = folder_path+'/'+'query' + queries +'_plot.png'
	plt.savefig(save_path, dpi = 150)
	#plt.show()

query1_file_names = []
query2_file_names = []
query3_file_names = []

#Iterate through the file to find the names of the files with the time measurements
for file_name in file_list:
	if file_name.endswith('timers.csv'):
		file_path = os.path.join(folder_path, file_name)
		if file_name.startswith('query1'):
			query1_file_names.append(file_name)
		elif file_name.startswith('query2'):
			query2_file_names.append(file_name)
		else:
			query3_file_names = file_name 

data_query1 = process_query_1_2(query1_file_names)
data_query2 = process_query_1_2(query2_file_names)
data_query3 = process_query_3(query3_file_names)

labels_query1 = [file_name.removesuffix('_timers.csv') for file_name in query1_file_names]
labels_query2 = [file_name.removesuffix('_timers.csv') for file_name in query2_file_names]
labels_query3 = ["2 executors", "3 executors", "4 executors"]

create_grouped_bar_chart(data_query1, labels_query1, "Query 1 measurements", "1")
create_grouped_bar_chart(data_query2, labels_query2, "Query 2 measurements", "2")
create_grouped_bar_chart(data_query3.set_index('Number of executors'), labels_query3, "Query 3 measurements", "3")
