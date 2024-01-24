# Advanced Topics in Database Systems --Results--

This folder contains two folders. The one named "original_results" contains the result files unchanged. That means that all the query@_results.csv will have "|" as a separator, while the query@_timers.csv files will have "," as a separator. The other folder, named "comma_separated" contains the same amount of files with the same data, but now all the files will have "," as a separator.

Both folders contain the results as computed by the queries. Each successive folder contains the answer to the respective query in csv format, as well as the plots for the first three queries. The folder named hint_and_explain contains three folders, one for each mode of explain(). 

> [!NOTE]
> In order to change the separator in files from "|" to "," the change_separator.py, inside the src/helper-code folder, was used. For this python > file to  work you need to create a different folder, where you copy and paste all four folders from the results
> folder. Therefore, when the change_separator.py finishes, there will be the original folder with the results (where some CSV files will have "|" delimiter) and another folder, where "," will be the separator for all CSV files.
