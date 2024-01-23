hadoop fs -rm -r /results/*

#Do 5 iterations for more accurate results
for i in {1..5}
do
   echo "RUNNING ITERATION $i"

    spark-submit query1_df.py 4
    spark-submit query1_sql.py 4

    spark-submit query2_df.py 4
    spark-submit query2_rdd.py 4

    spark-submit query3_df.py 2 None None
    spark-submit query3_df.py 3 None None
    spark-submit query3_df.py 4 None None

    echo "FINISHING ITERATION $i"
done

spark-submit query4_1.py 4 None None
spark-submit query4_2.py 4 None None

#A new bash file has been created for the hint and explain part
<<comment
spark-submit query3_df.py 4 BROADCAST > /home/user/opt/results/query3_broadcast.out
spark-submit query3_df.py 4 MERGE > /home/user/opt/results/query3_merge.out
spark-submit query3_df.py 4 SHUFFLE_HASH > /home/user/opt/results/query3_shuffle_hash.out
spark-submit query3_df.py 4 SHUFFLE_REPLICATE_NL > /home/user/opt/results/query3_shuffle_replicate_nl.out

spark-submit query4_1.py 4 BROADCAST > /home/user/opt/results/query4_1_broadcast.out
spark-submit query4_1.py 4 MERGE > /home/user/opt/results/query4_1_merge.out
spark-submit query4_1.py 4 SHUFFLE_HASH > /home/user/opt/results/query4_1_shuffle_hash.out
spark-submit query4_1.py 4 SHUFFLE_REPLICATE_NL > /home/user/opt/results/query4_1_shuffle_replicate_nl.out

spark-submit query4_2.py 4 BROADCAST > /home/user/opt/results/query4_2_broadcast.out
spark-submit query4_2.py 4 MERGE > /home/user/opt/results/query4_2_merge.out
spark-submit query4_2.py 4 SHUFFLE_HASH > /home/user/opt/results/query4_2_shuffle_hash.out
spark-submit query4_2.py 4 SHUFFLE_REPLICATE_NL > /home/user/opt/results/query4_2_shuffle_replicate_nl.out
comment


hadoop fs -copyToLocal /results/* /home/user/opt/results/
python3 ../helper-code/plot.py

directory = "/home/user/opt/results"
python3 ../helper-code/plot.py

directory="/home/user/opt/results"

#Save the query results to their respective file. Schema is as follows
# results
#├── hint_and_eplain
#├── query1
#├── query2
#├── query3
#└── query4

#Ensure the main folder exists
if [ ! -d "${directory}" ]; then
    echo "Error: Main folder '${directory}' not found."
    exit 1
fi

for file_path in "${directory}"/*; do
    #echo $file_path
    if [ -f "${file_path}" ]; then
        file_name=$(basename "${file_path}")
        prefix=$(echo "${file_name}" | cut -d'_' -f1)
        target_folder="${directory}/${prefix}"
        # Move the file to the target subfolder
        mv "${file_path}" "${target_folder}/"
        echo "Moved ${file_name} to ${target_folder}/"
    fi
done

#To copy results locally...
#scp -r user@user@snf-42305.ok-kno.grnetcloud.net:/home/user/opt/results /path/to/local/folder
