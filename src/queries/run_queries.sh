hadoop fs -rm -r /results/*

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

#To copy results locally...
#scp -r user@user@snf-42305.ok-kno.grnetcloud.net:/home/user/opt/results /path/to/local/folder