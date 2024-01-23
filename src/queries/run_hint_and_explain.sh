declare -a mode=("extended" "simple" "formatted")
declare -a query=("query3_df" "query4_1" "query4_2")
declare -a hint=("BROADCAST" "MERGE" "SHUFFLE_HASH" "SHUFFLE_REPLICATE_NL")

#The loops create a file for the results, if it does not exist already
#and run using all the different join hints and explain combinations
for modes in "${mode[@]}"
do
    for queries in "${query[@]}"
    do
        for hints in "${hint[@]}"
        do
            touch /home/user/opt/results/hint_and_eplain/${modes}/${queries}_${hints}_${modes}.out
            spark-submit ${queries}.py 4 ${hints} ${modes} > /home/user/opt/results/hint_and_eplain/${modes}/${queries}_${hints}_${modes}.out
        done
    done
done
