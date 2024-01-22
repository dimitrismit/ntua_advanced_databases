#declare -a mode=("extended" "simple" "formatted")
declare -a mode=("formatted")
declare -a query=("query3_df" "query4_1" "query4_2")
declare -a hint=("BROADCAST" "MERGE" "SHUFFLE_HASH" "SHUFFLE_REPLICATE_NL")

## now loop through the above array
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