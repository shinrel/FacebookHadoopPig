INPUT_PATH="/user/hadoop/input/all"
OUTPUT_PATH="/user/hadoop/output/Question2h"
OUTPUT_PATH_JOB1="/user/hadoop/output/Question2h/job1"
OUTPUT_PATH_JOB2="/user/hadoop/output/Question2h/job2"
#if [ $# -eq 0 ]
#then
#    echo "Usage: input Jar file path"
#    exit 1
#fi
hadoop fs -rmr -skipTrash $OUTPUT_PATH
hadoop dfs -mkdir $OUTPUT_PATH
hadoop jar Question2h.jar $INPUT_PATH $OUTPUT_PATH_JOB1 $OUTPUT_PATH_JOB2
echo "Result: \n"
hadoop fs -cat $OUTPUT_PATH_JOB1/part*
hadoop fs -cat $OUTPUT_PATH_JOB2/part*

