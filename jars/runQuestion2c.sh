INPUT_PATH="/user/hadoop/input/all"
OUTPUT_PATH="/user/hadoop/output/Question2c"
#if [ $# -eq 0 ]
#then
#    echo "Usage: input Jar file path"
#    exit 1
#fi
hadoop fs -rmr -skipTrash $OUTPUT_PATH
hadoop jar Question2c.jar $INPUT_PATH $OUTPUT_PATH
echo "Result: \n"
hadoop fs -cat $OUTPUT_PATH/part*
