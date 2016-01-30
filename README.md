# bigdata-analysis

Usage:

1. ./gradlew shadowJar

2. Go to build/libs folder

To run on local:

java -jar bigdata-analysis-1.0-all.jar arrivalDelay \<input-file-path\> \<output-dir-path\>

To run on hadoop:

hadoop jar bigdata-analysis-1.0-all.jar arrivalDelay \<hdfs-input-file-path\> \<hdfs-output-dir-path\>
