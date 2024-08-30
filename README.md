# spark
<img width="388" alt="Screenshot 2024-08-30 at 16 02 36" src="https://github.com/user-attachments/assets/4fcc08ae-c35f-4b7f-baf7-57d69f3eecbf">

spark-submit --master spark://spark-master:7077      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0     --executor-cores 1     --conf spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp -Divy.home=/tmp"     /opt/spark/shkCreate_edu_100/shkCreate_sync_simple.py

<img width="1710" alt="Screenshot 2024-08-30 at 16 08 06" src="https://github.com/user-attachments/assets/4a38a9a9-b175-4d36-89b3-c8198a39ee3c">

