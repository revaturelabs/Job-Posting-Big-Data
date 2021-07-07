bucket="s3://emr-src-7913"
jarname="wetjobads-assembly-0.1.0-SNAPSHOT.jar"
jarpath="target/scala-2.11"

#sbt assembly &&
aws2 s3 rm "${bucket}/${jarname}" &&
aws2 s3 cp "${jarpath}/${jarname}" $bucket &&
aws2 s3 ls $bucket