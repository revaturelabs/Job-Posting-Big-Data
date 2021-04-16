bucket="s3://emr-input-revusf"
jarname="wetjobads-assembly-0.1.0-SNAPSHOT.jar"
jarpath="target/scala-2.11"

#sbt assembly &&
s3cmd del "${bucket}/${jarname}" &&
s3cmd put ${jarpath}/${jarname} $bucket &&
s3cmd ls $bucket