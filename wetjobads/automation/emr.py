import boto3

connection = boto3.client("emr")

cluster_id = connection.run_job_flow(
    Name="test_emr_job_boto3",
    LogUri="s3://emr-logs-7913",
    ReleaseLabel="emr-5.33.0",
    Applications=[
        {"Name": "Spark"},
        {"Name": "Hadoop"},
        {"Name": "Hive"},
    ],
    Instances={
        "InstanceGroups": [
            {
                "Name": "Master nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Slave nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
        ],
        "Ec2KeyName": "march2021_ec2",
        "KeepJobFlowAliveWhenNoSteps": False,
        "TerminationProtected": False,
        # 'Ec2SubnetId': 'subnet-04a2978b7fc0b4606',
    },
    Steps=[
        {
            "Name": "extract-jobads-step",
            "ActionOnFailure": "TERMINATE_JOB_FLOW",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "/usr/bin/spark-submit",
                    "--deploy-mode",
                    "cluster",
                    "--class",
                    "com.revature.wetjobads.Runner",
                    "s3://emr-src-7913/wetjobads-assembly-0.1.0-SNAPSHOT.jar"
                ],
            },
        }
    ],
    VisibleToAllUsers=True,
    JobFlowRole="EMR_EC2_DefaultRole",
    ServiceRole="EMR_DefaultRole",
    Tags=[
        {
            "Key": "tag_name_1",
            "Value": "tab_value_1",
        },
        {
            "Key": "tag_name_2",
            "Value": "tag_value_2",
        },
    ],
)

print("cluster created with the step...", cluster_id["JobFlowId"])
