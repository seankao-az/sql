import argparse

from datetime import timedelta, datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, LongType, TimestampType

import dbldatagen as dg
import dbldatagen.distributions as dist

"""
Usage:
python alb_log_gen.py --output_uri <destination> --rows <number> --partitions <number>
"""

parser = argparse.ArgumentParser()
parser.add_argument(
    '--output_uri', help="The URI where output is saved, like an S3 bucket location.")
parser.add_argument(
    '--rows', help="Number of rows to generate.")
parser.add_argument(
    '--partitions', help="Number of partitions to generate.")
args = parser.parse_args()

schema = StructType([
    StructField("type", StringType(), True),
    StructField("time", TimestampType(), True),
    StructField("elb", StringType(), True),
    StructField("client_ip", StringType(), True),
    StructField("client_port", IntegerType(), True),
    StructField("target_ip", StringType(), True),
    StructField("target_port", IntegerType(), True),
    StructField("request_processing_time", DoubleType(), True),
    StructField("target_processing_time", DoubleType(), True),
    StructField("response_processing_time", DoubleType(), True),
    StructField("elb_status_code", IntegerType(), True),
    StructField("target_status_code", StringType(), True),
    StructField("received_bytes", LongType(), True),
    StructField("sent_bytes", LongType(), True),
    StructField("request_verb", StringType(), True),
    StructField("request_url", StringType(), True),
    StructField("request_proto", StringType(), True),
    StructField("user_agent", StringType(), True),
    StructField("ssl_cipher", StringType(), True),
    StructField("ssl_protocol", StringType(), True),
    StructField("target_group_arn", StringType(), True),
    StructField("trace_id", StringType(), True),
    StructField("domain_name", StringType(), True),
    StructField("chosen_cert_arn", StringType(), True),
    StructField("matched_rule_priority", StringType(), True),
    StructField("request_creation_time", TimestampType(), True),
    StructField("actions_executed", StringType(), True),
    StructField("redirect_url", StringType(), True),
    StructField("lambda_error_reason", StringType(), True),
    StructField("target_port_list", StringType(), True),
    StructField("target_status_code_list", StringType(), True),
    StructField("classification", StringType(), True),
    StructField("classification_reason", StringType(), True),
])

nRows = 10
if args.rows is not None:
    nRows = int(args.rows)

partitions = None
if args.partitions is not None:
    partitions = int(args.partitions)

# time
interval = timedelta(days=1, hours=1, minutes=1, seconds=1, milliseconds=1)
start = datetime(2022, 5, 3, 0, 0, 0)
end = datetime(2023, 5, 3, 6, 0, 0)

# user_agent
user_agents = ["curl/7.46.0", "Mozilla/5.0 (Android 4.4; Mobile; rv:41.0) Gecko/41.0 Firefox/41.0", "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_3_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"]

# target_status_code
status_codes = ["200", "404", "500", "301", "504"]

# request_verb
request_verbs = ["GET", "POST", "DELETE", "PUT"]

# ssl
ssl_cipher = "ECDHE-RSA-AES128-GCM-SHA256"
ssl_protocol = "TLSv1.2"

# arn
arn_prefix = "arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/my-targets/"

# .master() <- "local", "local[4]", "yarn"
with SparkSession.builder.appName("ALB").getOrCreate() as spark:
    # withColumn lines are temporary columns that won't go in the final table
    # they're for precomputing values for other columns
    x3 = (
        dg.DataGenerator(sparkSession=spark, name="alb_logs", rows=nRows, partitions=partitions)
            .withSchema(schema)
            .withColumnSpec("type", values=["http", "https"], random=True)
            .withColumnSpec("time", begin=start, end=end, interval=interval, random=True)
            # elb: "app/my-loadbalancer/<random id>"
            .withColumn("pre_elb", template=r"ddaadadddadadddd", random=True, omit=True)
            .withColumnSpec("elb", expr="concat('app/my-loadbalancer/', pre_elb)", baseColumn=["pre_elb"])

            .withColumnSpec("client_ip", template=r"192.168.\n.\n", random=True)
            .withColumnSpec("client_port", minValue=0, maxValue=9999, step=1, random=True)
            .withColumnSpec("target_ip", template=r"10.0.dd.dd", random=True)
            .withColumnSpec("target_port", minValue=0, maxValue=9999, step=1, random=True)

            .withColumn("pre_request_processing_time", "double", minValue=0, maxValue=0.5, step=0.001, distribution=dist.Exponential(100), random=True, omit=True)
            .withColumnSpec("request_processing_time", expr="pre_request_processing_time / 10", baseColumn=["pre_request_processing_time"])
            .withColumn("pre_target_processing_time", "double", minValue=0, maxValue=0.5, step=0.001, distribution=dist.Exponential(100), random=True, omit=True)
            .withColumnSpec("target_processing_time", expr="pre_target_processing_time / 10", baseColumn=["pre_target_processing_time"])
            .withColumn("pre_response_processing_time", "double", minValue=0, maxValue=1, step=0.001, distribution=dist.Exponential(100), random=True, omit=True)
            .withColumnSpec("response_processing_time", expr="pre_response_processing_time / 10", baseColumn=["pre_response_processing_time"])

            .withColumnSpec("elb_status_code", values=[200], random=True)
            .withColumnSpec("target_status_code", values=status_codes, weights=[200, 2, 1, 1, 1], random=True)

            .withColumnSpec("received_bytes", minValue=0, maxValue=100000, random=True)
            .withColumnSpec("sent_bytes", minValue=0, maxValue=100000, random=True)

            .withColumnSpec("request_verb", values=request_verbs, weights=[20, 20, 1, 5], random=True)
            # request_url: "http[s]://www.example<digit>.com"
            .withColumn("pre_request_url", template=r"://www.e\x\ampled.com", random=True, omit=True)
            .withColumnSpec("request_url", expr="concat(type, pre_request_url)", baseColumn=["type"])
            .withColumnSpec("request_proto", values=["HTTP/1.1"], random=True)
            .withColumnSpec("user_agent", values=user_agents, random=True)

            # "-" if not https
            # no strcmp for spark sql so use length of type to decide it's http or https
            # len is introduced in Spark 3.4.0
            .withColumnSpec("ssl_cipher", expr=f"if(length(type)=5, \"{ssl_cipher}\", \"-\")", baseColumn=["type"])
            .withColumnSpec("ssl_protocol", expr=f"if(length(type)=5, \"{ssl_protocol}\", \"-\")", baseColumn=["type"])

            .withColumn("pre_arn", template=r"ddaadadddadadddd", random=True, omit=True)
            .withColumnSpec("target_group_arn", expr=f"concat(\"{arn_prefix}\", pre_arn)", baseColumn=["pre_arn"])
            .withColumnSpec("trace_id", template=r"Root=d-dddddddd-ddadddaadaddddddddaaaddd", random=True)
            .withColumnSpec("domain_name", values=["-"], random=True)
            .withColumnSpec("chosen_cert_arn", expr="if(length(type)=5, \"arn:aws:acm:us-east-2:123456789012:certificate/12345678-1234-1234-1234-123456789012\", \"-\")", baseColumn=["type"])
            .withColumnSpec("matched_rule_priority", minValue=-1, maxValue=10000)

            # TODO: spark sql date_add only allow difference in days, not seconds
            # find a way to make <request_creation_time> a little bit earlier than <time>
            .withColumnSpec("request_creation_time", expr="time", baseColumn=["time"])
            .withColumn("pre_actions_executed", values=["forward", "redirect"], weights=[4, 1], random=True, omit=True)
            .withColumnSpec("actions_executed", expr="if(length(type)=5, concat(\"authenticate,\", pre_actions_executed), pre_actions_executed)", baseColumn=["type", "pre_actions_executed"])
            # redirect url just the same as request url
            .withColumnSpec("redirect_url", expr="if(length(pre_actions_executed)=8, request_url, \"-\")", baseColumn=["request_url", "pre_actions_executed"])
            .withColumnSpec("lambda_error_reason", values=["-"], random=True)
            .withColumnSpec("target_port_list", expr="concat(concat(target_ip, \":\"), target_port)", baseColumn=["target_ip", "target_port"])
            .withColumnSpec("target_status_code_list", expr="target_status_code", baseColumn=["target_status_code"])
            .withColumnSpec("classification", values=["-"], random=True)
            .withColumnSpec("classification_reason", values=["-"], random=True)
    )

    x3_output = x3.build(withTempView=True)
    # x3_output.printSchema()
    # x3_output.show()
    x3_output.write.option("compression", "gzip").json(args.output_uri)
    # x3_output.write.json("output")