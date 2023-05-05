Usage
=====

1. Install python libraries
```
$ pip install -r requirements.txt
```

2. Generate data
```
$ python alb_log_gen.py --output_uri <destination> --rows <number> --partitions <number>
```

Arguments
* output_uri: The URI where output is saved, like an S3 bucket location. Local path to a folder also works.
* rows [Optional]: Number of rows to generate. Default: 10
* partitions [Optional]: Number of partitions to generate. Default: `spark.sparkContext.defaultParallelism`