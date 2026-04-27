# core python + aws glue / spark imports
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# used to convert between spark dataframes and glue dynamicframes
from awsglue.dynamicframe import DynamicFrame


# get job name from arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# initialize spark + glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# initialize and start the glue job
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# filter only specific regions early (improves performance)
predicate_pushdown = "region in ('ca','gb','us')"

# read data from glue catalog (table: raw_statistics)
# push_down_predicate limits data read from source
datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database="db_youtube_raw",
    table_name="raw_statistics",
    transformation_ctx="datasource0",
    push_down_predicate=predicate_pushdown
)


# select + enforce schema (basically keeps only needed columns + types)
applymapping1 = ApplyMapping.apply(
    frame=datasource0,
    mappings=[
        ("video_id", "string", "video_id", "string"),
        ("trending_date", "string", "trending_date", "string"),
        ("title", "string", "title", "string"),
        ("channel_title", "string", "channel_title", "string"),
        ("category_id", "long", "category_id", "long"),
        ("publish_time", "string", "publish_time", "string"),
        ("tags", "string", "tags", "string"),
        ("views", "long", "views", "long"),
        ("likes", "long", "likes", "long"),
        ("dislikes", "long", "dislikes", "long"),
        ("comment_count", "long", "comment_count", "long"),
        ("thumbnail_link", "string", "thumbnail_link", "string"),
        ("comments_disabled", "boolean", "comments_disabled", "boolean"),
        ("ratings_disabled", "boolean", "ratings_disabled", "boolean"),
        ("video_error_or_removed", "boolean", "video_error_or_removed", "boolean"),
        ("description", "string", "description", "string"),
        ("region", "string", "region", "string")
    ],
    transformation_ctx="applymapping1"
)


# handles any ambiguous or conflicting data types
resolvechoice2 = ResolveChoice.apply(
    frame=applymapping1,
    choice="make_struct",
    transformation_ctx="resolvechoice2"
)


# removes null fields from the dataset
dropnullfields3 = DropNullFields.apply(
    frame=resolvechoice2,
    transformation_ctx="dropnullfields3"
)


# convert to spark dataframe so we can control partitions
datasink1 = dropnullfields3.toDF().coalesce(1)

# convert back to dynamicframe for glue output
df_final_output = DynamicFrame.fromDF(
    datasink1, glueContext, "df_final_output"
)


# write final data to s3 as parquet, partitioned by region
datasink4 = glueContext.write_dynamic_frame.from_options(
    frame=df_final_output,
    connection_type="s3",
    connection_options={
        "path": "s3://de-on-youtube-cleansed-useast1-dev/youtube/raw_statistics/",
        "partitionKeys": ["region"]
    },
    format="parquet",
    transformation_ctx="datasink4"
)


# commit job (marks it as successful)
job.commit()
