from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import Row
from attractor.DataframeSchemaProvider import DataframeSchemaProvider, MRDataFrameColumns
from libs.Settings import EdgeTypeEnum

def get_partitioned_dataframe(spark: SparkSession, reduce_output_rdd) -> DataFrame:
        # Row is the object type for DataFrame rows
        # Convert RDD to RDD of Rows
        row_rdd = reduce_output_rdd.map(
            lambda r: Row(
                edge_type=r[0],
                vertex_id=r[1],
                triplets=[
                    Row(i=int(t.split()[0]), j=int(t.split()[1]), k=int(t.split()[2]))
                    for t in r[3]
                ]
            )
        )
        schema = DataframeSchemaProvider.get_schema_partitioned()
        return spark.createDataFrame(row_rdd, schema)

def get_star_graph_dataframe(spark: SparkSession, star_graph_rdd) -> DataFrame:
    new_star_graph_rdd = star_graph_rdd.map(lambda row: {
        MRDataFrameColumns.VERTEX_ID.value: row[0],
        MRDataFrameColumns.NEIGHBORS.value: [
            {MRDataFrameColumns.VERTEX_ID.value: n[0], MRDataFrameColumns.DEGREE.value: n[1]}
            for n in row[1]
        ],
        MRDataFrameColumns.TRIPLETS.value: [
            {MRDataFrameColumns.I.value: t[0], MRDataFrameColumns.J.value: t[1], MRDataFrameColumns.K.value: t[2]}
            for t in row[2]
        ]
    })
    schema = DataframeSchemaProvider.get_schema_star_graph()
    return spark.createDataFrame(new_star_graph_rdd, schema=schema)