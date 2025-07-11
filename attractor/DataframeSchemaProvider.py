from pyspark.sql.types import (
    StringType,
    StructField,
    IntegerType,
    DoubleType,
    StructType,
)


class DataframeSchemaProvider:
    @staticmethod
    def get_schema_graph_jaccard():
        return StructType(
            [
                StructField("edge_type", StringType(), True),
                StructField("begin_vertex", IntegerType(), True),
                StructField("end_vertex", IntegerType(), True),
                StructField("distance", DoubleType(), True),
            ]
        )
