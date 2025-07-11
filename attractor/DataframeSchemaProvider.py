from pyspark.sql.types import (
    StringType,
    StructField,
    IntegerType,
    DoubleType,
    StructType,
    
)

from enum import Enum

class MRDataFrameColumns(Enum):
    EDGE_TYPE = 'edge_type'
    VERTEX_START = 'vertex_start'
    VERTEX_END = 'vertex_end'
    DISTANCE = 'distance'
    VERTEX_ID = 'vertex_id'
    DEGREE = 'degree'



class DataframeSchemaProvider:
    @staticmethod
    def get_schema_graph_jaccard():
        return StructType(
            [
                StructField(str(MRDataFrameColumns.EDGE_TYPE.value), StringType(), True),
                StructField(str(MRDataFrameColumns.VERTEX_START.value), IntegerType(), True),
                StructField(str(MRDataFrameColumns.VERTEX_END.value), IntegerType(), True),
                StructField(str(MRDataFrameColumns.DISTANCE.value), DoubleType(), True),
            ]
        )

    @staticmethod
    def get_schema_degree():
        return StructType(
            [
                StructField(MRDataFrameColumns.VERTEX_ID.value, IntegerType(), True),
                StructField(MRDataFrameColumns.DEGREE.value, IntegerType(), True),
            ]
        )
