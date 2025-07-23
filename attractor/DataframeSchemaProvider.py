from pyspark.sql.types import (
    StringType,
    StructField,
    IntegerType,
    DoubleType,
    StructType,
    ArrayType,
    FloatType,
)

from enum import Enum

class MRDataFrameColumns(Enum):
    EDGE_TYPE = 'type'
    VERTEX_START = 'center'
    VERTEX_END = 'target'
    DISTANCE = 'weight'
    VERTEX_ID = 'vertex_id'
    DEGREE = 'degree'
    TRIPLETS = 'triplets'
    I = 'i'
    J = 'j'
    K = 'k'
    NEIGHBORS = 'neighbors'



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

    @staticmethod
    def get_schema_partitioned():
        return StructType(
            [
                StructField(str(MRDataFrameColumns.EDGE_TYPE.value), StringType(), True), # 'S'
                StructField(MRDataFrameColumns.VERTEX_ID.value, IntegerType(), True), # center vertex of the star graph
                StructField(MRDataFrameColumns.TRIPLETS.value, ArrayType(StructType(
                    [
                    StructField(MRDataFrameColumns.I.value, IntegerType(), True),
                    StructField(MRDataFrameColumns.J.value, IntegerType(), True),
                    StructField(MRDataFrameColumns.K.value, IntegerType(), True),
                    ]
                )), True)
            ]
        )
    
    @staticmethod
    def get_schema_star_graph():
        return StructType([
            StructField(MRDataFrameColumns.VERTEX_ID.value, IntegerType(), True),
            StructField(MRDataFrameColumns.NEIGHBORS.value, ArrayType(
                StructType([
                    StructField(MRDataFrameColumns.VERTEX_ID.value, IntegerType(), True),
                    StructField(MRDataFrameColumns.DEGREE.value, IntegerType(), True),
                ])
            )),
            StructField(MRDataFrameColumns.TRIPLETS.value, ArrayType(StructType(
                    [
                    StructField(MRDataFrameColumns.I.value, IntegerType(), True),
                    StructField(MRDataFrameColumns.J.value, IntegerType(), True),
                    StructField(MRDataFrameColumns.K.value, IntegerType(), True),
                    ]
                )), True)
        ])