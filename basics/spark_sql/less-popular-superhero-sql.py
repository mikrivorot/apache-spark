from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

sparkSession: SparkSession = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schemaToApplyForNewTable: StructType = StructType([ \
                    StructField("id", IntegerType(), True), \
                    StructField("name", StringType(), True)])

names: DataFrame = sparkSession.read \
    .schema(schemaToApplyForNewTable) \
    .option("sep", " ") \
    .csv("./data/Marvel-Names.txt")

names.createOrReplaceTempView("superhero_names_view")

# Table X with one column "values"
oneColumnTable: DataFrame = sparkSession.read.text("./data/Marvel-Graph.txt")    
oneColumnTable.createOrReplaceTempView("superhero_connections_raw_view")
    
rawConnections: DataFrame = sparkSession.sql("""
    SELECT
        ELEMENT_AT(SPLIT(TRIM(superhero_connections_raw_view.value), ' '),1) as id,
        SUM(SIZE(SPLIT(TRIM(superhero_connections_raw_view.value), ' ')) - 1) as connections
    FROM superhero_connections_raw_view
    GROUP BY id
    ORDER BY connections ASC
    """)

rawConnections.createOrReplaceTempView("superhero_connections_view")
connections: DataFrame = sparkSession.sql("""
    SELECT
        snv.name,
        connections
    FROM superhero_connections_view AS scv
    JOIN superhero_names_view AS snv
    ON snv.id = scv.id
    WHERE connections = (SELECT MIN(superhero_connections_view.connections) FROM superhero_connections_view)
    """)
    
connections.show()