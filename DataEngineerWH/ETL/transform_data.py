# PYSPARK
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DateType, DecimalType
from pyspark.sql.functions import col, when, regexp_extract, concat, lit, mean, round, asc
from pyspark.sql.functions import year, month, when

class TransformData:
    def __init__(self, spark):
        self.spark = spark
        self.ruta_dist = "distribucion_establecimientos_productivos_sexo (1).csv"
        self.ruta_gen = "Datos_por_departamento_actividad_y_sexo.csv"
        self.ruta_act = "actividades_establecimientos.csv"
        self.ruta_salario = "salario_medio_clae6.csv"

    def transform_data_dist(self):
        data_distribucion_schema = StructType([
        StructField("cuit", StringType(), True),
        StructField("sucursal", IntegerType(), True),
        StructField("anio", DateType(), True),
        StructField("lat", DecimalType(), True),
        StructField("lon", DecimalType(), True),
        StructField("clae6", IntegerType(), True),
        StructField("in_departamento", IntegerType(), True),
        StructField("provincia_id", IntegerType(), True),
        StructField("quintil", IntegerType(), True),
        StructField("empleo", StringType(), True),
        StructField("proporcion_mujeres", DecimalType(), True)
        ])

        data_distribucion_df = self.spark.read.schema(data_distribucion_schema).format("csv").option("header","true").load(self.ruta_dist)

        data_distribucion_df = data_distribucion_df.withColumn('Año', year(col('anio')))
        data_distribucion_df = data_distribucion_df.withColumn('Mes', month(col('anio')))

        data_distribucion_df = data_distribucion_df.withColumn(
            'Min_empleados',
            regexp_extract(col('empleo'),r'(\d+)', 1)
        )

        data_distribucion_df = data_distribucion_df.withColumn(
            'Max_empleados',
            regexp_extract(col('empleo'), r'(\d+)-(\d+)', 2)
        )

        data_distribucion_df = data_distribucion_df.withColumn(
            'Mujeres',
            when((col('proporcion_mujeres') == 0), False)
            .otherwise(True)
        )

        data_distribucion_df = data_distribucion_df.withColumn(
            'Nivel_Empleo',
            regexp_extract(col('empleo'),r'[a-zA-Z0-9]', 0)
        )
        
        data_distribucion_df = data_distribucion_df.withColumn('Año', year(col('anio')))
        return data_distribucion_df
    
    def transform_data_gen(self):
        data_genero_schema = StructType([
        StructField("anio", DateType(), True),
        StructField("in_departamento", IntegerType(), True),
        StructField('departamento', StringType(), True),
        StructField("provincia_id", IntegerType(), True),
        StructField("provincia", StringType(), True),
        StructField("clae6", IntegerType(), True),
        StructField("clae2", IntegerType(), True),
        StructField("letra", StringType(), True),
        StructField('genero', StringType(), True),
        StructField('Empleo', IntegerType(), True),
        StructField('Establecimientos', IntegerType(), True),
        StructField('empresas_exportadoras', IntegerType(), True),
        ])

        data_genero_df = self.spark.read.schema(data_genero_schema).format("csv").option("header","true").load(self.ruta_gen)

        data_genero_df = data_genero_df.withColumn(
            'Sexo',
            when((col('genero') == 'Varones'), 1)
            .otherwise(0)
        )

        data_genero_df = data_genero_df.withColumn(
            'id_act',
            concat(col('letra'), lit('_'), col('clae6'))
        )

        data_genero_df = data_genero_df.withColumn('Año', year(col('anio')))
        data_genero_df = data_genero_df.withColumn('Mes', month(col('anio')))

        return data_genero_df
    

    def transform_data_act(self):
        data_actividades_schema = StructType([
            StructField("clae6", IntegerType(), True),
            StructField("clae2", IntegerType(), True),
            StructField('letra', StringType(), True),
            StructField("clae6_desc", StringType(), True),
            StructField("clae2_desc", StringType(), True),
            StructField("letra_desc", StringType(), True),
        ])

        data_actividades_df = self.spark.read.schema(data_actividades_schema).format("csv").option("header","true").load(self.ruta_act)

        data_actividades_df = data_actividades_df.withColumn(
            'id_act',
            concat(col('letra'), lit('_'), col('clae6'))
        )

        return data_actividades_df
    
    def transform_data_salario(self):
        data_sal_med_schema = StructType([
            StructField("fecha", DateType(), True),
            StructField("clae6", IntegerType(), True),
            StructField('w_mean', IntegerType(), True),
        ])

        data_sal_med_df = self.spark.read.schema(data_sal_med_schema).format("csv").option("header","true").load(self.ruta_salario)

        data_sal_med_df = data_sal_med_df.withColumn('Año', year(col('fecha')))
        data_sal_med_df = data_sal_med_df.withColumn('Mes', month(col('fecha')))
        data_sal_med_df = data_sal_med_df.filter((col('Año') >= 2021) & (col('Año') <= 2022))
        data_sal_med_df = data_sal_med_df.groupBy(['clae6', 'Año']).agg(round(mean('w_mean'),2).alias('w_mean')).orderBy(asc('Año'))

        return data_sal_med_df

    def transform_data_dep_prov(self, data_genero_df):
        # DATA DEPARTAMENTO
        data_dep_id = data_genero_df.select('in_departamento', 'departamento').distinct().collect()
        schema = StructType([
        StructField('in_departamento', IntegerType(), True),
        StructField('departamento', StringType(), True),
        ])

        data_dep_df = self.spark.createDataFrame(data_dep_id, schema)

        # DATA PROVINCIAS
        data_prov = data_genero_df.select('provincia_id', 'provincia').distinct().collect()
        schema = StructType([
        StructField('provincia_id', IntegerType(), True),
        StructField('provincia', StringType(), True),
        ])

        data_prov_df = self.spark.createDataFrame(data_prov, schema)

        # RETORNO DATA GENERO NORMALIZADO
        data_genero_df = data_genero_df.drop(*['departamento', 'provincia'])

        return data_dep_df, data_prov_df, data_genero_df