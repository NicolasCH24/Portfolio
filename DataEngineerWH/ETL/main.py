from pyspark.sql import SparkSession 
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'path_of_py_files')))

from transform_data import TransformData
from tables_dw import CreateTablesDW
from load_data import LoadDW

def spark_session():
    spark = SparkSession.builder.appName("Employment Analysis").getOrCreate()
    clase_transform = TransformData(spark)
    return clase_transform

def etl_phase_one(clase_transform):
    data_genero_df = clase_transform.transform_data_gen()
    data_actividades_df = clase_transform.transform_data_act()
    data_sal_med_df = clase_transform.transform_data_salario()
    data_dep_df, data_prov_df, data_genero_df = clase_transform.transform_data_dep_prov(data_genero_df)

    return data_genero_df, data_actividades_df, data_sal_med_df, data_dep_df, data_prov_df

def etl_phase_two(data_genero_df, data_actividades_df, data_sal_med_df, data_dep_df, data_prov_df):
    clase_tables = CreateTablesDW(data_genero_df, data_actividades_df, data_dep_df, data_prov_df, data_sal_med_df)

    table_fact = clase_tables.create_fact_table()
    dim_act = clase_tables.create_dim_act()
    dim_salario = clase_tables.create_dim_salario(dim_act)
    dim_prov = clase_tables.create_dim_prov()
    dim_dep = clase_tables.create_dim_dep()

    return dim_prov, dim_dep, dim_act, dim_salario, table_fact

def etl_phase_three(dim_prov, dim_dep, dim_act, dim_salario, table_fact):
    clase_load = LoadDW(dim_prov, dim_dep, dim_act, dim_salario, table_fact)
    clase_load.load_dim_fact_data()

def main():
    clase_transform = spark_session()

    data_genero_df, data_actividades_df, data_sal_med_df, data_dep_df, data_prov_df = etl_phase_one(clase_transform)

    dim_prov, dim_dep, dim_act, dim_salario, table_fact = etl_phase_two(data_genero_df, data_actividades_df, data_sal_med_df, data_dep_df, data_prov_df)

    etl_phase_three(dim_prov, dim_dep, dim_act, dim_salario, table_fact)

if __name__ == '__main__':
    main()

