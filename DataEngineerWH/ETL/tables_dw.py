from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


class CreateTablesDW:
    def __init__(self, data_genero_df, data_actividades_df, data_dep_df, data_prov_df, data_sal_med_df):
        self.data_genero_df = data_genero_df
        self.data_actividades_df = data_actividades_df
        self.data_dep_df = data_dep_df
        self.data_prov_df = data_prov_df
        self.data_sal_med_df = data_sal_med_df

    def create_fact_table(self):
        table_fact = self.data_genero_df.select(['provincia_id', 'in_departamento','clae6','genero','Empleo','Establecimientos','empresas_exportadoras','Año'])
        table_fact = table_fact.withColumnsRenamed({'provincia_id':'Provincia_ID', 'in_departamento':'Departamento_ID',
                                   'clae6':'ID_Clae6', 'genero':'Genero', 'empresas_exportadoras':'Empresas_Exportadoras', 'Año':'Fecha'})
        window_spec = Window.orderBy("Fecha")
        table_fact = table_fact.withColumn('ID_Gen', row_number().over(window_spec))
        table_fact = table_fact.select(['ID_Gen','Provincia_ID', 'Departamento_ID', 'ID_Clae6', 'Genero', 'Empleo', 'Establecimientos', 'Empresas_Exportadoras', 'Fecha'])
        return table_fact
    
    def create_dim_salario(self, dim_act):
        dim_salario = self.data_sal_med_df.select(['Año', 'clae6','w_mean'])
        # Definir una ventana de partición si es necesario
        window_spec = Window.orderBy("Año")
        dim_salario = dim_salario.withColumn('ID_Sal', row_number().over(window_spec))
        dim_salario = dim_salario.withColumn('ID_Sal_t', row_number().over(window_spec))
        dim_salario = dim_salario.withColumnsRenamed({'Año':'Fecha', 'clae6':'ID_Clae6', 'w_mean':'Salario_Mean'})
        dim_salario = dim_salario.select(['ID_Sal_t','ID_Sal', 'ID_Clae6','Salario_Mean', 'Fecha'])
        df_filtered_salarios = dim_salario.join(dim_act, on='ID_Clae6', how='inner')
        df_filtered_salarios = df_filtered_salarios.select('ID_Sal_t', 'ID_Clae6', 'Salario_Mean', 'Fecha')
        df_filtered_salarios = df_filtered_salarios.withColumnRenamed('ID_Sal_t', 'ID_Sal')

        return df_filtered_salarios
    
    def create_dim_prov(self):
        dim_prov = self.data_prov_df.withColumnsRenamed({'provincia_id':'Provincia_ID', 'provincia':'Provincia'})
        
        return dim_prov
    
    def create_dim_dep(self):
        dim_dep = self.data_dep_df.withColumnsRenamed({'in_departamento':'Departamento_ID','departamento':'Departamento'})
        
        return dim_dep
    
    def create_dim_act(self):
        dim_act = self.data_actividades_df.select(['clae6', 'clae6_desc'])
        dim_act = dim_act.withColumnsRenamed({'clae6':'ID_Clae6', 'clae6_desc':'Desc_clae6'})

        return dim_act