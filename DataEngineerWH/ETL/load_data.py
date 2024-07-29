class LoadDW:
    def __init__(self, dim_prov, dim_dep, dim_act, dim_salario, table_fact):
        self.dim_prov = dim_prov
        self.dim_act = dim_act
        self.dim_salario = dim_salario
        self.dim_dep = dim_dep
        self.table_fact = table_fact

    def load_dim_fact_data(self):
        for datos, tabla in {self.dim_prov:'dim_provincias', self.dim_dep:'dim_departamentos', self.dim_act:'dim_actividad', self.dim_salario:'dim_salarios', self.table_fact:'fact_empleo'}.items():
            datos.write \
            .format("jdbc") \
            .option("url", "jdbc:mysql://HOST:PORT/DB_NAME") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", tabla) \
            .option("user", "USER") \
            .option("password", "PASSWORD") \
            .mode("append") \
            .save()