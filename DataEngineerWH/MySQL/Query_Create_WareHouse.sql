USE datawarehouse;

CREATE TABLE IF NOT EXISTS Dim_Provincias (
    Provincia_ID INT PRIMARY KEY,
    Provincia VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS Dim_Departamentos (
    Departamento_ID INT PRIMARY KEY,
    Departamento VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS Dim_Actividad (
    ID_Clae6 INT PRIMARY KEY,
    Desc_clae6 TEXT
);

CREATE TABLE IF NOT EXISTS Dim_Salarios (
    ID_Sal INT PRIMARY KEY,
    ID_Clae6 INT,
    Salario_Mean DECIMAL(10,2),
    Fecha INT,
    FOREIGN KEY (ID_Clae6) REFERENCES Dim_Actividad(ID_Clae6)
);

CREATE TABLE IF NOT EXISTS Fact_Empleo (
	ID_Gen INT,
    Provincia_ID INT,
    Departamento_ID INT,
    ID_Clae6 INT,
    Genero VARCHAR(10),
    Empleo INT,
    Establecimientos INT,
    Empresas_Exportadoras INT,
    Fecha INT,
    PRIMARY KEY (ID_Gen),
    FOREIGN KEY (Provincia_ID) REFERENCES Dim_Provincias(Provincia_ID),
    FOREIGN KEY (Departamento_ID) REFERENCES Dim_Departamentos(Departamento_ID),
    FOREIGN KEY (ID_Clae6) REFERENCES Dim_Actividad(ID_Clae6)
);


