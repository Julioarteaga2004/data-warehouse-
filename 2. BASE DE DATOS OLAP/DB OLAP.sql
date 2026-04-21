-- Crear base de datos OLAP
CREATE DATABASE VentasTecnologiaOLAP;
GO

USE VentasTecnologiaOLAP;
GO

-- Dimensión Tiempo
CREATE TABLE DimTiempo (
    TiempoID  DATE PRIMARY KEY NOT NULL,
    Amio INT,
    NombreMes VARCHAR(20),
    Trimestre INT,
    Día VARCHAR(20),
    Semana INT
);

-- Dimensión Marca
CREATE TABLE DimMarca (
    MarcaID INT PRIMARY KEY  NOT NULL,
    Nombre VARCHAR(50)
);

-- Dimensión Categoría
CREATE TABLE DimCategoria (
    CategoriaID INT PRIMARY KEY  NOT NULL,
    Nombre VARCHAR(50)
);

-- Dimensión Proveedor
CREATE TABLE DimProveedor (
    ProveedorID INT PRIMARY KEY NOT NULL,
    Nombre VARCHAR(100),
    Email VARCHAR(100)
);

-- Dimensión Producto
CREATE TABLE DimProducto (
    ProductoID INT PRIMARY KEY  NOT NULL,
    Nombre VARCHAR(100),
    MarcaID INT,
    CategoriaID INT,
    ProveedorID INT,
    FOREIGN KEY (MarcaID) REFERENCES DimMarca(MarcaID),
    FOREIGN KEY (CategoriaID) REFERENCES DimCategoria(CategoriaID),
    FOREIGN KEY (ProveedorID) REFERENCES DimProveedor(ProveedorID)
);

-- Dimensión Dirección
CREATE TABLE DimDireccion (
    DireccionID INT PRIMARY KEY NOT NULL,
    Ciudad VARCHAR(50),
    Pais VARCHAR(50)
);

-- Dimensión Cliente
CREATE TABLE DimCliente (
    ClienteID INT PRIMARY KEY  NOT NULL,
    NombreCompleto VARCHAR(200),
    TipoCliente VARCHAR(20),
    DireccionID INT,
    FOREIGN KEY (DireccionID) REFERENCES DimDireccion(DireccionID)
);

-- Dimensión Empleado
CREATE TABLE DimEmpleado (
    EmpleadoID INT PRIMARY KEY  NOT NULL,
    NombreCompleto VARCHAR(200)
);


-- Tabla de Hechos: Ventas
CREATE TABLE HechosVentas (
    HechoID INT PRIMARY KEY IDENTITY(1,1),
    TiempoId DATE NOT NULL,
    ProductoID INT NOT NULL,
    ClienteID INT NOT NULL,
    EmpleadoID INT NOT NULL,
    TotalVenta DECIMAL(10,2) NULL,
    FOREIGN KEY (TiempoId) REFERENCES DimTiempo(TiempoID ),
    FOREIGN KEY (ProductoID) REFERENCES DimProducto(ProductoID),
    FOREIGN KEY (ClienteID) REFERENCES DimCliente(ClienteID),
    FOREIGN KEY (EmpleadoID) REFERENCES DimEmpleado(EmpleadoID)
);
