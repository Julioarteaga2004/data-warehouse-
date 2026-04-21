-- Crear la base de datos
CREATE DATABASE VentasTecnologiaOLTP;
GO

USE VentasTecnologiaOLTP;
GO

-- Tabla: Categorías
CREATE TABLE Categorias (
    CategoriaID INT PRIMARY KEY IDENTITY(1,1),
    Nombre VARCHAR(50) NOT NULL,
    Descripcion VARCHAR(200)
);

-- Tabla: Marcas
CREATE TABLE Marcas (
    MarcaID INT PRIMARY KEY IDENTITY(1,1),
    Nombre VARCHAR(50) NOT NULL,
    Descripcion VARCHAR(200)
);

-- Tabla: Proveedores
CREATE TABLE Proveedores (
    ProveedorID INT PRIMARY KEY IDENTITY(1,1),
    Nombre VARCHAR(100) NOT NULL,
    Email VARCHAR(100),
    Telefono VARCHAR(20)
);

-- Tabla: Productos
CREATE TABLE Productos (
    ProductoID INT PRIMARY KEY IDENTITY(1,1),
    Nombre VARCHAR(100) NOT NULL,
    Descripcion VARCHAR(500),
    Precio DECIMAL(10,2) NOT NULL,
    CategoriaID INT,
    MarcaID INT,
    ProveedorID INT,
    FOREIGN KEY (CategoriaID) REFERENCES Categorias(CategoriaID),
    FOREIGN KEY (MarcaID) REFERENCES Marcas(MarcaID),
    FOREIGN KEY (ProveedorID) REFERENCES Proveedores(ProveedorID)
);

-- Tabla: Almacenes
CREATE TABLE Almacenes (
    AlmacenID INT PRIMARY KEY IDENTITY(1,1),
    Nombre VARCHAR(100) NOT NULL,
    Direccion VARCHAR(200),
    Ciudad VARCHAR(50)
);

-- Tabla: Inventario
CREATE TABLE Inventario (
    InventarioID INT PRIMARY KEY IDENTITY(1,1),
    ProductoID INT,
    AlmacenID INT,
    Cantidad INT NOT NULL,
    FechaActualizacion DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (ProductoID) REFERENCES Productos(ProductoID),
    FOREIGN KEY (AlmacenID) REFERENCES Almacenes(AlmacenID)
);

-- Tabla: Clientes
CREATE TABLE Clientes (
    ClienteID INT PRIMARY KEY IDENTITY(1,1),
    Nombre VARCHAR(100) NOT NULL,
    Apellido VARCHAR(100),
    Email VARCHAR(100),
    TipoCliente VARCHAR(20) CHECK (TipoCliente IN ('Individual', 'Corporativo')),
    FechaRegistro DATETIME DEFAULT GETDATE()
);

-- Tabla: Direcciones
CREATE TABLE Direcciones (
    DireccionID INT PRIMARY KEY IDENTITY(1,1),
    ClienteID INT,
    Direccion VARCHAR(200) NOT NULL,
    Ciudad VARCHAR(50),
    Pais VARCHAR(50),
    CodigoPostal VARCHAR(10),
    FOREIGN KEY (ClienteID) REFERENCES Clientes(ClienteID)
);

-- Tabla: Contactos
CREATE TABLE Contactos (
    ContactoID INT PRIMARY KEY IDENTITY(1,1),
    ClienteID INT,
    Telefono VARCHAR(20),
    Email VARCHAR(100),
    TipoContacto VARCHAR(20) CHECK (TipoContacto IN ('Primario', 'Secundario')),
    FOREIGN KEY (ClienteID) REFERENCES Clientes(ClienteID)
);

-- Tabla: Empleados
CREATE TABLE Empleados (
    EmpleadoID INT PRIMARY KEY IDENTITY(1,1),
    Nombre VARCHAR(100) NOT NULL,
    Apellido VARCHAR(100),
    Email VARCHAR(100),
    Puesto VARCHAR(50),
    FechaContratacion DATETIME DEFAULT GETDATE()
);

-- Tabla: Pedidos
CREATE TABLE Pedidos (
    PedidoID INT PRIMARY KEY IDENTITY(1,1),
    ClienteID INT,
    EmpleadoID INT,
    FechaPedido DATETIME DEFAULT GETDATE(),
    Estado VARCHAR(20) CHECK (Estado IN ('Pendiente', 'Procesado', 'Enviado', 'Entregado', 'Cancelado')),
    FOREIGN KEY (ClienteID) REFERENCES Clientes(ClienteID),
    FOREIGN KEY (EmpleadoID) REFERENCES Empleados(EmpleadoID)
);

-- Tabla: DetallesPedidos
CREATE TABLE DetallesPedidos (
    DetalleID INT PRIMARY KEY IDENTITY(1,1),
    PedidoID INT,
    ProductoID INT,
    Cantidad INT NOT NULL,
    PrecioUnitario DECIMAL(10,2) NOT NULL,
    Descuento DECIMAL(5,2),
    FOREIGN KEY (PedidoID) REFERENCES Pedidos(PedidoID),
    FOREIGN KEY (ProductoID) REFERENCES Productos(ProductoID)
);

-- Tabla: Pagos
CREATE TABLE Pagos (
    PagoID INT PRIMARY KEY IDENTITY(1,1),
    PedidoID INT,
    MetodoPago VARCHAR(50) CHECK (MetodoPago IN ('Tarjeta', 'Transferencia', 'Efectivo')),
    Estado VARCHAR(20) CHECK (Estado IN ('Completado', 'Pendiente', 'Fallido')),
    FOREIGN KEY (PedidoID) REFERENCES Pedidos(PedidoID)
);

-- Tabla: Promociones
CREATE TABLE Promociones (
    PromocionID INT PRIMARY KEY IDENTITY(1,1),
    Nombre VARCHAR(100) NOT NULL,
    Descripcion VARCHAR(200),
    Descuento DECIMAL(5,2),
    FechaInicio DATETIME,
    FechaFin DATETIME
);

-- Tabla: ProductosPromociones
CREATE TABLE ProductosPromociones (
    ProductoPromocionID INT PRIMARY KEY IDENTITY(1,1),
    ProductoID INT,
    PromocionID INT,
    FOREIGN KEY (ProductoID) REFERENCES Productos(ProductoID),
    FOREIGN KEY (PromocionID) REFERENCES Promociones(PromocionID)
);

-- Tabla: Envios
CREATE TABLE Envios (
    EnvioID INT PRIMARY KEY IDENTITY(1,1),
    PedidoID INT,
    DireccionID INT,
    FechaEnvio DATETIME,
    Transportista VARCHAR(100),
    Estado VARCHAR(20) CHECK (Estado IN ('En tránsito', 'Entregado', 'Retrasado')),
    FOREIGN KEY (PedidoID) REFERENCES Pedidos(PedidoID),
    FOREIGN KEY (DireccionID) REFERENCES Direcciones(DireccionID)
);

-- Tabla: Reseñas
CREATE TABLE Reseñas (
    ReseñaID INT PRIMARY KEY IDENTITY(1,1),
    ProductoID INT,
    ClienteID INT,
    Calificacion INT CHECK (Calificacion BETWEEN 1 AND 5),
    Comentario VARCHAR(500),
    FechaReseña DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (ProductoID) REFERENCES Productos(ProductoID),
    FOREIGN KEY (ClienteID) REFERENCES Clientes(ClienteID)
);

-- Índices para mejorar el rendimiento
CREATE INDEX IX_Pedidos_ClienteID ON Pedidos(ClienteID);
CREATE INDEX IX_DetallesPedidos_PedidoID ON DetallesPedidos(PedidoID);
CREATE INDEX IX_Inventario_ProductoID ON Inventario(ProductoID);
CREATE INDEX IX_Reseñas_ProductoID ON Reseñas(ProductoID);