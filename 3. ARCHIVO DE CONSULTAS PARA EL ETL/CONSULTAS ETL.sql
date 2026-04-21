
--DIMENSION TIEMPO
SELECT DISTINCT
    CAST(FechaPedido AS DATE) AS TiempoID,
    YEAR(FechaPedido) AS Amio,
    DATENAME(MONTH, FechaPedido) AS NombreMes,
    DATEPART(QUARTER, FechaPedido) AS Trimestre,
    DATENAME(WEEKDAY, FechaPedido) AS Día,
    DATEPART(WEEK, FechaPedido) AS Semana
FROM Pedidos;

--DIMENSION MARCA
SELECT
    MarcaID,
    Nombre
FROM Marcas;

--DIMENSION CATEGORIAS
SELECT
    CategoriaID,
    Nombre
FROM Categorias;

--DIMENSION PROVEEDOR
SELECT
    ProveedorID,
    Nombre,
    Email
FROM Proveedores;

-- DIMENSION PRODUCTOS
SELECT
    ProductoID,
    Nombre,
    MarcaID,
    CategoriaID,
    ProveedorID
FROM Productos;

--DIMENSION DIRECCION

SELECT DISTINCT
    DireccionID,
    Ciudad,
    Pais
FROM Direcciones;

-- DIMENSION CLIENTES
SELECT
    c.ClienteID,
    CONCAT(c.Nombre, ' ', c.Apellido) AS NombreCompleto,
    c.TipoCliente,
    d.DireccionID
FROM Clientes c
INNER JOIN Direcciones d ON c.ClienteID = d.ClienteID;

-- DIMENSION EMPLEADO
SELECT
    EmpleadoID,
    CONCAT(Nombre, ' ', Apellido) AS NombreCompleto
FROM Empleados;

-- TABLA HECHOS
SELECT
    CAST(p.FechaPedido AS DATE) AS TiempoId,
    dp.ProductoID,
    p.ClienteID,
    p.EmpleadoID,
    SUM(dp.Cantidad * dp.PrecioUnitario * (1 - (ISNULL(dp.Descuento, 0) / 100))) AS TotalVenta
FROM Pedidos p
INNER JOIN DetallesPedidos dp ON p.PedidoID = dp.PedidoID
WHERE p.Estado = 'Entregado'
GROUP BY CAST(p.FechaPedido AS DATE), dp.ProductoID, p.ClienteID, p.EmpleadoID;


