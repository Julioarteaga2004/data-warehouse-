"""
Configuración del Proyecto ETL - Ventas Tecnología
"""

# Configuración de conexión origen (OLTP)
DATABASE_CONFIG_ORIGIN = {
    "server": "LAPTOP-7N3QU2NM\SQLEXPRESS",
    "database": "VentasTecnologiaOLTP",
    "username": "sa",
    "password": "1234"
}

# Configuración de conexión destino (OLAP)
DATABASE_CONFIG_DESTINATION = {
    "server": "LAPTOP-7N3QU2NM\SQLEXPRESS",
    "database": "VentasTecnologiaOLAP",
    "username": "sa",
    "password": "1234"
}

# Configuración de logging (SIN EMOJIS)
LOGGING_CONFIG = {
    "level": "INFO",
    "format": "%(asctime)s - %(levelname)s - %(message)s",
    "file": "etl_log.txt"
}

# Tablas organizadas por ORDEN DE DEPENDENCIAS
SUBDIMENSIONS = [
    "DimTiempo",      # Sin dependencias
    "DimMarca",       # Sin dependencias  
    "DimCategoria",   # Sin dependencias
    "DimProveedor",   # Sin dependencias
    "DimDireccion",   # Sin dependencias
    "DimEmpleado"     # Sin dependencias
]

DIMENSIONS = [
    "DimProducto",    # Depende de: DimMarca, DimCategoria, DimProveedor
    "DimCliente"      # Depende de: DimDireccion
]

FACT_TABLES = [
    "HechosVentas"    # Depende de: DimTiempo, DimProducto, DimCliente, DimEmpleado
]

# Todas las tablas en orden de dependencias
AVAILABLE_TABLES = SUBDIMENSIONS + DIMENSIONS + FACT_TABLES

# Definir dependencias de cada tabla
TABLE_DEPENDENCIES = {
    # Sub-dimensiones (sin dependencias)
    "DimTiempo": [],
    "DimMarca": [],
    "DimCategoria": [],
    "DimProveedor": [],
    "DimDireccion": [],
    "DimEmpleado": [],
    
    # Dimensiones principales
    "DimProducto": ["DimMarca", "DimCategoria", "DimProveedor"],
    "DimCliente": ["DimDireccion"],
    
    # Tabla de hechos
    "HechosVentas": ["DimTiempo", "DimProducto", "DimCliente", "DimEmpleado"]
}

# Claves foráneas por tabla
TABLE_FOREIGN_KEYS = {
    "DimProducto": {
        "MarcaID": "DimMarca",
        "CategoriaID": "DimCategoria", 
        "ProveedorID": "DimProveedor"
    },
    "DimCliente": {
        "DireccionID": "DimDireccion"
    },
    "HechosVentas": {
        "TiempoId": "DimTiempo",
        "ProductoID": "DimProducto",
        "ClienteID": "DimCliente",
        "EmpleadoID": "DimEmpleado"
    }
}

# Transformaciones disponibles
AVAILABLE_TRANSFORMATIONS = {
    "lowercase": "Convertir a minúsculas",
    "uppercase": "Convertir a mayúsculas", 
    "extract_year": "Extraer año de fecha",
    "extract_month": "Extraer mes de fecha",
    "extract_day": "Extraer día de fecha",
    "extract_hour": "Extraer hora de fecha/hora",
    "concatenate": "Concatenar con otro valor"
}
