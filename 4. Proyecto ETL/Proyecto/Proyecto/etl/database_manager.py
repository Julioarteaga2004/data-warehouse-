"""
🔗 Gestor de Conexiones de Base de Datos - CORREGIDO
"""

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine, text
import pyodbc
import logging
from config import DATABASE_CONFIG_ORIGIN, DATABASE_CONFIG_DESTINATION

class DatabaseManager:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.origin_engine = None
        self.destination_engine = None
        self.connect_databases()
        
    def create_connection_string(self, config):
        """Crear cadena de conexión para SQL Server"""
        return (
            f"mssql+pyodbc://{config['username']}:{config['password']}"
            f"@{config['server']}/{config['database']}"
            f"?driver=ODBC+Driver+17+for+SQL+Server&charset=utf8"
        )
        
    def connect_databases(self):
        """Establecer conexiones a las bases de datos"""
        try:
            # Conexión a base de datos origen (OLTP)
            origin_conn_str = self.create_connection_string(DATABASE_CONFIG_ORIGIN)
            self.origin_engine = create_engine(origin_conn_str, echo=False)
            
            # Conexión a base de datos destino (OLAP)
            dest_conn_str = self.create_connection_string(DATABASE_CONFIG_DESTINATION)
            self.destination_engine = create_engine(dest_conn_str, echo=False)
            
            # Probar conexiones
            with self.origin_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            with self.destination_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                
            self.logger.info("Conexiones a bases de datos establecidas correctamente")
            
        except Exception as e:
            self.logger.error(f"Error conectando a las bases de datos: {str(e)}")
            raise
            
    def execute_query(self, query, engine_type="origin"):
        """Ejecutar consulta SQL y retornar DataFrame"""
        try:
            engine = self.origin_engine if engine_type == "origin" else self.destination_engine
            
            with engine.connect() as connection:
                df = pd.read_sql(query, connection)
                
            self.logger.info(f"Consulta ejecutada exitosamente. Registros: {len(df)}")
            return df
            
        except Exception as e:
            self.logger.error(f"Error ejecutando consulta: {str(e)}")
            raise
            
    def insert_dataframe(self, df, table_name, if_exists='append'):
        """Insertar DataFrame en tabla destino - MEJORADO"""
        try:
            # SOLUCIÓN: Usar método más compatible con SQL Server
            rows_inserted = df.to_sql(
                table_name, 
                self.destination_engine, 
                if_exists=if_exists, 
                index=False,
                method=None,  # Usar método por defecto en lugar de 'multi'
                chunksize=50  # Procesar en chunks pequeños
            )
            
            self.logger.info(f"Datos insertados en {table_name}. Registros: {len(df)}")
            return len(df)
            
        except Exception as e:
            self.logger.error(f"Error insertando datos en {table_name}: {str(e)}")
            raise
            
    def get_existing_records(self, table_name, key_columns):
        """Obtener registros existentes para evitar duplicados"""
        try:
            if not key_columns:
                return pd.DataFrame()
                
            # SOLUCIÓN: Escapar nombres de columnas con corchetes
            escaped_columns = [f"[{col}]" for col in key_columns]
            key_columns_str = ", ".join(escaped_columns)
            query = f"SELECT {key_columns_str} FROM [{table_name}]"
            
            return self.execute_query(query, "destination")
            
        except Exception as e:
            self.logger.warning(f"No se pudieron obtener registros existentes de {table_name}: {str(e)}")
            return pd.DataFrame()
            
    def close_connections(self):
        """Cerrar conexiones"""
        try:
            if self.origin_engine:
                self.origin_engine.dispose()
            if self.destination_engine:
                self.destination_engine.dispose()
            self.logger.info("Conexiones cerradas correctamente")
        except Exception as e:
            self.logger.error(f"Error cerrando conexiones: {str(e)}")
