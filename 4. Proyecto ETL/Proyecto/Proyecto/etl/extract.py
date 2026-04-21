"""
📤 Módulo de Extracción de Datos
"""

import pandas as pd
import logging
from datetime import datetime

class DataExtractor:
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.logger = logging.getLogger(__name__)
        
    def extract_from_query(self, sql_query):
        """Extraer datos usando consulta SQL personalizada"""
        try:
            self.logger.info(f"Iniciando extracción con consulta: {sql_query[:100]}...")
            
            # Validar que la consulta sea SELECT
            if not sql_query.strip().upper().startswith('SELECT'):
                raise ValueError("Solo se permiten consultas SELECT")
                
            # Ejecutar consulta
            df = self.db_manager.execute_query(sql_query, "origin")
            
            if df.empty:
                self.logger.warning("La consulta no retornó datos")
            else:
                self.logger.info(f"Extracción exitosa: {len(df)} registros, {len(df.columns)} columnas")
                
            return df
            
        except Exception as e:
            self.logger.error(f"Error en extracción: {str(e)}")
            raise
            
    def extract_from_table(self, table_name, conditions=None):
        """Extraer datos de una tabla específica"""
        try:
            query = f"SELECT * FROM {table_name}"
            
            if conditions:
                query += f" WHERE {conditions}"
                
            return self.extract_from_query(query)
            
        except Exception as e:
            self.logger.error(f"Error extrayendo de tabla {table_name}: {str(e)}")
            raise
            
    def validate_extracted_data(self, df):
        """Validar datos extraídos"""
        validations = {
            "empty_dataframe": df.empty,
            "null_columns": df.isnull().all().any(),
            "duplicate_rows": df.duplicated().any()
        }
        
        for validation, result in validations.items():
            if result:
                self.logger.warning(f"Validación {validation}: {result}")
                
        return validations
