"""
🔄 Módulo de Transformación de Datos
"""

import pandas as pd
import logging
from datetime import datetime
import re

class DataTransformer:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
    def apply_transformations(self, df, field_transformations):
        """Aplicar transformaciones a los campos seleccionados"""
        try:
            self.logger.info("Iniciando transformaciones de datos")
            transformed_df = df.copy()
            
            for field, transformation in field_transformations.items():
                if field not in df.columns:
                    self.logger.warning(f"Campo '{field}' no encontrado en los datos")
                    continue
                    
                if transformation is None:
                    continue
                    
                self.logger.info(f"Aplicando transformación '{transformation['type']}' a campo '{field}'")
                transformed_df[field] = self._apply_single_transformation(
                    transformed_df[field], transformation
                )
                
            self.logger.info("Transformaciones completadas")
            return transformed_df
            
        except Exception as e:
            self.logger.error(f"Error en transformaciones: {str(e)}")
            raise
            
    def _apply_single_transformation(self, series, transformation):
        """Aplicar una transformación específica a una serie"""
        transform_type = transformation['type']
        
        try:
            if transform_type == 'lowercase':
                return series.astype(str).str.lower()
                
            elif transform_type == 'uppercase':
                return series.astype(str).str.upper()
                
            elif transform_type == 'extract_year':
                return pd.to_datetime(series).dt.year
                
            elif transform_type == 'extract_month':
                return pd.to_datetime(series).dt.month
                
            elif transform_type == 'extract_day':
                return pd.to_datetime(series).dt.day
                
            elif transform_type == 'extract_hour':
                return pd.to_datetime(series).dt.hour
                
            elif transform_type == 'concatenate':
                concat_value = transformation.get('value', '')
                return series.astype(str) + str(concat_value)
                
            else:
                self.logger.warning(f"Tipo de transformación desconocido: {transform_type}")
                return series
                
        except Exception as e:
            self.logger.error(f"Error aplicando transformación {transform_type}: {str(e)}")
            return series
            
    def clean_data(self, df):
        """Limpiar datos básicos"""
        try:
            original_rows = len(df)
            
            # Eliminar filas completamente vacías
            df_cleaned = df.dropna(how='all')
            
            # Limpiar espacios en strings
            string_columns = df_cleaned.select_dtypes(include=['object']).columns
            for col in string_columns:
                df_cleaned[col] = df_cleaned[col].astype(str).str.strip()
                
            rows_removed = original_rows - len(df_cleaned)
            if rows_removed > 0:
                self.logger.info(f"Limpieza completada. Filas removidas: {rows_removed}")
                
            return df_cleaned
            
        except Exception as e:
            self.logger.error(f"Error en limpieza de datos: {str(e)}")
            return df
            
    def validate_transformed_data(self, df, destination_table):
        """Validar datos transformados según tabla destino"""
        validations = []
        
        # Validaciones básicas
        if df.empty:
            validations.append("DataFrame vacío después de transformaciones")
            
        # Validaciones específicas por tabla
        table_validations = {
            'DimTiempo': self._validate_dim_tiempo,
            'HechosVentas': self._validate_hechos_ventas,
            'DimProducto': self._validate_dim_producto
        }
        
        if destination_table in table_validations:
            table_validations[destination_table](df, validations)
            
        for validation in validations:
            self.logger.warning(validation)
            
        return validations
        
    def _validate_dim_tiempo(self, df, validations):
        """Validaciones específicas para DimTiempo"""
        if 'TiempoID' in df.columns:
            try:
                pd.to_datetime(df['TiempoID'])
            except:
                validations.append("TiempoID no es un formato de fecha válido")
                
    def _validate_hechos_ventas(self, df, validations):
        """Validaciones específicas para HechosVentas"""
        required_columns = ['TiempoId', 'ProductoID', 'ClienteID', 'EmpleadoID']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            validations.append(f"Columnas faltantes en HechosVentas: {missing_columns}")
            
    def _validate_dim_producto(self, df, validations):
        """Validaciones específicas para DimProducto"""
        if 'ProductoID' in df.columns and df['ProductoID'].isnull().any():
            validations.append("ProductoID contiene valores nulos")
