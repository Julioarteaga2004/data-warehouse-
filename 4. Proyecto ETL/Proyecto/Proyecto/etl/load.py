"""
📥 Módulo de Carga de Datos
"""

import pandas as pd
import logging
from datetime import datetime

class DataLoader:
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.logger = logging.getLogger(__name__)
        
        # Definir claves primarias para cada tabla
        self.table_keys = {
            'DimTiempo': ['TiempoID'],
            'DimMarca': ['MarcaID'],
            'DimCategoria': ['CategoriaID'],
            'DimProveedor': ['ProveedorID'],
            'DimProducto': ['ProductoID'],
            'DimDireccion': ['DireccionID'],
            'DimCliente': ['ClienteID'],
            'DimEmpleado': ['EmpleadoID'],
            'HechosVentas': ['TiempoId', 'ProductoID', 'ClienteID', 'EmpleadoID']
        }
        
    def load_incremental(self, df, destination_table):
        """Cargar solo registros nuevos (carga incremental)"""
        try:
            self.logger.info(f"Iniciando carga incremental en {destination_table}")
            
            if df.empty:
                self.logger.warning("No hay datos para cargar")
                return 0
                
            # Obtener claves de la tabla
            key_columns = self.table_keys.get(destination_table, [])
            
            if not key_columns:
                self.logger.warning(f"No se definieron claves para {destination_table}, cargando todos los registros")
                return self._load_all_records(df, destination_table)
                
            # Verificar que las claves existan en el DataFrame
            missing_keys = [key for key in key_columns if key not in df.columns]
            if missing_keys:
                self.logger.error(f"Claves faltantes en los datos: {missing_keys}")
                raise ValueError(f"Claves faltantes: {missing_keys}")
                
            # Obtener registros existentes
            existing_records = self.db_manager.get_existing_records(destination_table, key_columns)
            
            if existing_records.empty:
                self.logger.info("No hay registros existentes, cargando todos los datos")
                return self._load_all_records(df, destination_table)
                
            # Filtrar registros nuevos
            new_records = self._filter_new_records(df, existing_records, key_columns)
            
            if new_records.empty:
                self.logger.info("No hay registros nuevos para cargar")
                return 0
                
            # Cargar registros nuevos
            return self._load_all_records(new_records, destination_table)
            
        except Exception as e:
            self.logger.error(f"Error en carga incremental: {str(e)}")
            raise
            
    def _filter_new_records(self, new_df, existing_df, key_columns):
        """Filtrar registros que no existen en el destino"""
        try:
            # Crear una clave compuesta para comparación
            new_df_keys = new_df[key_columns].copy()
            existing_df_keys = existing_df[key_columns].copy()
            
            # Convertir a string para comparación
            for col in key_columns:
                new_df_keys[col] = new_df_keys[col].astype(str)
                existing_df_keys[col] = existing_df_keys[col].astype(str)
                
            # Crear clave compuesta
            new_df_keys['composite_key'] = new_df_keys[key_columns].apply(
                lambda x: '|'.join(x.astype(str)), axis=1
            )
            existing_df_keys['composite_key'] = existing_df_keys[key_columns].apply(
                lambda x: '|'.join(x.astype(str)), axis=1
            )
            
            # Filtrar registros nuevos
            existing_keys = set(existing_df_keys['composite_key'])
            new_mask = ~new_df_keys['composite_key'].isin(existing_keys)
            
            filtered_df = new_df[new_mask].copy()
            
            self.logger.info(f"Registros filtrados: {len(new_df)} -> {len(filtered_df)} nuevos")
            
            return filtered_df
            
        except Exception as e:
            self.logger.error(f"Error filtrando registros nuevos: {str(e)}")
            raise
            
    def _load_all_records(self, df, destination_table):
        """Cargar todos los registros del DataFrame"""
        try:
            # Preparar datos según la tabla destino
            prepared_df = self._prepare_data_for_table(df, destination_table)
            
            # SOLUCIÓN: Cargar en lotes pequeños para evitar el error
            batch_size = 100  # Reducir el tamaño del lote
            total_inserted = 0
            
            for i in range(0, len(prepared_df), batch_size):
                batch = prepared_df.iloc[i:i+batch_size]
                
                try:
                    # Insertar lote
                    self.db_manager.insert_dataframe(
                        batch, 
                        destination_table, 
                        if_exists='append'
                    )
                    total_inserted += len(batch)
                    self.logger.info(f"Lote {i//batch_size + 1}: {len(batch)} registros insertados")
                    
                except Exception as batch_error:
                    self.logger.error(f"Error en lote {i//batch_size + 1}: {str(batch_error)}")
                    # Intentar insertar registro por registro en caso de error
                    for idx, row in batch.iterrows():
                        try:
                            single_row_df = pd.DataFrame([row])
                            self.db_manager.insert_dataframe(
                                single_row_df, 
                                destination_table, 
                                if_exists='append'
                            )
                            total_inserted += 1
                        except Exception as row_error:
                            self.logger.error(f"Error insertando fila {idx}: {str(row_error)}")
                            continue
            
            self.logger.info(f"Carga completada en {destination_table}: {total_inserted} registros")
            return total_inserted
            
        except Exception as e:
            self.logger.error(f"Error cargando registros: {str(e)}")
            raise
            
    def _prepare_data_for_table(self, df, table_name):
        """Preparar datos específicamente para cada tabla"""
        prepared_df = df.copy()
        
        try:
            if table_name == 'DimTiempo':
                prepared_df = self._prepare_dim_tiempo(prepared_df)
            elif table_name == 'HechosVentas':
                prepared_df = self._prepare_hechos_ventas(prepared_df)
                
            # SOLUCIÓN: Escapar nombres de columnas problemáticos
            prepared_df = self._escape_column_names(prepared_df)
                
            return prepared_df
            
        except Exception as e:
            self.logger.error(f"Error preparando datos para {table_name}: {str(e)}")
            return prepared_df
            
    def _escape_column_names(self, df):
        """Escapar nombres de columnas que pueden causar problemas en SQL Server"""
        # Mapeo de nombres problemáticos
        column_mapping = {
            'Año': 'Anio',  # Cambiar ñ por ni temporalmente
        }
        
        # Renombrar columnas problemáticas
        df_renamed = df.rename(columns=column_mapping)
        
        return df_renamed
        
    def _prepare_dim_tiempo(self, df):
        """Preparar datos para DimTiempo"""
        if 'TiempoID' in df.columns:
            df['TiempoID'] = pd.to_datetime(df['TiempoID']).dt.date
            
        return df
        
    def _prepare_hechos_ventas(self, df):
        """Preparar datos para HechosVentas"""
        # Convertir tipos de datos
        numeric_columns = ['TotalVenta']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
        return df
        
    def validate_load_results(self, original_count, loaded_count, destination_table):
        """Validar resultados de la carga"""
        validations = []
        
        if loaded_count == 0:
            validations.append("No se cargaron registros")
        elif loaded_count > original_count:
            validations.append("Se cargaron más registros de los esperados")
        else:
            validations.append(f"Carga exitosa: {loaded_count}/{original_count} registros")
            
        for validation in validations:
            self.logger.info(validation)
            
        return validations
