"""
🎯 Procesador Principal ETL
"""

import logging
from datetime import datetime
from .extract import DataExtractor
from .transform import DataTransformer
from .load import DataLoader

class ETLProcessor:
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.extractor = DataExtractor(db_manager)
        self.transformer = DataTransformer()
        self.loader = DataLoader(db_manager)
        self.logger = logging.getLogger(__name__)
        
    def extract_data(self, source_query):
        """Fase de Extracción"""
        try:
            self.logger.info("=== FASE DE EXTRACCIÓN ===")
            
            # Extraer datos
            extracted_data = self.extractor.extract_from_query(source_query)
            
            # Validar datos extraídos
            validations = self.extractor.validate_extracted_data(extracted_data)
            
            return extracted_data
            
        except Exception as e:
            self.logger.error(f"Error en fase de extracción: {str(e)}")
            raise
            
    def transform_data(self, data, selected_fields, transformations):
        """Fase de Transformación"""
        try:
            self.logger.info("=== FASE DE TRANSFORMACIÓN ===")
            
            # Seleccionar solo los campos requeridos
            if selected_fields:
                data = data[selected_fields].copy()
                
            # Limpiar datos
            cleaned_data = self.transformer.clean_data(data)
            
            # Aplicar transformaciones
            transformed_data = self.transformer.apply_transformations(
                cleaned_data, transformations
            )
            
            return transformed_data
            
        except Exception as e:
            self.logger.error(f"Error en fase de transformación: {str(e)}")
            raise
            
    def load_data(self, data, destination_table):
        """Fase de Carga"""
        try:
            self.logger.info("=== FASE DE CARGA ===")
            
            # Validar datos antes de cargar
            validations = self.transformer.validate_transformed_data(data, destination_table)
            
            # Cargar datos incrementalmente
            loaded_count = self.loader.load_incremental(data, destination_table)
            
            # Validar resultados de carga
            load_validations = self.loader.validate_load_results(
                len(data), loaded_count, destination_table
            )
            
            return loaded_count
            
        except Exception as e:
            self.logger.error(f"Error en fase de carga: {str(e)}")
            raise
            
    def run_complete_etl(self, source_query, selected_fields, transformations, destination_table):
        """Ejecutar ETL completo"""
        try:
            start_time = datetime.now()
            self.logger.info(f"Iniciando ETL completo hacia {destination_table}")
            
            # Extracción
            extracted_data = self.extract_data(source_query)
            
            # Transformación
            transformed_data = self.transform_data(extracted_data, selected_fields, transformations)
            
            # Carga
            loaded_count = self.load_data(transformed_data, destination_table)
            
            # Resumen final
            end_time = datetime.now()
            duration = end_time - start_time
            
            self.logger.info("=== ETL COMPLETADO EXITOSAMENTE ===")
            self.logger.info(f"Duración: {duration}")
            self.logger.info(f"Registros procesados: {len(extracted_data)}")
            self.logger.info(f"Registros cargados: {loaded_count}")
            
            return {
                'success': True,
                'duration': duration,
                'records_processed': len(extracted_data),
                'records_loaded': loaded_count,
                'destination_table': destination_table
            }
            
        except Exception as e:
            self.logger.error(f"ETL falló: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'destination_table': destination_table
            }
