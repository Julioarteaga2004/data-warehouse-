"""
ETL Principal - Sistema de Ventas Tecnología con Gestión de Dependencias
"""

import sys
import os
import logging
from datetime import datetime

# Agregar la carpeta etl al path
sys.path.append(os.path.join(os.path.dirname(__file__), 'etl'))

from config import *
from etl.database_manager import DatabaseManager
from etl.etl_processor import ETLProcessor
from etl.dependency_manager import DependencyManager

class ETLMain:
    def __init__(self):
        self.setup_logging()
        self.db_manager = DatabaseManager()
        self.etl_processor = ETLProcessor(self.db_manager)
        self.dependency_manager = DependencyManager(self.db_manager)
        
    def setup_logging(self):
        """Configurar el sistema de logging"""
        logging.basicConfig(
            level=getattr(logging, LOGGING_CONFIG["level"]),
            format=LOGGING_CONFIG["format"],
            handlers=[
                logging.FileHandler(LOGGING_CONFIG["file"]),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def show_welcome(self):
        """Mostrar mensaje de bienvenida"""
        print("=" * 70)
        print("🚀 SISTEMA ETL - VENTAS TECNOLOGÍA")
        print("=" * 70)
        print("📊 Extracción, Transformación y Carga de Datos")
        print("🎯 Desde OLTP hacia Data Warehouse OLAP")
        print("🔗 Con Gestión de Dependencias de Claves Foráneas")
        print("=" * 70)
        
    def show_available_tables_with_dependencies(self):
        """Mostrar tablas disponibles organizadas por dependencias"""
        print("\n📋 TABLAS DEL DATA WAREHOUSE (Ordenadas por Dependencias):")
        print("=" * 70)
        
        # Obtener estado de las tablas
        table_status = self.dependency_manager.get_loadable_tables()
        
        # Mostrar Sub-dimensiones
        print("\n🔸 SUB-DIMENSIONES (Sin dependencias):")
        print("-" * 50)
        for i, table in enumerate(SUBDIMENSIONS, 1):
            status = "✅ DISPONIBLE" if any(t["table"] == table for t in table_status["loadable"]) else "❌ BLOQUEADA"
            print(f"{i:2d}. {table:<15} - {status}")
        
        # Mostrar Dimensiones principales
        print("\n🔹 DIMENSIONES PRINCIPALES:")
        print("-" * 50)
        start_idx = len(SUBDIMENSIONS) + 1
        for i, table in enumerate(DIMENSIONS, start_idx):
            deps = TABLE_DEPENDENCIES.get(table, [])
            status = "✅ DISPONIBLE" if any(t["table"] == table for t in table_status["loadable"]) else "❌ BLOQUEADA"
            deps_str = f"(Depende de: {', '.join(deps)})" if deps else ""
            print(f"{i:2d}. {table:<15} - {status} {deps_str}")
        
        # Mostrar Tabla de Hechos
        print("\n🔶 TABLA DE HECHOS:")
        print("-" * 50)
        start_idx = len(SUBDIMENSIONS) + len(DIMENSIONS) + 1
        for i, table in enumerate(FACT_TABLES, start_idx):
            deps = TABLE_DEPENDENCIES.get(table, [])
            status = "✅ DISPONIBLE" if any(t["table"] == table for t in table_status["loadable"]) else "❌ BLOQUEADA"
            deps_str = f"(Depende de: {', '.join(deps)})" if deps else ""
            print(f"{i:2d}. {table:<15} - {status} {deps_str}")
        
        print("=" * 70)
        
        # Mostrar resumen de tablas bloqueadas
        if table_status["blocked"]:
            print("\n⚠️  TABLAS BLOQUEADAS POR DEPENDENCIAS:")
            for blocked in table_status["blocked"]:
                missing = ', '.join(blocked["missing_dependencies"])
                print(f"   • {blocked['table']}: Requiere datos en [{missing}]")
        
        return table_status
        
    def get_user_input(self, prompt, input_type=str, options=None):
        """Obtener entrada del usuario con validación"""
        while True:
            try:
                user_input = input(f"\n{prompt}: ").strip()
                
                if input_type == int:
                    value = int(user_input)
                    if options and value not in options:
                        print(f"❌ Opción inválida. Seleccione entre {options}")
                        continue
                    return value
                elif input_type == str:
                    if not user_input:
                        print("❌ Este campo no puede estar vacío")
                        continue
                    return user_input
                    
            except ValueError:
                print("❌ Entrada inválida. Intente nuevamente.")
                continue
                
    def select_destination_table(self):
        """Seleccionar tabla destino con validación de dependencias"""
        while True:
            table_status = self.show_available_tables_with_dependencies()
            
            # Solo permitir seleccionar tablas disponibles
            loadable_tables = [t["table"] for t in table_status["loadable"]]
            
            if not loadable_tables:
                print("\n❌ No hay tablas disponibles para cargar.")
                print("💡 Debe cargar primero las sub-dimensiones.")
                input("\nPresione Enter para continuar...")
                continue
            
            print(f"\n📌 TABLAS DISPONIBLES PARA CARGAR ({len(loadable_tables)}):")
            for i, table in enumerate(loadable_tables, 1):
                table_type = next(t["type"] for t in table_status["loadable"] if t["table"] == table)
                print(f"{i:2d}. {table} ({table_type})")
            
            try:
                table_index = self.get_user_input(
                    "Seleccione el número de la tabla destino", 
                    int, 
                    range(1, len(loadable_tables) + 1)
                )
                
                selected_table = loadable_tables[table_index - 1]
                
                # Verificar dependencias una vez más
                dep_check = self.dependency_manager.check_table_dependencies(selected_table)
                
                if dep_check["can_load"]:
                    print(f"\n✅ Tabla seleccionada: {selected_table}")
                    print(f"📝 {dep_check['message']}")
                    return selected_table
                else:
                    print(f"\n❌ {dep_check['message']}")
                    print("💡 Seleccione otra tabla o cargue las dependencias primero.")
                    continue
                    
            except (ValueError, IndexError):
                print("❌ Selección inválida. Intente nuevamente.")
                continue
        
    def get_source_query(self):
        """Obtener consulta SQL de origen"""
        print("\n📝 CONSULTA SQL DE ORIGEN:")
        print("Puede ingresar una consulta SQL personalizada o el nombre de una tabla")
        print("Ejemplo: SELECT * FROM Productos WHERE Precio > 100")
        print("O simplemente: Productos")
        
        query = self.get_user_input("Ingrese su consulta SQL o nombre de tabla")
        
        # Si no es una consulta completa, asumir que es nombre de tabla
        if not query.upper().startswith('SELECT'):
            query = f"SELECT * FROM {query}"
            
        return query
        
    def select_fields_and_transformations(self, source_data):
        """Seleccionar campos y transformaciones"""
        if source_data.empty:
            raise Exception("No hay datos en la consulta de origen")
            
        available_fields = list(source_data.columns)
        print(f"\n📊 CAMPOS DISPONIBLES ({len(available_fields)}):")
        print("-" * 50)
        
        for i, field in enumerate(available_fields, 1):
            sample_value = str(source_data[field].iloc[0]) if len(source_data) > 0 else "N/A"
            print(f"{i:2d}. {field:<20} (Ej: {sample_value[:30]})")
            
        print("\nIngrese los números de los campos que desea procesar (separados por coma)")
        print("Ejemplo: 1,3,5")
        
        field_selection = self.get_user_input("Campos seleccionados")
        selected_indices = [int(x.strip()) - 1 for x in field_selection.split(',')]
        selected_fields = [available_fields[i] for i in selected_indices]
        
        # Configurar transformaciones para cada campo
        transformations = {}
        for field in selected_fields:
            print(f"\n🔧 TRANSFORMACIONES PARA '{field}':")
            print("1. Sin transformación")
            print("2. Convertir a minúsculas")
            print("3. Convertir a mayúsculas")
            print("4. Extraer año (solo fechas)")
            print("5. Extraer mes (solo fechas)")
            print("6. Extraer día (solo fechas)")
            print("7. Extraer hora (solo fechas)")
            print("8. Concatenar con valor")
            
            transform_option = self.get_user_input(
                f"Seleccione transformación para '{field}'", 
                int, 
                range(1, 9)
            )
            
            if transform_option == 1:
                transformations[field] = None
            elif transform_option == 2:
                transformations[field] = {"type": "lowercase"}
            elif transform_option == 3:
                transformations[field] = {"type": "uppercase"}
            elif transform_option == 4:
                transformations[field] = {"type": "extract_year"}
            elif transform_option == 5:
                transformations[field] = {"type": "extract_month"}
            elif transform_option == 6:
                transformations[field] = {"type": "extract_day"}
            elif transform_option == 7:
                transformations[field] = {"type": "extract_hour"}
            elif transform_option == 8:
                concat_value = self.get_user_input("Valor para concatenar")
                transformations[field] = {"type": "concatenate", "value": concat_value}
                
        return selected_fields, transformations
        
    def run_etl_process(self):
        """Ejecutar el proceso ETL completo con validación de dependencias"""
        try:
            self.logger.info("Iniciando proceso ETL")
            
            # 1. Seleccionar tabla destino (con validación de dependencias)
            destination_table = self.select_destination_table()
            self.logger.info(f"Tabla destino seleccionada: {destination_table}")
            
            # 2. Obtener consulta de origen
            source_query = self.get_source_query()
            self.logger.info(f"Consulta de origen: {source_query}")
            
            # 3. Extraer datos
            print("\n⏳ Extrayendo datos...")
            source_data = self.etl_processor.extract_data(source_query)
            print(f"✅ Extraídos {len(source_data)} registros")
            
            # 4. Seleccionar campos y transformaciones
            selected_fields, transformations = self.select_fields_and_transformations(source_data)
            
            # 5. Transformar datos
            print("\n⏳ Transformando datos...")
            transformed_data = self.etl_processor.transform_data(
                source_data, selected_fields, transformations
            )
            print(f"✅ Transformados {len(transformed_data)} registros")
            
            # 6. Validar claves foráneas antes de cargar
            print("\n🔍 Validando claves foráneas...")
            fk_validation = self.dependency_manager.validate_foreign_keys(transformed_data, destination_table)
            print(fk_validation["message"])
            
            if not fk_validation["valid"]:
                print("\n❌ VALIDACIÓN DE CLAVES FORÁNEAS FALLIDA:")
                for fk_col, fk_info in fk_validation["missing_keys"].items():
                    print(f"   • Campo '{fk_col}': {len(fk_info['missing_values'])} valores no existen en {fk_info['reference_table']}")
                    print(f"     Valores faltantes: {fk_info['missing_values'][:5]}{'...' if len(fk_info['missing_values']) > 5 else ''}")
                
                print(f"\n💡 SOLUCIÓN: Cargue primero las tablas de dependencias:")
                for dep in TABLE_DEPENDENCIES.get(destination_table, []):
                    print(f"   • {dep}")
                
                return False
            
            # 7. Cargar datos (solo nuevos registros)
            print("\n⏳ Cargando datos al Data Warehouse...")
            loaded_count = self.etl_processor.load_data(transformed_data, destination_table)
            
            print(f"\n🎉 PROCESO ETL COMPLETADO EXITOSAMENTE")
            print(f"📊 Registros cargados: {loaded_count}")
            print(f"📅 Fecha/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            self.logger.info(f"ETL completado. Registros cargados: {loaded_count}")
            
        except Exception as e:
            error_msg = f"Error en el proceso ETL: {str(e)}"
            print(f"\n❌ {error_msg}")
            self.logger.error(error_msg)
            return False
            
        return True
        
    def run(self):
        """Método principal de ejecución"""
        self.show_welcome()
        
        while True:
            print("\n" + "=" * 70)
            print("🎯 MENÚ PRINCIPAL")
            print("=" * 70)
            print("1. Ejecutar proceso ETL")
            print("2. Ver estado de tablas y dependencias")
            print("3. Salir")
            
            option = self.get_user_input("Seleccione una opción", int, [1, 2, 3])
            
            if option == 1:
                self.run_etl_process()
            elif option == 2:
                self.show_available_tables_with_dependencies()
                input("\nPresione Enter para continuar...")
            elif option == 3:
                print("\n👋 ¡Hasta luego!")
                break

if __name__ == "__main__":
    etl_main = ETLMain()
    etl_main.run()
