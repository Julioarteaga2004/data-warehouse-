"""
🔗 Gestor de Dependencias para ETL
"""

import logging
from config import TABLE_DEPENDENCIES, TABLE_FOREIGN_KEYS, SUBDIMENSIONS, DIMENSIONS, FACT_TABLES

class DependencyManager:
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.logger = logging.getLogger(__name__)
        
    def check_table_dependencies(self, target_table):
        """Verificar si las dependencias de una tabla están satisfechas"""
        try:
            dependencies = TABLE_DEPENDENCIES.get(target_table, [])
            
            if not dependencies:
                return {
                    "can_load": True,
                    "missing_dependencies": [],
                    "message": f"✅ {target_table} no tiene dependencias. Puede cargarse."
                }
            
            missing_deps = []
            satisfied_deps = []
            
            for dep_table in dependencies:
                if self._table_has_data(dep_table):
                    satisfied_deps.append(dep_table)
                else:
                    missing_deps.append(dep_table)
            
            can_load = len(missing_deps) == 0
            
            if can_load:
                message = f"✅ {target_table} puede cargarse. Dependencias satisfechas: {satisfied_deps}"
            else:
                message = f"❌ {target_table} NO puede cargarse. Faltan dependencias: {missing_deps}"
                
            return {
                "can_load": can_load,
                "missing_dependencies": missing_deps,
                "satisfied_dependencies": satisfied_deps,
                "message": message
            }
            
        except Exception as e:
            self.logger.error(f"Error verificando dependencias de {target_table}: {str(e)}")
            return {
                "can_load": False,
                "missing_dependencies": [],
                "message": f"❌ Error verificando dependencias: {str(e)}"
            }
    
    def _table_has_data(self, table_name):
        """Verificar si una tabla tiene datos"""
        try:
            query = f"SELECT COUNT(*) as count FROM {table_name}"
            result = self.db_manager.execute_query(query, "destination")
            
            if not result.empty and result.iloc[0]['count'] > 0:
                return True
            return False
            
        except Exception as e:
            self.logger.warning(f"No se pudo verificar datos en {table_name}: {str(e)}")
            return False
    
    def validate_foreign_keys(self, df, target_table):
        """Validar que las claves foráneas existan en las tablas referenciadas"""
        try:
            fk_config = TABLE_FOREIGN_KEYS.get(target_table, {})
            
            if not fk_config:
                return {
                    "valid": True,
                    "missing_keys": {},
                    "message": f"✅ {target_table} no tiene claves foráneas para validar."
                }
            
            missing_keys = {}
            
            for fk_column, ref_table in fk_config.items():
                if fk_column not in df.columns:
                    continue
                    
                # Obtener valores únicos de la columna FK
                fk_values = df[fk_column].dropna().unique()
                
                if len(fk_values) == 0:
                    continue
                
                # Verificar que existan en la tabla referenciada
                missing_values = self._check_missing_foreign_keys(fk_values, ref_table, fk_column)
                
                if missing_values:
                    missing_keys[fk_column] = {
                        "reference_table": ref_table,
                        "missing_values": missing_values
                    }
            
            is_valid = len(missing_keys) == 0
            
            if is_valid:
                message = f"✅ Todas las claves foráneas de {target_table} son válidas."
            else:
                message = f"❌ Claves foráneas inválidas en {target_table}: {list(missing_keys.keys())}"
                
            return {
                "valid": is_valid,
                "missing_keys": missing_keys,
                "message": message
            }
            
        except Exception as e:
            self.logger.error(f"Error validando claves foráneas: {str(e)}")
            return {
                "valid": False,
                "missing_keys": {},
                "message": f"❌ Error validando claves foráneas: {str(e)}"
            }
    
    def _check_missing_foreign_keys(self, fk_values, ref_table, fk_column):
        """Verificar qué valores de FK no existen en la tabla referenciada"""
        try:
            # Determinar el nombre de la columna PK en la tabla referenciada
            pk_column = self._get_primary_key_column(ref_table, fk_column)
            
            # Crear consulta para verificar existencia
            values_str = "', '".join([str(v) for v in fk_values])
            query = f"SELECT {pk_column} FROM {ref_table} WHERE {pk_column} IN ('{values_str}')"
            
            existing_values = self.db_manager.execute_query(query, "destination")
            
            if not existing_values.empty and pk_column in existing_values.columns:
                existing_values[pk_column] = existing_values[pk_column].astype(str)
            
            if existing_values.empty:
                return list(fk_values)
            
            existing_set = set(existing_values[pk_column].values)
            # SOLUCIÓN: Convertir 'v' a string antes de la comparación
            missing_values = [v for v in fk_values if str(v) not in existing_set]
            
            return missing_values
            
        except Exception as e:
            self.logger.error(f"Error verificando FK en {ref_table}: {str(e)}")
            return list(fk_values)  # Asumir que todos faltan si hay error
    
    def _get_primary_key_column(self, ref_table, fk_column):
        """Obtener el nombre de la columna PK de la tabla referenciada"""
        # Mapeo de tablas a sus columnas PK
        pk_mapping = {
            "DimTiempo": "TiempoID",
            "DimMarca": "MarcaID",
            "DimCategoria": "CategoriaID",
            "DimProveedor": "ProveedorID",
            "DimDireccion": "DireccionID",
            "DimCliente": "ClienteID",
            "DimEmpleado": "EmpleadoID",
            "DimProducto": "ProductoID"
        }
        
        return pk_mapping.get(ref_table, fk_column)
    
    def get_loadable_tables(self):
        """Obtener lista de tablas que pueden cargarse actualmente"""
        loadable = []
        blocked = []
        
        for table in SUBDIMENSIONS + DIMENSIONS + FACT_TABLES:
            dep_check = self.check_table_dependencies(table)
            
            if dep_check["can_load"]:
                loadable.append({
                    "table": table,
                    "type": self._get_table_type(table),
                    "dependencies": TABLE_DEPENDENCIES.get(table, [])
                })
            else:
                blocked.append({
                    "table": table,
                    "type": self._get_table_type(table),
                    "missing_dependencies": dep_check["missing_dependencies"]
                })
        
        return {
            "loadable": loadable,
            "blocked": blocked
        }
    
    def _get_table_type(self, table_name):
        """Obtener el tipo de tabla (Sub-dimensión, Dimensión, Hechos)"""
        if table_name in SUBDIMENSIONS:
            return "Sub-dimensión"
        elif table_name in DIMENSIONS:
            return "Dimensión"
        elif table_name in FACT_TABLES:
            return "Tabla de Hechos"
        return "Desconocido"
