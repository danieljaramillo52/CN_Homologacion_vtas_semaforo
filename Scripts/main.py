import config_path_routes
from Controllers.config_loader import ConfigClaves, ConfigLoader
from Utils.DataQuality_Functions import ensure_dir, resolve_existing_file, verificar_columnas
from Utils.exclusive_functions import VerificadorCodigos
from  Utils.logger_functions import setup_logging
from Scripts.procesar_insumos import ProcesarInsumos


class Aplicacion:
    """Orquesta el flujo principal con etapas: 'carga' → 'listo' → 'filtrar'."""

    def __init__(self) -> None:
        self.config_loader = ConfigLoader()
        self.get_config = self.config_loader.get_config
        self.claves = ConfigClaves(self.config_loader)
    
    def main(self):
        """Orquesta el flujo de trabajo de la automatización "homologación vtas semáforo
        """
        # Configuración global del logger. 
        logger = setup_logging()
        
        # Verificar existencia de directorios y archivos.
        config_insumos = self.get_config("config_insumos")
        dict_cols = self.get_config("dict_cols")
        
        path_insumos = ensure_dir(base_dir=config_insumos["path_insumos"])
    
        path_vtas = resolve_existing_file(base_dir=path_insumos, filename=config_insumos["nom_base"])
        config_insumos["path_insumos"]

        # Verificar insumos. 
        procesador_insumos = ProcesarInsumos(config_insumos=config_insumos, dict_cols=dict_cols)

        # Carga minima previo a verificar cols.
        base_vtas_min , drivers_min = procesador_insumos.carga_minima(path_insumo=path_vtas)
        
        # Validación columnas insumos. 
        verificar_columnas(df=base_vtas_min,columnas_esperadas=dict_cols["cols_ventas"], nombre=config_insumos["base_vtas"]["nom_hoja_vtas"])
        
        verificar_columnas(df=drivers_min,columnas_esperadas=dict_cols["cols_drivers"], nombre=config_insumos["drivers"]["nom_hoja_driver"])
        
        # Carga de archivos completa y con cols selecionadas
        df_vtas, df_drivers = procesador_insumos.carga_completa(path_insumo=path_vtas)
        
        # Instanciar la clase con los DataFrames ya validados
        verificador = VerificadorCodigos(
            df_vtas=df_vtas,
            df_drivers=df_drivers,
            cols_vtas=dict_cols["cols_ventas"],
            cols_drivers=dict_cols["cols_drivers"]
        )
        
        # Aplicar la fórmula (columna calculada)
        df_vtas["STATUS"] = verificador.create_col_status()
        
        df_vtas["COD ECOM FINAL"] = verificador.create_col_cod_cliente_alt()
        
        df_vtas["COD AC FINAL"] = verificador.create_col_agente_resuelta(driver_val="cod", fallback="clave")
        
        df_vtas["NOMBRE AC FINAL"] = verificador.create_col_agente_resuelta(driver_val="nombre", fallback="nombre")
        
if __name__ == "__main__":
    app = Aplicacion()
    app.main()