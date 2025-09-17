import os
import config_path_routes
from Controllers.config_loader import ConfigClaves, ConfigLoader
from Utils.DataQuality_Functions import ensure_dir, resolve_existing_file, verificar_columnas
from Utils.exclusive_functions import VerificadorCodigos
from Utils.logger_functions import setup_logging
from Scripts.procesar_insumos import ProcesarInsumos


class Aplicacion:
    """Orquesta el flujo principal con etapas: 'carga' → 'listo' → 'filtrar'."""

    def __init__(self) -> None:
        self.config_loader = ConfigLoader()
        self.get_config = self.config_loader.get_config
        self.claves = ConfigClaves(self.config_loader)
    
    def main(self):
        """Orquesta el flujo de trabajo de la automatización 'homologación vtas semáforo'."""
        logger = setup_logging()
        
        # Cargar configuración
        config_insumos = self.get_config("config_insumos")
        dict_cols = self.get_config("dict_cols")
        cfg_result = self.get_config("Resultados")
        
        # Paths base
        path_insumos = ensure_dir(base_dir=config_insumos["path_insumos"])
    
        # Archivos independientes
        path_vtas = resolve_existing_file(
            base_dir=path_insumos,
            filename=config_insumos["base_vtas"]["nom_base"]       
        )
        path_drivers = resolve_existing_file(
            base_dir=path_insumos,
            filename=config_insumos["drivers"]["nom_base"])

        # Verificar insumos (carga mínima)
        procesador_insumos = ProcesarInsumos(config_insumos=config_insumos, dict_cols=dict_cols)
        
        base_vtas_min, drivers_min = procesador_insumos.carga_minima(
            path_vtas=path_vtas, 
            path_drivers=path_drivers
        )
        
        # Validación de columnas esperadas
        verificar_columnas(
            df=base_vtas_min,
            columnas_esperadas=dict_cols["cols_ventas"],
            nombre=config_insumos["base_vtas"]["nom_hoja"]
        )
        verificar_columnas(
            df=drivers_min,
            columnas_esperadas=dict_cols["cols_drivers"],
            nombre=config_insumos["drivers"]["nom_hoja"]
        )
        
        # Carga completa (misma lógica/resultado que antes)
        df_vtas, df_drivers = procesador_insumos.carga_completa(
            path_vtas=path_vtas, 
            path_drivers=path_drivers
        )
        
        # Transformaciones
        verificador = VerificadorCodigos(
            df_vtas=df_vtas,
            df_drivers=df_drivers,
            cols_vtas=dict_cols["cols_ventas"],
            cols_drivers=dict_cols["cols_drivers"]
        )
        
        df_vtas["status"] = verificador.create_col_status()
        df_vtas[dict_cols["cols_ventas"]["codigo_ecom"]] = verificador.create_col_cod_cliente_alt()
        df_vtas[dict_cols["cols_ventas"]["agente_comercial_clave"]] = verificador.create_col_agente_resuelta(driver_val="cod", fallback="clave")
        df_vtas[dict_cols["cols_ventas"]["agente_comercial"]] = verificador.create_col_agente_resuelta(driver_val="nombre", fallback="nombre")
        
        mask_corregir_status = (
            (df_vtas[dict_cols["cols_ventas"]["tipo_venta"]] == "I") &
            (df_vtas[dict_cols["cols_ventas"]["agente_comercial"]] == "Sin asignar") &
            (df_vtas["status"] == "SIN COD AC CORREGIDO")
        )

        df_vtas.loc[mask_corregir_status, "status"] = "SIN COD AC"

        
        # Exportar resultado
        out_dir = ensure_dir(base_dir=cfg_result["path_resultado"])
        out_path = os.path.join(out_dir, "homologación_vtas.xlsx")
        logger.info(f"Exportando resultado → {out_path}")
        df_vtas.to_excel(out_path, index=False)
        

if __name__ == "__main__":
    app = Aplicacion()
    app.main()
