
from typing import Dict, Any
import  Utils.general_functions as gf

class ProcesarInsumos:
    def __init__(self, config_insumos: Dict[str, Any],dict_cols : Dict[str, Any] ,config_msg: Dict[str, Any] | None = None ):
        """
        Inicializa el controlador de insumos.

        Args:
            config_insumos: Configuración de rutas y archivos (dict).
            config_msg: Configuración de mensajes de UI (dict).
        """
        self.config_insumos = config_insumos
        self.cnf_msg = config_msg
        self.cnf_cols = dict_cols
        

    def _carga_generica(self, path_insumo: str, modo_pruebas: bool = False, cols: list | None = None ):
        """
        Método genérico para cargar insumos.
        """
        cfg_vtas = self.config_insumos["base_vtas"]
        cfg_drivers = self.config_insumos["drivers"]

        df_vtas = gf.Lectura_insumos_excel(
            path_insumo=path_insumo,
            nom_hoja=cfg_vtas["nom_hoja_vtas"],
            modo_pruebas=modo_pruebas,
            cols_verificados=cols
        )

        df_drivers = gf.Lectura_insumos_excel(
            path_insumo=path_insumo,
            nom_hoja=cfg_drivers["nom_hoja_driver"],
            modo_pruebas=modo_pruebas,
            cols_verificados=cols
        )

        return df_vtas, df_drivers

    def carga_minima(self, path_insumo: str):
        """
        Carga mínima (modo_pruebas=True).
        """
        return self._carga_generica(path_insumo, modo_pruebas=True)

    def carga_completa(self, path_insumo: str):
        """
        Carga completa (modo_pruebas=False).
        """
        return self._carga_generica(path_insumo, 
        cols=self.config_insumos["base_vtas"]["cols_vtas"])
        
    