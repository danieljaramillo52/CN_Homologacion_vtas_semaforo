from typing import Dict, Any
import Utils.general_functions as gf

class ProcesarInsumos:
    def __init__(self, config_insumos: Dict[str, Any], dict_cols: Dict[str, Any], config_msg: Dict[str, Any] | None = None):
        """
        Controlador para cargar y validar insumos de ventas y drivers.

        Args:
            config_insumos (Dict[str, Any]): Configuración de rutas, nombres de archivo y hojas de Excel.
            dict_cols (Dict[str, Any]): Diccionario global con las definiciones de columnas.
            config_msg (Dict[str, Any] | None): Configuración opcional de mensajes de UI.
        """
        self.config_insumos = config_insumos
        self.cnf_cols = dict_cols
        self.cnf_msg = config_msg

        # Guardar nombres de hoja para evitar redundancia
        self.hoja_vtas = self.config_insumos["base_vtas"]["nom_hoja"]
        self.hoja_drv = self.config_insumos["drivers"]["nom_hoja"]

    def _carga(self, *, path: str, hoja: str, modo_pruebas: bool, cols: list | dict | None):
        """
        Carga genérica de un archivo Excel.

        Args:
            path (str): Ruta del archivo Excel.
            hoja (str): Nombre de la hoja a leer.
            modo_pruebas (bool): Si True, realiza una carga mínima para verificación.
            cols (list | dict | None): Columnas a verificar o seleccionar. Si None, carga todas.

        Returns:
            DataFrame: DataFrame con los datos cargados desde el archivo.
        """
        return gf.Lectura_insumos_excel(
            path_insumo=path,
            nom_hoja=hoja,
            modo_pruebas=modo_pruebas,
            cols_verificados=cols
        )

    def carga_minima(self, path_vtas: str, path_drivers: str):
        """
        Carga mínima de las bases de ventas y drivers (modo de prueba).

        Args:
            path_vtas (str): Ruta del archivo Excel de ventas.
            path_drivers (str): Ruta del archivo Excel de drivers.

        Returns:
            tuple: (DataFrame de ventas, DataFrame de drivers) con un subconjunto reducido de datos.
        """
        df_vtas = self._carga(path=path_vtas, hoja=self.hoja_vtas, modo_pruebas=True, cols=None)
        df_drivers = self._carga(path=path_drivers, hoja=self.hoja_drv, modo_pruebas=True, cols=None)
        return df_vtas, df_drivers

    def carga_completa(self, path_vtas: str, path_drivers: str):
        """
        Carga completa de las bases de ventas y drivers.

        Args:
            path_vtas (str): Ruta del archivo Excel de ventas.
            path_drivers (str): Ruta del archivo Excel de drivers.

        Returns:
            tuple: (DataFrame de ventas, DataFrame de drivers).  
            - Ventas se carga con las columnas definidas en la configuración.  
            - Drivers se carga completo sin filtro de columnas.
        """
        cols_vtas = self.config_insumos["base_vtas"]["cols_vtas"]

        df_vtas = self._carga(path=path_vtas, hoja=self.hoja_vtas, modo_pruebas=False, cols=cols_vtas)
        df_drivers = self._carga(path=path_drivers, hoja=self.hoja_drv, modo_pruebas=False, cols=None)

        return df_vtas, df_drivers
