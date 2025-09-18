import pandas as pd
from typing import Dict, Literal
from types import SimpleNamespace
from loguru import logger


class VerificadorCodigos:
    """
    Valida y aplica la lógica de asignación de códigos entre las bases de ventas y drivers.
    Replica la lógica de la fórmula Excel de manera vectorizada.
    """
    TIPO_ATENCION = "I"
    COD_HASH = "#"
    RESULTADO_OK = "OK"
    RESULTADO_SIN_COD_CORREGIDO = "SIN COD AC CORREGIDO"
    RESULTADO_SIN_COD_AC = "SIN COD AC"
    SIN_ASIGNAR = "Sin asignar"
    
    def __init__(
        self,
        df_vtas: pd.DataFrame,
        df_drivers: pd.DataFrame,
        cols_vtas: Dict[str, str],
        cols_drivers: Dict[str, str],
    ):
        """
        Inicializa el verificador con los DataFrames y sus mapeos de columnas.

        Args:
            df_vtas (pd.DataFrame): DataFrame de ventas.
            df_drivers (pd.DataFrame): DataFrame de drivers.
            cols_vtas (Dict[str, str]): Alias -> nombre real de columnas de ventas.
            cols_drivers (Dict[str, str]): Alias -> nombre real de columnas de drivers.
        """
        self.df_vtas = df_vtas
        self.df_drivers = df_drivers
        self.cols_vtas = cols_vtas
        self.cols_drivers = cols_drivers

        # Accesores por punto (azúcar sintáctico). No cambia la lógica de los diccionarios.
        self.V = SimpleNamespace(**cols_vtas)
        self.D = SimpleNamespace(**cols_drivers)

        # Mapa usado por create_col_status (Cod SAP -> Cambio Cod ECOM a CRM)
        self.mapa_drivers = self._crear_mapa()

    def _crear_mapa(self) -> Dict[str, str]:
        """Mapea Cod SAP -> Cambio Cod ECOM a CRM."""
        return dict(zip(
            self.df_drivers[self.D.cod_sap],
            self.df_drivers[self.D.cambio_cod_ecom_crm]
        ))

    def create_col_status(self) -> pd.Series:
        """
        - Si Tipo de Venta ≠ "I" → "OK".
        - Si Tipo de Venta = "I" y Agente Comercial - Clave = "#" → "SIN COD AC CORREGIDO".
        - Si Tipo de Venta = "I" y Agente Comercial - Clave ≠ "#" → lookup en drivers; si no existe → "OK".

        Returns:
            pd.Series: Serie 'status' calculada.
        """
        serie_status = pd.Series(self.RESULTADO_OK, index=self.df_vtas.index)

        mask_tipo_I = self.df_vtas[self.V.tipo_venta] == self.TIPO_ATENCION
        mask_hash   = self.df_vtas[self.V.agente_comercial_clave] == self.COD_HASH
        mask_lookup = mask_tipo_I & (~mask_hash)

        serie_status.loc[mask_tipo_I & mask_hash] = self.RESULTADO_SIN_COD_CORREGIDO
        serie_status.loc[mask_lookup] = (
            self.df_vtas.loc[mask_lookup, self.V.agente_comercial_clave]
            .map(self.mapa_drivers)
            .fillna(self.RESULTADO_OK)
        )
        return serie_status

    def create_col_cod_cliente_alt(self, literal_if_false: bool = False) -> pd.Series:
        """
        Calcula código alterno:
        - Si tipo_venta == TIPO_ATENCION → mapear cliente_clave (cod_actual → cod_cliente_alt),
          si no hay match → codigo_ecom.
        - Si no es TIPO_ATENCION → devuelve False si literal_if_false=True, de lo contrario codigo_ecom.

        Args:
            literal_if_false (bool): Controla el valor cuando no aplica la regla.

        Returns:
            pd.Series: Serie con el código calculado.
        """
        res = (pd.Series(False, index=self.df_vtas.index)
               if literal_if_false else self.df_vtas[self.V.codigo_ecom].copy())

        mapa_hk = dict(zip(
            self.df_drivers[self.D.cod_actual],
            self.df_drivers[self.D.cod_cliente_alt]
        ))
        mask_tipo_I = (self.df_vtas[self.V.tipo_venta] == self.TIPO_ATENCION)

        res.loc[mask_tipo_I] = (
            self.df_vtas.loc[mask_tipo_I, self.V.cliente_clave]
            .map(mapa_hk)
            .fillna(self.df_vtas.loc[mask_tipo_I, self.V.codigo_ecom])
        )
        return res

    def create_col_agente_resuelta(
        self,
        driver_val: Literal["cod", "nombre"] = "cod",
        fallback:   Literal["clave", "nombre"] = "clave",
    ) -> pd.Series:
        """
        Devuelve el agente resuelto:
        - Por defecto: usa ventas[agente_comercial_clave|agente_comercial] según `fallback`.
        - Si tipo_venta == TIPO_ATENCION y agente_comercial == "Sin asignar":
          hace lookup por cliente_clave (cod_actual → cod_jefe_ventas/jefe_ventas).

        Args:
            driver_val (Literal["cod","nombre"]): Valor de drivers a usar.
            fallback (Literal["clave","nombre"]): Columna de ventas por defecto.

        Returns:
            pd.Series: Serie con el agente resuelto.
        """
        fb_col = self.V.agente_comercial_clave if fallback == "clave" else self.V.agente_comercial
        res = self.df_vtas[fb_col].copy()

        mask = (
            (self.df_vtas[self.V.tipo_venta] == self.TIPO_ATENCION) &
            (self.df_vtas[self.V.agente_comercial] == self.SIN_ASIGNAR)
        )

        drv_val = self.D.cod_jefe_ventas if driver_val == "cod" else self.D.jefe_ventas
        mapa = dict(zip(self.df_drivers[self.D.cod_actual], self.df_drivers[drv_val]))

        res.loc[mask] = (
            self.df_vtas.loc[mask, self.V.cliente_clave]
            .map(mapa)
            .fillna(self.df_vtas.loc[mask, fb_col])
        )
        return res

    def corregir_status_sin_cod_ac(self, status_col: str = "status") -> pd.Series:
        """
        Si tipo_venta == TIPO_ATENCION, agente_comercial == SIN_ASIGNAR y
        status == RESULTADO_SIN_COD_CORREGIDO, entonces status ← RESULTADO_SIN_COD_AC.

        Args:
            status_col (str): Nombre de la columna de estado a actualizar.

        Returns:
            pd.Series: Serie 'status' actualizada.
        """
        mask = (
            (self.df_vtas[self.V.tipo_venta] == self.TIPO_ATENCION) &
            (self.df_vtas[self.V.agente_comercial] == self.SIN_ASIGNAR) &
            (self.df_vtas[status_col] == self.RESULTADO_SIN_COD_CORREGIDO)
        )
        self.df_vtas.loc[mask, status_col] = self.RESULTADO_SIN_COD_AC
        return self.df_vtas[status_col]



