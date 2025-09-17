import pandas as pd
from typing import Dict, Literal
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
            df_vtas: DataFrame de ventas.
            df_drivers: DataFrame de drivers.
            cols_vtas: Diccionario con alias -> nombre real de columnas de ventas.
            cols_drivers: Diccionario con alias -> nombre real de columnas de drivers.
        """
        
        self.df_vtas = df_vtas
        self.df_drivers = df_drivers
        self.cols_vtas = cols_vtas
        self.cols_drivers = cols_drivers
        self.mapa_drivers = self._crear_mapa()

    def _crear_mapa(self) -> Dict[str, str]:
        """Mapea Cod SAP -> Cambio Cod ECOM a CRM"""
        
        key = self.cols_drivers["cod_sap"]                 
        val = self.cols_drivers["cambio_cod_ecom_crm"]     
        
        return dict(zip(self.df_drivers[key], self.df_drivers[val]))

    def create_col_status(self) -> pd.Series:
        """
        - Si Tipo de Venta ≠ "I" → "OK".
        - Si Tipo de Venta = "I" y Agente Comercial - Clave = "#" → "SIN COD AC".
        - Si Tipo de Venta = "I" y Agente Comercial - Clave ≠ "#" → BUSCARV en drivers; si no existe → "OK".
        """
        col_tipo = self.cols_vtas["tipo_venta"]                
        col_g    = self.cols_vtas["agente_comercial_clave"]    

       
        serie_status = pd.Series(self.RESULTADO_OK, index=self.df_vtas.index)

        mask_tipo_I = self.df_vtas[col_tipo] == self.TIPO_ATENCION      
        mask_hash   = self.df_vtas[col_g]   == self.COD_HASH   
        mask_lookup = mask_tipo_I & (~mask_hash)                            

        
        serie_status.loc[mask_tipo_I & mask_hash] = self.RESULTADO_SIN_COD_CORREGIDO        
   
        serie_status.loc[mask_lookup] = (
            self.df_vtas.loc[mask_lookup, col_g]
            .map(self.mapa_drivers)       
            .fillna(self.RESULTADO_OK)
        )

        return serie_status



    def create_col_cod_cliente_alt(self, literal_if_false: bool = False) -> pd.Series:
        """
        Calcula (vectorizado) un código a partir de ventas y drivers.

        Reglas:
        - Si tipo_venta == self.TIPO_ATENCION: mapear cliente_clave usando drivers
        (cod_actual -> cod_cliente_alt); si no hay match, usar codigo_ecom.
        - Si no: devolver False si literal_if_false=True, de lo contrario codigo_ecom.

        Parámetros:
        - literal_if_false (bool): controla el valor cuando no aplica la regla.

        Retorna:
        - pd.Series alineada a df_vtas.
        """

        # Columnas reales desde la config
        col_tipo   = self.cols_vtas["tipo_venta"]       
        col_key_D  = self.cols_vtas["cliente_clave"]     
        codigo_ecom      = self.cols_vtas["codigo_ecom"]       

        cod_actual  = self.cols_drivers["cod_actual"]     
        cod_cliente_alt  = self.cols_drivers["cod_cliente_alt"]

        mapa_hk = dict(zip(self.df_drivers[cod_actual], self.df_drivers[cod_cliente_alt]))

        
        res = (pd.Series(False, index=self.df_vtas.index)
            if literal_if_false else self.df_vtas[codigo_ecom].copy())

        
        mask_tipo_I = (self.df_vtas[col_tipo] == self.TIPO_ATENCION)  

        res.loc[mask_tipo_I] = (
            self.df_vtas.loc[mask_tipo_I, col_key_D]
            .map(mapa_hk)                                  
            .fillna(self.df_vtas.loc[mask_tipo_I, codigo_ecom])  
        )

        return res
    

    def create_col_agente_resuelta(
        self,
        driver_val: Literal["cod", "nombre"] = "cod",
        fallback:   Literal["clave", "nombre"] = "clave",
    ) -> pd.Series:
        """
        Devuelve una serie calculada:
        - Por defecto: columna de agente (clave o nombre) según `fallback`.
        - Si tipo_venta == TIPO_ATENCION y agente_comercial == "Sin asignar":
            hace lookup por cliente_clave en drivers (cod_actual -> cod/jefe según `driver_val`);
            si no hay match, conserva el fallback.

        Args:
        driver_val: "cod"    → usa drivers["cod_jefe_ventas"] 
                    "nombre" → usa drivers["jefe_ventas"] 
        fallback:   "clave"  → usa ventas["agente_comercial_clave"] 
                    "nombre" → usa ventas["agente_comercial"] 
        """
        # columnas ventas (reales desde config)
        col_tipo          = self.cols_vtas["tipo_venta"]
        col_agente_nombre = self.cols_vtas["agente_comercial"]
        col_agente_clave  = self.cols_vtas["agente_comercial_clave"]
        col_cliente_clave = self.cols_vtas["cliente_clave"]

        
        drv_key = self.cols_drivers["cod_actual"]
        
        if driver_val == "cod":
            drv_val = self.cols_drivers["cod_jefe_ventas"]  
        else:
            drv_val = self.cols_drivers["jefe_ventas"]   
        
        # fallback en ventas
        fb_col = col_agente_clave if fallback == "clave" else col_agente_nombre

        # valor por defecto
        res = self.df_vtas[fb_col].copy()

        # condición: tipo_venta == "I" y agente_comercial == "Sin asignar"
        mask = (self.df_vtas[col_tipo] == self.TIPO_ATENCION) & \
            (self.df_vtas[col_agente_nombre] == self.SIN_ASIGNAR)

        mapa = dict(zip(self.df_drivers[drv_key], self.df_drivers[drv_val]))

        res.loc[mask] = (
            self.df_vtas.loc[mask, col_cliente_clave]
            .map(mapa)
            .fillna(self.df_vtas.loc[mask, fb_col])
        )
        return res

