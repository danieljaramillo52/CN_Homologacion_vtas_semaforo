# Funciones generales del proyecto
from loguru import logger
from typing import Optional
from pathlib import Path
import yaml
import pandas as pd 
import time

def Registro_tiempo(original_func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = original_func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        logger.info(
            f"Tiempo de ejecución de {original_func.__name__}: {execution_time} segundos"
        )
        return result

    return wrapper

def procesar_configuracion(nom_archivo_configuracion: str) -> dict:
    """Lee un archivo YAML de configuración para un proyecto.

    Args:
        nom_archivo_configuracion (str): Nombre del archivo YAML que contiene
            la configuración del proyecto.

    Returns:
        dict: Un diccionario con la información de configuración leída del archivo YAML.
    """
    try:
        with open(nom_archivo_configuracion, "r", encoding="utf-8") as archivo:
            configuracion_yaml = yaml.safe_load(archivo)
        logger.success("Proceso de obtención de configuración satisfactorio")
    except Exception as e:
        logger.critical(f"Proceso de lectura de configuración fallido {e}")
        raise e

    return configuracion_yaml



@Registro_tiempo
def Lectura_insumos_excel(
    path_insumo: str, nom_hoja: str , engine = "openpyxl", cols_verificados: Optional[list[str]] = None, modo_pruebas=False 
) -> pd.DataFrame:
    """
    Lee datos desde un archivo Excel en una hoja específica, con soporte para modo de pruebas.

    - En **modo normal** (`modo_pruebas=False`): carga todas las filas y todas las columnas.
    - En **modo pruebas** (`modo_pruebas=True`): carga solo 2 filas y restringe la lectura
      a las columnas indicadas en `cols_verificados`.

    Args:
        path_insumo (str): Ruta absoluta al archivo Excel (incluye extensión).
        nom_hoja (str): Nombre de la hoja a leer.
        engine (str, opcional): Motor de lectura de pandas (`openpyxl` por defecto).
        cols_verificados (list[str], opcional): Columnas esperadas a leer en modo pruebas.
            Si `modo_pruebas=False`, este argumento se ignora.
        modo_pruebas (bool, opcional): Si es `True`, carga mínima de datos
            (2 filas + columnas verificadas). Por defecto es `False`.

    Returns:
        pd.DataFrame: DataFrame con los datos leídos de la hoja seleccionada.

    Raises:
        Exception: Si ocurre un error durante la lectura del archivo.
    """
    base_leida = None
    archivo = Path(path_insumo).name
    try:
        
        nrows = 2 if modo_pruebas else None
        usecols = cols_verificados if modo_pruebas else None
        
        logger.info(f"Inicio lectura {archivo} Hoja: {nom_hoja}")
        base_leida = pd.read_excel(
            path_insumo,
            sheet_name=nom_hoja,
            dtype=str,
            engine=engine,
            nrows=nrows,
            usecols=usecols
            
        )

        logger.success(
            f"Lectura de {archivo},  hoja: {nom_hoja} realizada con éxito"
        )  # Se registrará correctamente con el método "success"
    except Exception as e:
        logger.error(f"Proceso de lectura fallido: {e}")
        raise Exception

    return base_leida


def Lectura_simple_excel(path: str, nom_insumo: str, nom_hoja=None) -> pd.DataFrame:
    """Lee archivos de Excel con cualquier extensión y carga los datos de una hoja específica.

    Lee el archivo especificado por `nom_insumo` ubicado en la ruta `path` y carga los datos de la hoja
    especificada por `nom_Hoja`. Selecciona solo las columnas indicadas por `cols`.

    Args:
        path (str): Ruta de la carpeta donde se encuentra el archivo.
        nom_insumo (str): Nombre del archivo con extensión.
        nom_Hoja (str): Nombre de la hoja del archivo que se quiere leer.
        

    Returns:
        Si nom_hoja != None 
            pd.DataFrame: Dataframe que contiene los datos leídos del archivo Excel.
        Si nom_hoja == None
            dict ({sheets : pd.Dataframe}) : Diccionario cuyas claves son los nombres de las hojas del archivo .xlsx , y los valores son de tipo pd.Dataframe. 
    Raises:
        Exception: Si ocurre un error durante el proceso de lectura del archivo.
    """
    base_leida = None

    try:
        logger.info(f"Inicio lectura {nom_insumo}")
        if nom_hoja == None:
            base_leida = pd.read_excel(
                path + nom_insumo, dtype=str, sheet_name=nom_hoja
            )
        else:
            base_leida = pd.read_excel(
                path + nom_insumo, dtype=str, 
            )

        logger.success(
            f"Lectura de {nom_insumo} realizada con éxito"
        )  # Se registrará correctamente con el método "success"
    except Exception as e:
        logger.error(f"Proceso de lectura fallido: {e}")
        raise Exception

    return base_leida