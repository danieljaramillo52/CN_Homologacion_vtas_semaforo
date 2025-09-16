from __future__ import annotations
from loguru import logger
import logging
import sys
from functools import wraps
from pathlib import Path
import pandas as pd



def manejar_excepciones(func):
    """
    Decorador minimalista para capturar errores comunes de sistema de archivos,
    registrarlos en CRITICAL con traza y finalizar el proceso con código 1.

    Args:
        func: Función objetivo a envolver.

    Returns:
        wrapper: Función envuelta con manejo de excepciones y salida controlada.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)

        except NotADirectoryError as e:
            logger.opt(exception=True).critical(f"Directorio inválido: {e}")
            sys.exit(1)
        except FileNotFoundError as e:
            logger.opt(exception=True).critical(f"Archivo no encontrado: {e}")
            sys.exit(1)
        except PermissionError as e:
            logger.opt(exception=True).critical(f"Permiso denegado: {e}")
            sys.exit(1)
        except OSError as e:
            logger.opt(exception=True).critical(f"Error de E/S del sistema: {e}")
            sys.exit(1)
        except Exception as e:
            logger.opt(exception=True).critical(f"Error inesperado: {e}")
            sys.exit(1)
    return wrapper


@manejar_excepciones
def ensure_dir(base_dir: str | Path) -> Path:
    """
    Verifica que `base_dir` exista y sea un directorio y devuelve su ruta absoluta.

    Args:
        base_dir: Ruta del directorio a validar (relativa o absoluta).

    Returns:
        Path: Ruta absoluta del directorio validado.
    """
    base = Path(base_dir)
    if not base.is_dir():
        raise NotADirectoryError(base)
    return base.resolve()


@manejar_excepciones
def resolve_existing_file(base_dir: str | Path, filename: str) -> Path:
    """
    Verifica que `base_dir` sea un directorio y que `filename` exista dentro;
    devuelve la ruta absoluta del archivo.

    Args:
        base_dir: Directorio base donde se buscará el archivo.
        filename: Nombre del archivo (puede incluir subcarpetas relativas).

    Returns:
        Path: Ruta absoluta del archivo existente.
    """
    # Utilizamos resolve para trabajar con la ruta absoluta. 
    file_path = ((base_dir / filename).resolve())
    if not file_path.is_file():
        raise FileNotFoundError(f"{file_path.name} en {base_dir}")
    logger.info(f"Archivo {file_path.name} encontrado.")
    return file_path.resolve()


def verificar_columnas(df, columnas_esperadas: dict, nombre: str = "DataFrame"):
    """
    Verifica que el DataFrame contenga todas las columnas esperadas.

    Args:
        df (pd.DataFrame): DataFrame cargado.
        columnas_esperadas (dict): Diccionario {alias: nombre_columna_real}.
        nombre (str): Nombre descriptivo del DataFrame (para logs).

    Raises:
        ValueError: Si faltan columnas.
    """
    # En dict_cols los valores son los nombres reales
    esperadas = set(columnas_esperadas.values())
    presentes = set(df.columns)

    faltantes = esperadas - presentes

    if faltantes:
        logger.error(f"❌ {nombre}: faltan columnas {faltantes}")
        raise ValueError(f"Faltan columnas en {nombre}: {faltantes}")

    logger.success(f"✅ {nombre}: todas las columnas esperadas están presentes.")
