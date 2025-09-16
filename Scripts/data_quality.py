# Modulo para hacer verificaciones de calidad de data previo a correr los procesos.
from Utils.DataQuality_Functions import ensure_dir, resolve_existing_file
from loguru import logger

def DataQualityTest(config_insumos: dict) -> dict:
    """Método orquestadore Test básico de dataQuality sobre archivos de una ruta especificada

    Args:
        config (dict): Diccionario que tiene la configuración del proyecto necesario para hacer comprobaciones.
    Returns:
        dict: Dict_Validaciones.
    """
    # Verificar existencia de directorios y archivos.
    path_insumos = ensure_dir(base_dir=config_insumos["path_insumos"])
    
    path_vtas = resolve_existing_file(base_dir=path_insumos, filename=config_insumos["base_vtas"]["nom_base"])
    config_insumos["path_insumos"]

    # Verificar existencia de archivos de insumos.
    Contador = 0


def DataQuality_columns(config : dict) -> dict:
    """
    """
    pass