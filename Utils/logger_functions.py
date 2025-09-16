import os
from loguru import logger

def setup_logging():
    """Configura el logger para guardar los registros en un archivo externo.

    Esta función configura el logger de Loguru para guardar los registros en un archivo
    llamado 'logs.log' dentro de la carpeta 'logs_folder'. Si la carpeta no existe, se
    creará automáticamente.

    Returns:
        logger: El objeto logger configurado para registrar en el archivo externo.
    """
    # Obtén la ruta absoluta de la carpeta logs_folder en el directorio principal
    logs_folder = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs_folder")
    os.makedirs(logs_folder, exist_ok=True)  # Crea la carpeta si no existe

    # Configuración del manejador para guardar en un archivo externo en la carpeta logs_folder
    log_file = os.path.join(logs_folder, "logs.log")

    # Configura el logger para que solo guarde los registros de la sesión actual y sobrescriba el archivo en cada ejecución
    # El parámetro "rotation" se establece en "500 MB" para rotar el archivo cuando alcance 500 MB de tamaño.
    # El parámetro "mode" se establece en "w" para sobrescribir el archivo en cada ejecución.
    logger.add(log_file, rotation="500 MB", mode="w")

    return logger



