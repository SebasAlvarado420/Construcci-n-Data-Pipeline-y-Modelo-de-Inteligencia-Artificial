import os
import zipfile
import logging
import pandas as pd
import json
import boto3

logging.basicConfig(filename="audit_log.txt", level=logging.INFO, 
                    format="%(asctime)s - %(levelname)s - %(message)s")

# -------------------------------------------------------------------
# Carga de Roles y Verificación de Permisos
# -------------------------------------------------------------------
def load_roles(config_file="config.json"):
    if not os.path.exists(config_file):
        logging.error(f"No se encontró el archivo de configuración {config_file}")
        raise FileNotFoundError(f"No se encontró {config_file}")
    with open(config_file, "r", encoding="utf-8") as f:
        return json.load(f)

def can_execute(role, operation):
    roles = load_roles()
    if role not in roles:
        logging.warning(f"Rol '{role}' no existe en config.json.")
        return False
    allowed_ops = roles[role].get("allowed_operations", [])
    return operation in allowed_ops

# -------------------------------------------------------------------
# Funciones del Pipeline
# -------------------------------------------------------------------
def extract_data(dataset="johnsmith88/heart-disease-dataset", output="data_original.csv"):
    logging.info(f"Descargando dataset '{dataset}' usando Kaggle API...")
    command = f"kaggle datasets download -d {dataset} -p ."
    os.system(command)
    
    zip_file = f"{dataset.split('/')[-1]}.zip"  
    if os.path.exists(zip_file):
        with zipfile.ZipFile(zip_file, 'r') as z:
            for file in z.namelist():
                if file.endswith(".csv"):
                    z.extract(file, path=".")
                    os.rename(file, output)
                    logging.info(f"Archivo {file} extraído y renombrado a {output}.")
                    break
        os.remove(zip_file)
    else:
        logging.error("Archivo zip no encontrado.")
        raise FileNotFoundError("Archivo zip no encontrado.")

def clean_data(input_file="data_original.csv", output_file="data.csv"):
    try:
        df = pd.read_csv(input_file)
    except pd.errors.ParserError:
        df = pd.read_csv(input_file, delimiter=";")
    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)
    df.to_csv(output_file, index=False)
    logging.info(f"Datos limpios guardados en {output_file}")

def export_for_ai(input_file="data.csv", final_file="data_analisis.csv"):
    df = pd.read_csv(input_file)
    df.to_csv(final_file, index=False)
    logging.info(f"Datos listos para IA exportados en {final_file}")

# -------------------------------------------------------------------
# Subir a AWS S3 con encriptación SSE-S3
# -------------------------------------------------------------------
def upload_to_s3(
    file_to_upload="data_analisis.csv",
    bucket_name="grupo-1-data-pipeline",
    object_key="data_analisis.csv"
):
    logging.info(f"Subiendo {file_to_upload} a s3://{bucket_name}/{object_key}")
    
    if not os.path.exists(file_to_upload):
        logging.error(f"El archivo local {file_to_upload} no existe.")
        return

    s3_client = boto3.client('s3')
    try:
        with open(file_to_upload, 'rb') as f:
            s3_client.put_object(
                Bucket=bucket_name,
                Key=object_key,
                Body=f,
                ServerSideEncryption='AES256'
            )
        logging.info(f"Archivo subido con éxito a s3://{bucket_name}/{object_key}")
    except Exception as e:
        logging.exception(f"Error subiendo a S3: {e}")
        raise

# -------------------------------------------------------------------
# Pipeline Principal con Roles
# -------------------------------------------------------------------
def run_pipeline(role="admin"):
    # Extraer datos
    if can_execute(role, "extract_data"):
        if not os.path.exists("data_original.csv"):
            extract_data()
        else:
            logging.info("data_original.csv ya existe, se omite extracción.")
    else:
        logging.warning(f"El rol '{role}' no tiene permiso para extract_data.")
    
    # Limpiar
    if can_execute(role, "clean_data"):
        clean_data()
    else:
        logging.warning(f"El rol '{role}' no tiene permiso para clean_data.")
    
    # Exportar para IA
    if can_execute(role, "export_for_ai"):
        export_for_ai()
    else:
        logging.warning(f"El rol '{role}' no tiene permiso para export_for_ai.")
    
    # Subir a S3
    if can_execute(role, "upload_to_s3"):
        upload_to_s3()
    else:
        logging.warning(f"El rol '{role}' no tiene permiso para upload_to_s3.")
    
    logging.info("Pipeline completo ejecutado con éxito.")

if __name__ == "__main__":
    run_pipeline(role="admin")
