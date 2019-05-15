from google.cloud import storage
import patoolib
import glob
import os, shutil
import hashlib
import mysql_process as msp
import datetime
import sys
import platform

dir = '/home/alex/preprod_img/'
bucket_landing = 'sobreviviencia-landing-32gfse4fvd7'
bucket_data_lake = 'sobrevivencia-data-lake-df75gf756fdbwe23cvbb678df43gnqf'
bucket_model = 'model-resuts-1j6jhb63sdb0'
# predio/indice/fecha_hora/files *.shp *.shpx *.csv *.etc

def download_images():
    client = storage.Client()
    bucket = bucket_landing
    bucket = client.get_bucket(bucket)
    prefix = None
    delimiter = None
    filelist = bucket.list_blobs(prefix = prefix, delimiter=delimiter)
    for f in filelist:
        bucket.blob(f.name).download_to_filename(dir + f.name)

def extract_files():
    for file in glob.glob(dir + '*'):
        patoolib.extract_archive(file, outdir=dir)
        os.remove(file)

def image_list():
    img_list = []
    dir_list = glob.glob(dir + '*')
    for img_dir in dir_list:
        for obj in glob.glob(img_dir + "/*"):
            img_list.append(obj)
    return img_list

def del_files():
    for folder in glob.glob(dir + '*'):
        shutil.rmtree(folder, ignore_errors=True)

def upload_file(dir, fileroute):
    shakey = str(hashlib.md5(os.urandom(32)).hexdigest()) + "." + fileroute.split("/")[-1].split(".")[1]
    destination_blob_name = shakey
    client = storage.Client()
    bucket = bucket_data_lake
    bucket = client.get_bucket(bucket)
    blob = bucket.blob(dir + "/" + destination_blob_name)
    blob.upload_from_filename(fileroute)
    print('File {} uploaded to {}.'.format(
        fileroute,
        destination_blob_name))
    return bucket_data_lake + "/" + dir + "/" + destination_blob_name

def download_filtered_images(local_dir, id_proceso, tipo_img, indice):
    filelist = msp.get_urllist(id_proceso, tipo_img, indice)
    bucket = filelist[0]['RUTA_RESULTADO'].split("/")[0]
    client = storage.Client()
    bucket = client.get_bucket(bucket)
    for f in filelist:
        ruta = f['RUTA_RESULTADO'].split("/")[-2] + "/" + f['RUTA_RESULTADO'].split("/")[-1]
        bucket.blob(ruta).download_to_filename(local_dir + ruta.split("/")[-1])
    return filelist

def upload_processed_files(tipo_img, filelist):
    try:
        msp.update_processed_img(tipo_img, filelist)
        print("Catalogo actualizado exitosamente")
    except:
        print("Problemas actualizando el catalogo")

def upload_model_files(dir_model_files):
    splitter = ""
    if platform.system() == "Linux":
        dir_model_files = [ dir_model_files + "/etiquetas/*", dir_model_files + "/grillas/*" , dir_model_files + "/predios/*", dir_model_files + "/rodales/*" ]
        splitter = "/"
    elif platform.system() == "Windows":
        dir_model_files = [ dir_model_files + "\\etiquetas\\*", dir_model_files + "\\grillas\\*" , dir_model_files + "\\predios\\*", dir_model_files + "\\rodales\\*" ]
        splitter = "\\"
    client = storage.Client()
    bucket = client.get_bucket(bucket_model)
    filelist = []
    for dr in dir_model_files:
        filelist.append(glob.glob(dr))
    last_id = msp.last_id()
    fecha = str(datetime.datetime.now())
    last_ps = msp.proc_ejecucion(2, fecha)
    for dirs in filelist:
        for f in dirs:
            arr = f.split(splitter)
            del arr[0]
            codigo = arr[-1].split("_")[0]
            index = arr[-1].split(".")[0].replace("_grilla", "").replace("_etiquetas", "").replace("_rodal", "")
            seccion = arr[-1].split("_")[1]
            especie = arr[-1].split("_")[2]
            apl = arr[-1].split("_")[3].split(".")[0].replace("_grilla", "").replace("_etiquetas", "").replace("_rodal", "")
            filename = arr[-1]
            ruta_destino = bucket_model + "/" + codigo + "/" + index + "/" + fecha + "/" + filename
            blob = bucket.blob(ruta_destino)
            blob.upload_from_filename(f)
            indices = [ index, codigo, "", seccion, especie, apl, 10, 2, ruta_destino, fecha ]
            last_id = msp.insert_catalog(indices, last_id, last_ps)