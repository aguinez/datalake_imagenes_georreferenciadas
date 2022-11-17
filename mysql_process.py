import pymysql
import datetime

def get_connection():
    connection = pymysql.connect(host='x',
                                user='x',
                                password='x',
                                db='x',
                                charset='x',
                                cursorclass=pymysql.cursors.DictCursor)
    return connection

def last_id():
    connection = get_connection()
    last_id = int()
    cursor = connection.cursor()
    cursor.execute("""SELECT `AUTO_INCREMENT`
    FROM  INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'CATALOG'
    AND   TABLE_NAME   = 'CATALOG';""")
    for row in cursor:
        last_id = row['AUTO_INCREMENT']
    cursor.close()
    connection.close()
    return last_id

#SET FOREIGN_KEY_CHECKS = 0; 
def proc_ejecucion(id_proceso, fecha):
    connection = get_connection()
    id_ejecucion = int()
    query = "INSERT INTO CATALOG.PROC_EJECUCION (ID_PROCESO, FECHA) VALUES ({}, '{}')"
    cursor = connection.cursor()
    cursor.execute(query.format(id_proceso, fecha))
    connection.commit()
    cursor.execute("""SELECT `AUTO_INCREMENT`
    FROM  INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'CATALOG'
    AND   TABLE_NAME   = 'PROC_EJECUCION';""")
    for row in cursor:
        id_ejecucion = row['AUTO_INCREMENT']
    connection.close()
    cursor.close()
    return id_ejecucion


def insert_catalog(indices, last_id, last_ps):
    connection = get_connection()
    query1 = "INSERT INTO CATALOG.CATALOG (INDICE,CODIGO,NOMBRE_PREDIO,SECCION,ESPECIE,APL,ID_TIPO_IMG,ID_PROCESO,RUTA_RESULTADO,FECHA) VALUES ('{}', '{}', '{}', '{}', '{}', {}, {}, {}, '{}', '{}')"
    query2 = "INSERT INTO CATALOG.DETALLE_EJECUCION (ID_EJECUCION, ID_IMAGEN_FUENTE) VALUES ({}, {})"
    try:
        cursor = connection.cursor()
        query1 = query1.format(indices[0], indices[1], indices[2], indices[3], indices[4], indices[5], indices[6], indices[7], indices[8], indices[9])
        cursor.execute(query1)
        query2 = query2.format(last_ps, last_id)
        cursor.execute(query2)
        connection.commit()
        connection.close()
        last_id = last_id + 1
    except Exception as e:
        print(str(e))
    return last_id

# ESTA FUNCION OBTIENE LA LISTA DE RUTAS DE LAS IMAGENES DEL PROCESO X Y TIPO DE IMAGENES Y-Z PARA SU POSTERIOR DESCARGA
def get_urllist(id_proceso, tipo_img, indice):
    result = []
    connection = get_connection()
    query = """select C.ID, C.RUTA_RESULTADO FROM CATALOG.PROC_EJECUCION PE
    INNER JOIN CATALOG.DETALLE_EJECUCION DE
    ON PE.ID_EJECUCION = DE.ID_EJECUCION
    INNER JOIN CATALOG.CATALOG C
    ON DE.ID_IMAGEN_FUENTE = C.ID
    WHERE C.ID_TIPO_IMG IN ({})
    AND PE.ID_PROCESO = {}
    AND C.INDICE = '{}'""".format(tipo_img, id_proceso, indice)
    cursor = connection.cursor()
    cursor.execute(query)
    for row in cursor:
        result.append(row)
    cursor.close()
    connection.close()
    return result

def update_processed_img(new_img_kind, catalog_id):
    connection = get_connection()
    cursor = connection.cursor()
    for cid in catalog_id:
        query = "UPDATE CATALOG.CATALOG SET ID_TIPO_IMG = {} WHERE ID = {}".format(new_img_kind, cid['ID'])
        cursor.execute(query)
    connection.commit()
    cursor.close()
    connection.close()


#20006_09_PIRA_2018_rodal
#20006_09_PIRA_2018_rodal.shp
#20006_09_PIRA_2018_rodal.shx
#20006_09_PIRA_2018_rodal.dbf
#20006_09_PIRA_2018.tif
#20006_09_PIRA_2018_etiquetas.tif
#20006_09_PIRA_2018_grilla.shp
