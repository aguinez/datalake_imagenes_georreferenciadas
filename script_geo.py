from osgeo import gdal,ogr,osr
import fiona
from shapely.geometry import Point, Polygon, MultiPolygon, shape
from pyproj import Proj, transform
import json
import glob
from shapely.geometry import Point, MultiPoint
from shapely.ops import nearest_points
from GPSPhoto import gpsphoto
import datetime
from google.cloud import storage
import download_list_images as dli
import mysql_process as msp
#dli.download_images()
#dli.extract_files()
#imglist = dli.image_list()
imglist = ['/home/alex/preprod_img/Fotos_25012019/DJI_0164.JPG']

def GetExtent(gt,cols,rows):
    ext=[]
    xarr=[0,cols]
    yarr=[0,rows]
    for px in xarr:
        for py in yarr:
            x=gt[0]+(px*gt[1])+(py*gt[2])
            y=gt[3]+(px*gt[4])+(py*gt[5])
            ext.append([x,y])
            print(x,y)
        yarr.reverse()
    return ext


def ReprojectCoords(coords,src_srs,tgt_srs):
    trans_coords=[]
    transform = osr.CoordinateTransformation( src_srs, tgt_srs)
    for x,y in coords:
        x,y,z = transform.TransformPoint(x,y)
        trans_coords.append([x,y])
    return trans_coords


def convertirCoordenadas(multipol):
    inProj = Proj(init='EPSG:32718', preserve_units=True)
    outProj = Proj(init='epsg:4326')
    transformado = []
    for poly in multipol:
        row = []
        deepness = set()
        properties = set()
        if len(poly['geometry']['coordinates'][0]) > 1:
            deepness = poly['geometry']['coordinates']
        else:
            deepness = poly['geometry']['coordinates'][0]
        properties = poly['properties']
        for points in deepness:
            for point in points:
                x,y = point[0], point[1]
                x2,y2 = transform(inProj,outProj,x,y)
                row.append((x2, y2))
        transformado.append({'coordinates':[row], 'properties':properties})
    return transformado


def rev_geoext(geo_ext):
    geo_corr = []
    for elem in geo_ext:
        geo_corr.append([elem[0], elem[1]])
    return geo_corr


def unique_places(multipol):
    predios = []
    used = set()
    for v in multipol:
        predios.append(str(v['properties']['CODIGO']) + "_" + str(v['properties']['SECCION']) + "_" + str(v['properties']['RODAL']) + "_" + str(v['properties']['TIPOUSO']) + "_" + str(v['properties']['APL']))
    unique = [x for x in predios if x not in used and (used.add(x) or True)]
    return unique


def get_predio_centroide(centroid, shp_lp_transformado, coors, allcoor):
    predio = ''
    for poly in shp_lp_transformado:
        try:
            if Polygon(poly['coordinates'][0]).contains(centroid) == True:
                ID = poly['properties']['ID_PREDIO']
                nombre = poly['properties']['NOMBRE']
        except:
            continue
    if predio != '':
        predio = [ nombre, glob.glob('/home/alex/shapefile_transformado/' + ID + '*.json') ]
    else:
        nearest = nearest_points(MultiPoint(coors), centroid)
        finder = str(nearest[0]).split("(")[1].split(")")[0].split(" ")
        finder = (float(finder[0]), float(finder[1]))
        for p in shp_lp_transformado:
            if finder in p['coordinates'][0]:
                ID = p['properties']['ID_PREDIO']
                nombre = p['properties']['NOMBRE']
                predio = [ nombre, glob.glob('/home/alex/shapefile_transformado/' + ID + '*.json') ]
        if predio == '':
            for p in allcoor:
                if nearest[0] == p['point']:
                    ID = p['predio'].split("_")[0]
                    nombre = p['predio'].split("_")[1]
                    predio = [ nombre, glob.glob('/home/alex/shapefile_transformado/' + ID + '*.json') ]
    return predio


## DEFINE PUNTO MAS CERCANO EN CADA JSON
def polygono_mascercano(centroid, shp):
    predio = ''
    mascercano = None
    nearest = None
    for poly in shp:
        for coor in poly['coordinates']:
            pol = MultiPoint(coor)
            if nearest is None:
                nearest = nearest_points(pol, centroid)
                mascercano = {'point':nearest[0], 'properties':poly['properties']}
            else:
                aux = nearest_points(pol, centroid)
                pol = MultiPoint([aux[0], nearest[0]])
                nearest = nearest_points(pol, centroid)
                if nearest[0] == aux[0]:
                    mascercano = {'point':nearest[0], 'properties':poly['properties']}
    return mascercano



## DEFINE PUNTO MAS CERCANO ENTRE LOS ARCHIVOS REVISADOS
def get_properties_mascercano(centroid, acum_mascercano):
    lista_cercanos = []
    result = set()
    for x in acum_mascercano:
        lista_cercanos.append(x['point'])
    nearest = nearest_points(MultiPoint(lista_cercanos), centroid)
    for p in acum_mascercano:
        if p['point'] == nearest[0]:
            result = {'properties':p['properties'], 'point':p['point']}
    return result


shp_limite_predial = "/home/alex/LIMITE_PREDIAL_201812/LIMITE_PREDIAL_201812.shp"
shp_lp_transformado = convertirCoordenadas(fiona.open(shp_limite_predial))

allcoor = []

for p in shp_lp_transformado:
    for tpl in p['coordinates'][0]:
        allcoor.append({'predio': p['properties']['ID_PREDIO'] + "_" + p['properties']['NOMBRE'],'point':Point(tpl[0], tpl[1])})

coors = []
for p in allcoor:
    coors.append(p['point'])

# PONER IMAGENES DE DIRECTORIO DE GS EN UNA LISTA Y LUEGO DESCARGAR E ITERAR POR ELEMENTO EN ESA LISTA
# -------
file = open("/home/alex/indices.csv",'a+')
file.write("IMAGEN,CENTROIDE,PREDIO,INDICE" + "\n")
file.close()
fecha = str(datetime.datetime.now())
id_proceso = int()
last_id = msp.last_id()
last_ps = msp.proc_ejecucion(0, fecha)

for img in imglist:
    try:
        clase = "BR/"
        tipo_img = 0
        coords = gpsphoto.getGPSData(img)
        centroid = Point(coords['Longitude'], coords['Latitude'])
    except:
        clase = "TIF/"
        tipo_img = 3
        raster=img
        ds=gdal.Open(raster)
        gt=ds.GetGeoTransform()
        cols = ds.RasterXSize
        rows = ds.RasterYSize
        ext=GetExtent(gt,cols,rows)
        src_srs=osr.SpatialReference()
        src_srs.ImportFromWkt(ds.GetProjection())
        tgt_srs=osr.SpatialReference()
        tgt_srs.ImportFromEPSG(4326)
        tgt_srs = src_srs.CloneGeogCS()
        geo_ext=ReprojectCoords(ext,src_srs,tgt_srs)
        geo_corr = rev_geoext(geo_ext)
        poly = Polygon(geo_corr)
        centroid = Polygon(geo_ext).centroid
    predio_files = get_predio_centroide(centroid, shp_lp_transformado, coors, allcoor)
    if predio_files != '':
        acum_mascercano = []
        for p in predio_files[1]:
            shp = json.load(open(p))
            acum_mascercano.append(polygono_mascercano(centroid, shp))
        mascercano = get_properties_mascercano(centroid, acum_mascercano)
        indice = str(mascercano['properties']['CODIGO']) + "_" + str(mascercano['properties']['SECCION']) + "_" + str(mascercano['properties']['TIPOUSO']) + "_" + str(mascercano['properties']['APL'])
    else:
        indice = "IMAGEN NO CLASIFICABLE"
        mascercano = dict()
        mascercano['point'] = ''
        print(img)
    download_url = dli.upload_file(clase + mascercano['properties']['CODIGO'], img)
    indices = [ indice, str(mascercano['properties']['CODIGO']), str(predio_files[0]), str(mascercano['properties']['SECCION']), str(mascercano['properties']['TIPOUSO']), str(mascercano['properties']['APL']), str(tipo_img), 0, download_url, str(datetime.datetime.now()) ]
    last_id = msp.insert_catalog(indices, last_id, last_ps)

#dli.del_files()