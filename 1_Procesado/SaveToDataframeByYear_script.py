import argparse
import glob

import sys
sys.path.append('./lib')
from utils import openTarFile
from utils import TranslateMachine

from pyspark.sql import Row
from pyspark.rdd import RDD
from pyspark.sql import SparkSession


def writeDf (data, mode, savepath, year):
        df = spark.createDataFrame(data)
        df.write.mode(mode).parquet ( savepath + '/' +  str(year) ) 



def readYear (year, path, format, savepath):
    #path ='///export/data_ml4ds/IntelComp/Datasets/ted/xmls/'
    filePattern = path + '*' + str(year) + '*.tar.gz'

    totalResult = []
    

    #Procesamos todos los formularios que empiecen por F
    validForm = ['F']

    #la primera vez machacamos el contenido, luego ya lo pasamos a append.
    mode = 'overwrite'
    for file in glob.glob( filePattern ):        
        print (file)
        dataFile = openTarFile (file)
        tm = TranslateMachine ( format )
        print ('Filtrando formularios, en el original hay %s datos' % len(dataFile))
        dataFile = list(filter (lambda d: list (d['TED_EXPORT']['FORM_SECTION'].keys())[0][:1] in validForm, dataFile))
        print ('nos quedamos con %s datos' % len (dataFile))
        print ('convirtiendo datos')
        totalResult = [tm.translateKeys (data) for data in dataFile]
        print ('Grabando datos %s' % mode)
        writeDf (totalResult, mode, savepath, year)
        mode = 'append'


    #return totalResult





if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--list", nargs="+", default=[2018,2019,2020,2021,2022])
    parser.add_argument ('-r','--readpath', help='ruta de lectura de los xml, ruta local, no hdfs', required=True)
    parser.add_argument ('-s','--savepath', help='ruta donde salvar el dataframe, ruta de hdfs', required=True)
    parser.add_argument ('-f','--format', help='el formato que queremos salvar, sólo campos Place <place>, sólo campos ted, <ted>, o las dos cosas, <all>',
                         required=True, choices=['all', 'ted', 'place'])

    value = parser.parse_args()

    spark = SparkSession\
       .builder\
       .appName("PythonSort")\
       .getOrCreate()

    sc = spark.sparkContext

    print(sc.version)

    for year in value.list:
        print ('vamos con %s' % year)
        #totalResult = readYear (year, value.readpath, value.format, value.savepath)
        readYear (year, value.readpath, value.format, value.savepath)
        #df = spark.createDataFrame(totalResult)
        #df.write.mode("overwrite").parquet ( value.savepath + '/' +  str(year) ) 
         

