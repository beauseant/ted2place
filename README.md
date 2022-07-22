# ted2place
Volcado de ficheros de contratación europea, TED y conversión a contratación española Place

Las licitaciones correspondientes a la contratación Europea se pueden encontrar en: https://ted.europa.eu/TED/browse/browseByMap.do

Para realizar el procesado de la información que aparece en ese portal, han puesto a disposición de todo el mundo un conjunto de ficheros comprimidos que contienen un archivo XML por cada licitación.

Esos ficheros comprimidos tienen una periocidad mensual.

Por lo tanto, el procesado de la información se compone de dos procesos que se ejecutan por separado para que tenga más flexibilidad el sistema.


## Descarga de la información:

Una vez al mes (o cuando se considere necesario) se descarga la información del mes, o meses anteriores, para ello se ha creado un script  en python que recomendamos añadir al cron de la siguiente manera:

#### En el cron:

    *el dia 10 de cada mes se descargan los datos del mes anterior:*
    0 0 10 * * lanzar.sh

#### El contenido del lanzar.sh puede tener el siguiente aspecto:

    DAY=$(date -d "$D" '+%d')
    MONTH=$(date -d "$D" '+%m') 
    PREVMONTH=$(($MONTH-1))   
    YEAR=$(date -d "$D" '+%Y')

    echo "Day: $DAY"
    echo "Month: $MONTH"
    echo "Previous: $PREVMONTH"
    echo "Year: $YEAR"

    /bin/python saveXML.py -y $YEAR -m $PREVMONTH -p RUTA_SALVAR

    Donde RUTA_SALVAR es el directorio donde se quiera volcar la información.
    
    

## Procesado de la información:

El segundo proceso se encarga de procesar la información descargada en el punto anterior y generar un dataframe en formato parquet que se graba en el HDFS. De esta forma se puede procesar con Spark los datos.

La idea de este punto es grabar la información por años, aunque se ajunta un código de ejemplo sobre cómo se pueden juntar todos los años en un único dataframe.

El script de python que se encarga de este punto necesita los siguiente parámetros:

       -h, --help            show this help message and exit
     --list LIST [LIST ...]
     -r READPATH, --readpath READPATH
                        ruta de lectura de los xml, ruta local, no hdfs
     -s SAVEPATH, --savepath SAVEPATH
                        ruta donde salvar el dataframe, ruta de hdfs
     -f {all,ted,place}, --format {all,ted,place}
                        el formato que queremos salvar, sólo campos Place
                        <place>, sólo campos ted, <ted>, o las dos cosas,
                        <all>

+ **--list** Parámetro opcional, la lista de años a descargar, por defecto toma 2018 2019 2020 2021 2022
+ **-r** Parámetro obligatorio, la ruta desde la que se leen los ficheros mensuales comprimidos, lo que en el punto anterior se ha llamado **RUTA_SALVAR** Al ser una ruta local debe ponerse triple barra, por ejemplo ///Datos/Descargas/Ted/
+ **-s** Parámetro obligatorio La ruta dónde salvar el dataframe, al ser una ruta de HDFS se usa una barra sencilla, por ejemplo, /HDFS/Dataframe/Ted
+ **--list** Parámetro obligatorio, el formato en que queremos salvar:
    + place: Sólo la conversión de los datos de Ted a Place, además de un conjunto de identificadores para poder enlazar.
    + ted: Sólo datos de ted, sin ninguna conversión.
    + all: Los datos de ted y adjuntos los datos convertidos.

La mejor opción, en nuestra opinión, es ejecutar primero con place y luego con ted. De esta forma tenemos dos conjuntos de datos independintes con los datos en bruto y otro con los datos procesados. Si hace falta unirlos puede hacerse a través del identificador.

Para más información sobre la estructura de datos generada revise el fichero de Estructura de datos.

















