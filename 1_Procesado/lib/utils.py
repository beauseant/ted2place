import tarfile
import xmltodict
import json

def openTarFile ( filename ):

    with tarfile.open(filename, "r:gz") as file:
        # don't use file.members as it's 
        # not giving nested files and folders
        
        dictdata = []

        for member in file:        
            if member.name.endswith ('xml'):
                data = file.extractfile(member.name).read()
                dictdata.append(xmltodict.parse(data))
        return dictdata



def getNested(data, *args):
    if args and data:
        element  = args[0]
        if element:
            value = data.get(element)
            return value if len(args) == 1 else getNested(value, *args[1:])


def invertNested (test_str, sep):
  if sep not in test_str:
    return test_str
  key, val = test_str.split(sep, 1)
  return {key: invertNested (val, sep)}            


def merge(a, b, path=None):
    "merges b into a"
    if path is None: path = []
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                merge(a[key], b[key], path + [str(key)])
            elif a[key] == b[key]:
                pass # same leaf value
            else:
                raise Exception('Conflict at %s' % '.'.join(path + [str(key)]))
        else:
            a[key] = b[key]
    return a



class TranslateMachine:

    __keysToSaveMain = ['@DOC_ID','LINKS_SECTION']
    __keysTranslateMain = {'@DOC_ID':'DOC_ID','LINKS_SECTION':'LINKS'}

    __keysToSaveMainTechnicalSection = ['RECEPTION_ID','DELETION_DATE']
    __keysTranslateTechnicalSection = {'RECEPTION_ID':'RECEPTION_ID', 'DELETION_DATE':'DELETION_DATE'}

    
    __conversionRules = {}


    __format = ''

    __tedData   = {}
    __placeData = {}

    #__keysToSaveNoticeData = ['NO_DOC_OJS']
    #__keysTranslateNoticeData = {'NO_DOC_OJS':'NO_DOC_OJS'}


    #format indica si queremos salvar:
    #    sólo aquellos campos que están relacionados con place, format = place
    #    sólo aquellos campos que están relacionados con ted, format = ted
    #    todos los campos; format = ted.
    def __init__ ( self, format ):
        assert format in ['all', 'place', 'ted'], 'save parameter must be all, ted or place'

        try:
            with open('ConversionRules.json', 'r') as fp:
                self.__conversionRules = json.load(fp)
        except Exception as E:
            print ('error abriendo fichero de conversión a Place, %s' % E)
            exit()

        self.__format = format


    def translateMainSection ( self, data ):
        return  {self.__keysTranslateMain[key] : data[key] for key in self.__keysToSaveMain }

    def translateTechnicalSection ( self, data ):
        #si queremos un diccionarion con otro diccionario para cada sección:
        #data['TECHNICAL_SECTION'] = {keysTranslateTechnicalSection[key]: tedDoc['TED_EXPORT']['TECHNICAL_SECTION'][key] for key in keysTranslateTechnicalSection }
        #si queremos un sólo nivel:
        return {self.__keysTranslateTechnicalSection[key]: data[key] for key in self.__keysToSaveMainTechnicalSection }    


   
    def __translateToPlace ( self ):

        
        data = self.__tedData
        new_data = {}        
        self.__placeData = {}


        #preparamos los campos de data que pueden dar problemas:
        #cpvs = {'DatosGeneralesDelExpediente':{'Clasificacion CPV':''}}
        if type(data['CODED_DATA_SECTION']['NOTICE_DATA']['ORIGINAL_CPV']) == list:
            cpvs =  ([{'value' : cpv['@CODE'],'text':cpv['#text']} for cpv in data['CODED_DATA_SECTION']['NOTICE_DATA']['ORIGINAL_CPV']])
        else:
            cpvs =  ([{'value' : data['CODED_DATA_SECTION']['NOTICE_DATA']['ORIGINAL_CPV']['@CODE'],'text':data['CODED_DATA_SECTION']['NOTICE_DATA']['ORIGINAL_CPV']['#text']} ])

        if 'n2021:CA_CE_NUTS' in data['CODED_DATA_SECTION']['NOTICE_DATA']:
            if type(data['CODED_DATA_SECTION']['NOTICE_DATA']['n2021:CA_CE_NUTS']) == list:
                listCACE = [ {'value': cace['@CODE'],'text':cace['#text']} for cace in data['CODED_DATA_SECTION']['NOTICE_DATA']['n2021:CA_CE_NUTS']]
            else:
                listCACE = [ {'value': data['CODED_DATA_SECTION']['NOTICE_DATA']['n2021:CA_CE_NUTS']['@CODE'],'text':data['CODED_DATA_SECTION']['NOTICE_DATA']['n2021:CA_CE_NUTS']['@CODE']} if 'n2021:CA_CE_NUTS' in  data['CODED_DATA_SECTION']['NOTICE_DATA'].keys()  else '']
        else:
            listCACE = []

        try:
            if type(data['CODED_DATA_SECTION']['CODIF_DATA']['MA_MAIN_ACTIVITIES']) == list:
                listActi =[ {'text': act['#text'], 'value':act['@CODE']} for act in data['CODED_DATA_SECTION']['CODIF_DATA']['MA_MAIN_ACTIVITIES']]
            else:
                listActi = [{'text': data['CODED_DATA_SECTION']['CODIF_DATA']['MA_MAIN_ACTIVITIES']['#text'], 'value':data['CODED_DATA_SECTION']['CODIF_DATA']['MA_MAIN_ACTIVITIES']['@CODE'] } if 'MA_MAIN_ACTIVITIES' in data['CODED_DATA_SECTION']['CODIF_DATA'].keys() else '' ]
        except:
            listActi = []

        currency = json.dumps (data['CODED_DATA_SECTION']['NOTICE_DATA'].get ('VALUES',''))
        if currency == '""':
            currency = 'NONE'

        '''
        if 'VALUES' in data['CODED_DATA_SECTION']['NOTICE_DATA']:
            currency = json.dumps (data['CODED_DATA_SECTION']['NOTICE_DATA']['VALUES'])
            if 'VALUE' in :
            try:
                currency = {
                                'tipo':data['CODED_DATA_SECTION']['NOTICE_DATA']['VALUES']['VALUE']['@TYPE'],
                                'importe':data['CODED_DATA_SECTION']['NOTICE_DATA']['VALUES']['VALUE']['#text'],
                                'moneda':data['CODED_DATA_SECTION']['NOTICE_DATA']['VALUES']['VALUE']['@CURRENCY']
                            }
            except:                
                potato =  self.__placeData
                import ipdb ; ipdb.set_trace ()
        '''

        for key in self.__conversionRules.keys():
            datoNested = getNested (data, *key.split(','))
            if datoNested == None:
                datoNested = 'NONE'

            valorNested = self.__conversionRules [key] + ',' + str(datoNested)
            self.__placeData = merge (self.__placeData , (invertNested (valorNested, ',')) )



        self.__placeData['DatosGeneralesDelExpediente']['Clasificacion CPV'] = cpvs
        self.__placeData['DatosGeneralesDelExpediente']['Valor estimado del contrato'] = currency

        self.__placeData['EntidadAdjudicadora']['Ubicacion organica'] = listCACE
        self.__placeData['EntidadAdjudicadora']['Actividad'] = listActi

        '''
        if len(self.__placeData['EntidadAdjudicadora']['Actividad']) == 0:
            import ipdb ; ipdb.set_trace()
            self.__placeData['EntidadAdjudicadora']['Actividad'] =  data['CONTRACTING_BODY']['CA_ACTIVITY']['@VALUE'] if 'CA_ACTIVITY' in data['CONTRACTING_BODY'].keys() else '' 
        
        if self.__placeData['EntidadAdjudicadora']['URL perfil de contratante'] == '':
            self.__placeData['EntidadAdjudicadora']['URL perfil de contratante']['URL Perfil del contratante'] = data['CONTRACTING_BODY'].get ('URL_DOCUMENT', '')
        
        '''
        #__translateContractingBody

        #al final tenemos dos opciones, queremos salvar sólo los datos de place o queremos adjuntar a los datos de place los de ted

        #potato =  self.__placeData
        #import ipdb ; ipdb.set_trace ()


        if self.__format == 'place':
            data = self.__placeData
        else:
            data.update (self.__placeData)

        
        return data

        


    def __getObjtDesc (self, objdesc):

        data = []
        #si hay varios elementos es que se trata de un lote, en caso de no ser una lista lo metemos en una para simplificar el procesado
        if type(objdesc) != list:
            objdesc = [objdesc] 
        #esto no debe ser lo mÃs eficiente, pero...
        #hay que simplificar el diccionario porque luego, al pasarlo a parquet no queda bien tanto diccionario anidado sin sentido.
        for desc in objdesc:
            try:
                item = desc['@ITEM']
            except:
                item = None

            cpvs = []
            try:
                for cpv in desc['CPV_ADDITIONAL']:
                    cpvs.append (cpv['CPV_CODE']['@CODE'])
            except:
                pass

            try:
                nuts = desc['n2021:NUTS']['@CODE']
            except:
                nuts = None
        
            try:
                main_site = desc['MAIN_SITE']['P']
                if desc['MAIN_SITE']['P']==None:
                    main_site = None
            except:
                main_site = None

            try:
                short_desc = desc['SHORT_DESCR']['P']
            except:
                short_desc = None

            if 'DURATION' in desc.keys():
                duration = dict (desc['DURATION'])
            else:
                duration = None

            euprog = None
            if 'NO_EU_PROGR_RELATED' in desc.keys():
                euprog = 'NO'

            if 'EU_PROGR_RELATED' in desc.keys():
                euprog = 'YES'


            renev = None
            if 'NO_RENEWAL' in desc.keys():
                renev = 'NO'

            if 'RENEWAL' in desc.keys():
                renev = 'YES'

            aceptv = None
            if 'NO_ACCEPTED_VARIANTS' in desc.keys():
                aceptv  = 'NO'

            if 'ACCEPTED_VARIANTS' in desc.keys():                
                aceptv  = 'YES'


            if 'AC' in desc.keys():
                ac = dict (desc['AC'])
            else:
                ac = None

            noavar = None
            if 'NO_ACCEPTED_VARIANTS' in desc.keys():
                noavar = 'NO'
            if 'ACCEPTED_VARIANTS' in desc.keys():
                noavar = 'YES'

            if 'CRITERIA_CANDIDATE' in desc.keys():
                criteria = desc['CRITERIA_CANDIDATE']['P']
            else:
                criteria = None

            renewal = None
            if 'RENEWAL_DESCR' in desc.keys():
                renewal = desc['RENEWAL_DESCR']['P']


            data.append ({'ITEM':item,'CPV_ADDITIONAL':cpvs,'n2021:NUTS':nuts,'MAIN_SITE':main_site, 
                        'DURATION':duration,'EU_PROGR_RELATED':euprog , 'ACCEPTED_VARIANTS': aceptv ,'SHORT_DESCR':short_desc,
                        'LOT':desc.get('LOT_NO',None), 'RENEWAL':renev, 'AC':ac, 'ACCEPTED_VARIANTS':noavar,
                        'NB_MIN_LIMIT_CANDIDATE':desc.get('NB_MIN_LIMIT_CANDIDATE',None), 'NB_MAX_LIMIT_CANDIDATE':desc.get('NB_MAX_LIMIT_CANDIDATE',None),
                        'CRITERIA_CANDIDATE':criteria, 'NB_ENVISAGED_CANDIDATE':desc.get('NB_ENVISAGED_CANDIDATE',None),'DATE_START':desc.get('DATE_START',None),
                         'DATE_END':desc.get('DATE_END',None),'DURATION':desc.get('DURATION',None), 'RENEWAL_DESCR':renewal
                        }) 
    


        return data

    def __getPart (self, part):

        if 'TITLE' in part:
            title = str(part['TITLE']['P'])
        else:
            title = None

        if 'VAL_ESTIMATED_TOTAL' in part:
            val_est = part['VAL_ESTIMATED_TOTAL']['#text']
        else:
            val_est = None

        if 'SHORT_DESCR' in part:
            short_d = json.loads (json.dumps (part['SHORT_DESCR']['P']))
            if type (short_d) == list:
                short_d = short_d[0]
        else:
            short_d = None

        if 'TYPE_CONTRACT' in part:
            type_cont = json.loads (json.dumps (part['TYPE_CONTRACT']['@CTYPE']))
        else:
            type_cont = None

        if 'OBJECT_DESCR' in part:
            obj_desc = self.__getObjtDesc (part['OBJECT_DESCR'])
            num_lotes = len (obj_desc)
        else:
            obj_desc = []
            num_lotes = 0

        lots = None
        tipolote = {}
        if 'NO_LOT_DIVISION' in part.keys():
            lots = 'NO'
        
        #Si existe división por lotes, ponemos un flag y, además, sacamos el tipo de 
        #división de lotes:
        #/LOT_ALL
        #/LOT_MAX_NUMBER
        #/LOT_ONE_ONLY
        #/LOT_MAX_ONE_TENDERER
        #/LOT_COMBINING_CONTRACT_RIGHT
        #        

        if 'LOT_DIVISION' in part.keys():
            lots = 'YES'
            if part['LOT_DIVISION'] != None:
                try:
                    part['LOT_DIVISION'].pop('LOT_ALL')
                    clave = next(iter( part['LOT_DIVISION']))
                    tipolote = {clave:part['LOT_DIVISION'][clave]}
                except:
                    tipolote = {}


        try:
            valor = {'TITLE': title,'REFERENCE_NUMBER':part.get('REFERENCE_NUMBER',None),\
                         'CPV_MAIN':str(part['CPV_MAIN']['CPV_CODE']['@CODE']),'TYPE_CONTRACT':type_cont,\
                          'SHORT_DESCR':short_d, 'VAL_ESTIMATED_TOTAL':val_est,\
                          'LOT_DIVISION':lots,'NUM_LOT':num_lotes,'OBJECT_DESCR':obj_desc,'DATE_PUBLICATION_NOTICE':part.get('DATE_PUBLICATION_NOTICE',None),
                          'TIPO_LOTE' : tipolote
                    }

        except Exception as E:
            print (E)
            import ipdb ; ipdb.set_trace()
        return valor

    def procesarPart (self, data ):

        parts = []

        if type(data) == list:
            for d in data:
                parts.append (self.__getPart (d))
        else:           
            parts.append (self.__getPart (data))



        return parts



    def __getProcedure (self, data):
        
        procedure = {}
        if 'PROCEDURE' in data.keys():
            #Existe un porcentaje muy pequeï¿½o en el que existe PROCEDURE pero es None,
            #si eso ocurre le creamos un diccionario vacï¿½o y asÃ­ las lï¿½neas siguientes no fallan
            #y no hay que capturar el fallo uno por uno.

            if data['PROCEDURE'] == None:
                data['PROCEDURE'] = {}

            procedure['TYPE_PROCEDURE'] = None
            if 'PT_OPEN' in data['PROCEDURE'].keys():
                    procedure['TYPE_PROCEDURE'] ='OPEN'
            if 'PT_DA_EMERGENCY_MEASURE' in data['PROCEDURE'].keys():
                    procedure['TYPE_PROCEDURE'] ='DA_EMERGENCY_MEASURE'
            if 'PT_DA_INTERNAL_OPERATOR' in data['PROCEDURE'].keys():
                    procedure['TYPE_PROCEDURE'] ='DA_INTERNAL_OPERATOR'
            if 'PT_DA_SMALL_CONTRACT' in data['PROCEDURE'].keys():
                    procedure['TYPE_PROCEDURE'] ='DA_SMALL_CONTRACT'
            if 'PT_DA_MEDIUM_ENTERPRISE' in data['PROCEDURE'].keys():
                    procedure['TYPE_PROCEDURE'] ='DA_MEDIUM_ENTERPRISE'
            if 'PT_DA_RAILWAY_TRANSPORT' in data['PROCEDURE'].keys():
                    procedure['TYPE_PROCEDURE'] ='PT_DA_RAILWAY_TRANSPORT'


            procedure['CONTRACT_COVERED_GPA'] = None
            if 'NO_CONTRACT_COVERED_GPA' in data['PROCEDURE'].keys():
                procedure['CONTRACT_COVERED_GPA'] = 'NO'

            if 'CONTRACT_COVERED_GPA' in data['PROCEDURE'].keys():
                procedure['CONTRACT_COVERED_GPA'] = 'YES'

            procedure['DATE_RECEIPT_TENDERS']           = data['PROCEDURE'].get ('DATE_RECEIPT_TENDERS', None)
            procedure['TIME_RECEIPT_TENDERS']           = data['PROCEDURE'].get ('TIME_RECEIPT_TENDERS', None)

            try:
                if data['PROCEDURE']['LANGUAGES'] == list:
                    procedure['LANGUAGES'] = data['PROCEDURE']['LANGUAGES']
                else:
                    procedure['LANGUAGES'] = [data['PROCEDURE']['LANGUAGES']]
            except:
                procedure['LANGUAGES'] = None

            try:
                procedure['DURATION_TENDER_VALID'] = dict(data['PROCEDURE']['DURATION_TENDER_VALID'])
            except:
                procedure['DURATION_TENDER_VALID'] = None

            try:
                procedure['DATE_OPENING_TENDERS'] = dict(data['PROCEDURE']['DATE_OPENING_TENDERS'])
            except:
                procedure['DATE_OPENING_TENDERS'] = None

        return procedure  

    def __getAwardContract (self, data):

        acon = []        
        if 'AWARD_CONTRACT' in data.keys ():
            if type(data['AWARD_CONTRACT']) == list:                
                #import ipdb ; ipdb.set_trace ()
                acon = data['AWARD_CONTRACT']
            else:
                acon = [data['AWARD_CONTRACT']]            

        return acon


    def translateFormSection ( self, data ):
        

        formData = {}
        #formData['EN_TRANSLATION'] = False
        formData['DATE_DISPATCH_NOTICE']=[]
        formData['CONTRACTING_BODY'] = []


        #si es una lista de valores es que se trata de una traduccion, un idioma en cada uno. Buscamos el original y 
        #la traducción al ingles si existe, tambien guardamos el original :
        #HE QUITADO LA PARTE DE GUARDAR LA TRADUCCIÓN A INGLÉS. Complicaba la conversión a place y apenas había campos.


        if type (data) == list:         
            for value in data:
                formData['DATE_DISPATCH_NOTICE'].append(value['COMPLEMENTARY_INFO'].get ('DATE_DISPATCH_NOTICE',None))
                #formData['CONTRACTING_BODY'].append (value['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY'])
                formData['CONTRACTING_BODY'] = value['CONTRACTING_BODY']
                #if value['@LG'] == 'EN' and value['@CATEGORY'] == 'TRANSLATION':
                #    formData['EN_TRANSLATION'] = True
                    #formData['OBJECT_CONTRACT_TRANS'] = self.procesarPart ( value['OBJECT_CONTRACT'] )
                    #formData['OBJECT_CONTRACT_TRANS'] = json.dumps (value['OBJECT_CONTRACT'])

                if  value['@CATEGORY'] == 'ORIGINAL':
                    formData['LG'] = value['@LG']
                    #formData['OBJECT_CONTRACT'] = self.procesarPart ( value['OBJECT_CONTRACT'] )               
                    formData['OBJECT_CONTRACT'] = value['OBJECT_CONTRACT'] 
                    formData['PROCEDURE'] = self.__getProcedure (value)
                    formData['AWARD_CONTRACT'] = self.__getAwardContract (value)


        #solo tiene un idioma:
        else:
            formData['LG'] = data['@LG']
            #formData['OBJECT_CONTRACT_ORIGINAL'] = self.procesarPart ( data['OBJECT_CONTRACT'] )
            formData['OBJECT_CONTRACT'] = self.procesarPart ( data['OBJECT_CONTRACT'] )
            formData['DATE_DISPATCH_NOTICE'].append(data['COMPLEMENTARY_INFO'].get ('DATE_DISPATCH_NOTICE',None))
            #formData['CONTRACTING_BODY'].append (data['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY'])
            formData['CONTRACTING_BODY'] = data['CONTRACTING_BODY']
            formData['PROCEDURE'] = self.__getProcedure (data)
            formData['AWARD_CONTRACT'] = self.__getAwardContract (data)

        if type (formData['DATE_DISPATCH_NOTICE']) ==list:
            formData['DATE_DISPATCH_NOTICE'] = formData['DATE_DISPATCH_NOTICE'][0]

        #a veces aparece mÃs de una empresa licitadora, en realidad son traduciones de la misma, la que vale es la primera.
        if type(formData['CONTRACTING_BODY']) == list:
            formData['CONTRACTING_BODY'] = formData['CONTRACTING_BODY'][0]


        #import ipdb ; ipdb.set_trace()                    
        formData['OBJECT_CONTRACT'] = json.dumps (formData['OBJECT_CONTRACT']) 
        formData['PROCEDURE'] = json.dumps(formData['PROCEDURE'])

        

        return formData

    def translateKeys ( self, tedDoc ):

        data = {}

        formdata = {}



        formdata['FORM_ID'] = list (tedDoc['TED_EXPORT']['FORM_SECTION'].keys())[0]


        data = self.translateMainSection ( tedDoc['TED_EXPORT'] )       

        techData = self.translateTechnicalSection (tedDoc['TED_EXPORT']['TECHNICAL_SECTION'])
        data.update ( techData )

        #notData = self.translateNoticeData (tedDoc['TED_EXPORT']['CODED_DATA_SECTION']['NOTICE_DATA'])
        #data.update (notData)
        data['CODED_DATA_SECTION'] = tedDoc['TED_EXPORT']['CODED_DATA_SECTION']

        data.update ( formdata )

        formdata2 = self.translateFormSection ( tedDoc['TED_EXPORT']['FORM_SECTION'][formdata['FORM_ID']] ) 

        data.update ( formdata2 )

        data['YEAR'] = data['DOC_ID'].split('-')[1]

        self.__tedData = data        

        if self.__format != 'ted':
            self.__translateToPlace ()
            #potato = self.__translateToPlace ()
            #import ipdb ; ipdb.set_trace()
            return self.__placeData
        else:
            return data


        

