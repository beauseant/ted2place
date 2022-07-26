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




class TranslateMachine:

    __keysToSaveMain = ['@DOC_ID','LINKS_SECTION']
    __keysTranslateMain = {'@DOC_ID':'DOC_ID','LINKS_SECTION':'LINKS'}

    __keysToSaveMainTechnicalSection = ['RECEPTION_ID','DELETION_DATE']
    __keysTranslateTechnicalSection = {'RECEPTION_ID':'RECEPTION_ID', 'DELETION_DATE':'DELETION_DATE'}

    __keysTranslatePlace = {'MAIN': {'YEAR': 'Ted_year', 'DOC_ID':'Ted_doc_id','FORM_ID':'Ted_form_id','RECEPTION_ID':'Ted_reception_id'}}


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
        self.__format = format


    def translateMainSection ( self, data ):
        return  {self.__keysTranslateMain[key] : data[key] for key in self.__keysToSaveMain }

    def translateTechnicalSection ( self, data ):
        #si queremos un diccionarion con otro diccionario para cada sección:
        #data['TECHNICAL_SECTION'] = {keysTranslateTechnicalSection[key]: tedDoc['TED_EXPORT']['TECHNICAL_SECTION'][key] for key in keysTranslateTechnicalSection }
        #si queremos un sólo nivel:
        return {self.__keysTranslateTechnicalSection[key]: data[key] for key in self.__keysToSaveMainTechnicalSection }    


    ''''
    def  translateNoticeData (self, data):
        notData =  {self.__keysTranslateNoticeData[key]: data[key] for key in self.__keysToSaveNoticeData }    
        cpvList = []

        if type(data['ORIGINAL_CPV']) == list:
            for cpv in data['ORIGINAL_CPV']:
                cpvList.append ({'CODE':cpv['@CODE'],'TEXT':cpv['#text']} )

            notData['ORIGINAL_CPV'] = cpvList
        else:
            cpvList.append ({'CODE':data['ORIGINAL_CPV']['@CODE'],'TEXT':data['ORIGINAL_CPV']['#text']})

        notData['ORIGINAL_CPV'] = cpvList
        
        uriList = []

        if type(data['URI_LIST']['URI_DOC']) == list:
            uriList = [uri['#text'] for uri in data['URI_LIST']['URI_DOC']]
        else:
            uriList.append ([data['URI_LIST']['URI_DOC']['#text'] ])

        #salvar la lista entera da problemas en dataframe de spark
        #if type(notData['URI_LIST'== list:
        #    notData['URI_LIST'] xuriList[0][0]

        return notData 
    '''

    def __translateCodedDataSection (self, data):

        placeData = {}

        #PARTE REF_OJS:
        #######################################
        placeData['PublicacionesOficiales'] = {'Fecha de Publicacion':data['REF_OJS']['DATE_PUB']}
        placeData['PublicacionesOficiales'] = {'Fecha de envio de anuncio al diario oficial':data['REF_OJS']['DATE_PUB']}
        #######################################

        #NOTICE DATA:
        #######################################

        placeData['EntidadAdjudicadora'] = {'URL perfil de contratante':data['NOTICE_DATA'].get('IA_URL_ETENDERING','')}
        placeData['DatosGeneralesDelExpediente'] = {'Numero del expediente': data['NOTICE_DATA'].get('NO_DOC_OJS','')}

        if type(data['NOTICE_DATA']['ORIGINAL_CPV']) == list:
            listCPVs =  [{'value' : cpv['@CODE'],'text':cpv['#text']} for cpv in data['NOTICE_DATA']['ORIGINAL_CPV']]
        else:
            listCPVs =  [{'value' : data['NOTICE_DATA']['ORIGINAL_CPV']['@CODE'],'text':data['NOTICE_DATA']['ORIGINAL_CPV']['#text']} ]

        placeData['DatosGeneralesDelExpediente'] = {'Clasificacion CPV': listCPVs}

        placeData['EntidadAdjudicadora'].update ({'Sitio web': data['NOTICE_DATA'].get('IA_URL_GENERAL','')})        
        placeData['LugarDeEjecucion'] = {'Pais': data['NOTICE_DATA']['ISO_COUNTRY']['@VALUE'] if  'ISO_COUNTRY' in data['NOTICE_DATA'].keys() else ''}

        placeData['DatosGeneralesDelExpediente'].update ({'Valor estimado del contrato':data['NOTICE_DATA'].get('VALUES','')})

        if 'n2021:CA_CE_NUTS' in data['NOTICE_DATA']:
            if type(data['NOTICE_DATA']['n2021:CA_CE_NUTS']) == list:
                listCACE = [ {'value': cace['@CODE'],'text':cace['#text']} for cace in data['NOTICE_DATA']['n2021:CA_CE_NUTS']]
            else:
                listCACE = [ {'value': data['NOTICE_DATA']['n2021:CA_CE_NUTS']['@CODE'],'text':data['NOTICE_DATA']['n2021:CA_CE_NUTS']['@CODE']} if 'n2021:CA_CE_NUTS' in  data['NOTICE_DATA'].keys()  else '']
        else:
            listCACE = []

        placeData['EntidadAdjudicadora'].update ( {'Ubicacion organica': listCACE})


        #######################################


        #CODIF_DATA
        #######################################

        placeData['CriterioDeAdjudicacion'] = {}
        placeData['CriterioDeAdjudicacion'].update ({'Descripcion': {'text': data['CODIF_DATA']['AC_AWARD_CRIT']['#text'], 'value':data['CODIF_DATA']['AC_AWARD_CRIT']['@CODE'] } if 'AC_AWARD_CRIT' in data['CODIF_DATA'].keys() else '' })

        placeData['ProcesoDeLicitacion'] = {}
        placeData['ProcesoDeLicitacion'].update ({'Descripcion': {'text': data['CODIF_DATA']['TY_TYPE_BID']['#text'], 'value':data['CODIF_DATA']['TY_TYPE_BID']['@CODE'] } if 'TY_TYPE_BID' in data['CODIF_DATA'].keys() else '' })
        placeData['ProcesoDeLicitacion'].update ({'Tramitacion': {'text': data['CODIF_DATA']['PR_PROC']['#text'], 'value':data['CODIF_DATA']['PR_PROC']['@CODE'] } if 'PR_PROC' in data['CODIF_DATA'].keys() else '' })

        placeData['EntidadAdjudicadora'].update ({'Tipo de administracion': {'text': data['CODIF_DATA']['AA_AUTHORITY_TYPE']['#text'], 'value':data['CODIF_DATA']['AA_AUTHORITY_TYPE']['@CODE'] } if 'AA_AUTHORITY_TYPE' in data['CODIF_DATA'].keys() else '' })


        placeData['PublicacionesOficiales'].update ({'Fecha de Publicación': data['CODIF_DATA'].get('DS_DATE_DISPATCH','')})

        placeData['DatosGeneralesDelExpediente'].update ({'Pliego de clausulas administrativas': {'text': data['CODIF_DATA']['RP_REGULATION']['#text'], 'value':data['CODIF_DATA']['RP_REGULATION']['@CODE'] } if 'RP_REGULATION' in data['CODIF_DATA'].keys() else '' })
        placeData['DatosGeneralesDelExpediente'].update ({'Tipo de contrato': {'text': data['CODIF_DATA']['TD_DOCUMENT_TYPE']['#text'], 'value':data['CODIF_DATA']['TD_DOCUMENT_TYPE']['@CODE'] } if 'TD_DOCUMENT_TYPE' in data['CODIF_DATA'].keys() else '' })
        placeData['DatosGeneralesDelExpediente'].update ({'Objeto del contrato': {'text': data['CODIF_DATA']['NC_CONTRACT_NATURE']['#text'], 'value':data['CODIF_DATA']['NC_CONTRACT_NATURE']['@CODE'] } if 'NC_CONTRACT_NATURE' in data['CODIF_DATA'].keys() else '' })


        try:
            if type(data['CODIF_DATA']['MA_MAIN_ACTIVITIES']) == list:
                listActi =[ {'text': act['#text'], 'value':act['@CODE']} for act in data['CODIF_DATA']['MA_MAIN_ACTIVITIES']]
            else:
                listActi = [{'text': data['CODIF_DATA']['MA_MAIN_ACTIVITIES']['#text'], 'value':data['CODIF_DATA']['MA_MAIN_ACTIVITIES']['@CODE'] } if 'MA_MAIN_ACTIVITIES' in data['CODIF_DATA'].keys() else '' ]
        except:
            listActi = []

        placeData['EntidadAdjudicadora'].update ({'Actividad': listActi})



        placeData['PlazoDePresentacionDeOferta'] = {'Fecha': data['CODIF_DATA'].get('DT_DATE_FOR_SUBMISSION','')}
        placeData['PlazoDePresentacionDeSolicitudes'] = {'Fecha': data['CODIF_DATA'].get('DT_DATE_FOR_SUBMISSION','')}

        #######################################

        self.__placeData.update (placeData)


    def __translateContractingBody (self, data):

        
        ##AMPLIAMOS DATOS DE LA EntidadAdjudicadora:
        #############################################

        entidadAd = {}
        entidadAd['TipoDeAministracion'] = data['CA_TYPE']['@VALUE'] if 'CA_TYPE' in data.keys() else '' 

        if self.__placeData['EntidadAdjudicadora']['Actividad'] == '':
            entidadAd['Actividad'] =  data['CA_ACTIVITY']['@VALUE'] if 'CA_ACTIVITY' in data.keys() else '' 
        
        if self.__placeData['EntidadAdjudicadora']['URL perfil de contratante'] == '':
            entidadAd['URL Perfil del contratante'] = data.get ('URL_DOCUMENT', '')

        addContBody = data['ADDRESS_CONTRACTING_BODY']

        entidadAd['Nombre']                     = addContBody.get ('OFFICIALNAME', '')        
        entidadAd['Sitio web']                  = addContBody.get ('URL_GENERAL', '')        
        entidadAd['Pais']                       = addContBody.get ('COUNTRY', '')   
        entidadAd['Codigo Postal']              = addContBody.get ('POSTAL_CODE', '')   
        entidadAd['Poblacion']                  = addContBody.get ('TOWN', '')   
        entidadAd['Nombre para contacto']       = addContBody.get ('CONTACT_POINT', '')   
        entidadAd['Calle']                      = addContBody.get ('ADDRESS', '')   
        entidadAd['Ubicacion']                  = addContBody.get ('n2021:NUTS', '')   
        entidadAd['Correo electronico']         = addContBody.get ('E_MAIL', '')   
        entidadAd['ID|NIF|ID Plataforma']       = addContBody.get ('NATIONALID', '')   
        entidadAd['Telefono']                   = addContBody.get ('PHONE', '')   
        entidadAd['URL perfil de contratante']  = addContBody.get ('URL_BUYER', '')   
        entidadAd['Fax']                        = addContBody.get ('FAX', '')   

        self.__placeData['EntidadAdjudicadora'].update (entidadAd)

    def __translateToPlace ( self ):

        
        data = self.__tedData

        new_data = {}

        self.__placeData = {}
        self.__placeData.update ( {self.__keysTranslatePlace['MAIN'][key] : data[key] for key in self.__keysTranslatePlace['MAIN'] } )
        #patata =self.__tedData
        #patata2 = self.__keysTranslatePlace
        #import ipdb ; ipdb.set_trace()

        #cada una de estas funciones va actualizando el campo self.__placeData.
        self.__translateCodedDataSection (data['CODED_DATA_SECTION'])
        #self.__translateContractingBody  (data['CONTRACTING_BODY'])

        
        #al final tenemos dos opciones, queremos salvar sólo los datos de place o queremos adjuntar a los datos de place los de ted
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
            return self.__placeData
        else:
            return data


        

