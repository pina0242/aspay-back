
from app.core.models import DBDGENPERS, DBDCOMPERS, DBDIRPERS, DBRELPERS, DBCTAPERS, DBDOCPERS , DBENTIDAD , DBSERVNIV
from app.core.cypher import encrypt_message , decrypt_message
from app.core.cai import valcampo, obtener_fecha_actual, tipos_id_se_validan, validar_id_persona, es_mayor_de_edad, creaNum10, \
                    busca_activ_econ , obtener_entidad_unica , generar_iban_interno 
from app.core.config import settings
from app.core.state import app_state
from app.core.auth import create_access_token
from app.core.security import hash_password, verify_password
from sqlalchemy import  func
from datetime import timedelta, datetime, date
import logging
import random
import json 
import uuid

logger = logging.getLogger(__name__)

class DatgenService:
    
    def __init__(self, db_session=None):
        self.db = db_session

    def implemenService(self, db, entidad, usuario_alt='SISTEMA'):
        """
        Versión definitiva corregida - SIN pasar fecha_alta
        """
        print(f'=== Implementando servicios para entidad: {entidad} ===')
        
        # 1. Obtener configuraciones de servicios de referencia
        servicios_con_config = db.query(DBSERVNIV).filter(
            DBSERVNIV.entidad == '0001',
            DBSERVNIV.indexc == 'O'
        ).all()
        
        if not servicios_con_config:
            print(" No se encontraron servicios de referencia")
            return {'error': 'No hay servicios de referencia'}
        
        print(f"Servicios de referencia encontrados: {len(servicios_con_config)}")
        
        
        configuraciones = []
        for serv_ref in servicios_con_config:
            config = {
                'Servicio': serv_ref.Servicio,
                'nivel': serv_ref.nivel,
                'indauth': serv_ref.indauth,
                'indcost': serv_ref.indcost,
                'indmon': serv_ref.indmon,
                'indexc': serv_ref.indexc,
                'impmax': serv_ref.impmax,
                'status': serv_ref.status,
                'usuario_alt': usuario_alt
            }
            configuraciones.append(config)
            print(f"  {serv_ref.Servicio}")
        
        # 3. Llamar a función bulk corregida
        resultado = self.registrar_servicios_bulk_corregido(
            db=db,
            entidad=entidad,
            configuraciones=configuraciones
        )
        
        # 4. Mostrar resumen
        print(f"\nResumen para {entidad}:")
        print(f"   - Servicios procesados: {resultado.get('total_procesados', 0)}")
        print(f"   - Servicios creados: {resultado.get('total_creados', 0)}")
        print(f"   - Servicios fallidos: {resultado.get('total_fallidos', 0)}")
        
        return resultado

    def registrar_servicios_bulk_corregido(self, db, entidad, configuraciones):
        """
        Versión CORREGIDA - El constructor DBSERVNIV NO acepta fecha_alta
        """
        from datetime import datetime
        
        nuevos_registros = []
        servicios_creados = []
        servicios_fallidos = []
        
        for config in configuraciones:
            servicio_nombre = config.get('Servicio', 'DESCONOCIDO')
            
            try:
                # Verificar si ya existe
                existe = db.query(DBSERVNIV.id).filter(
                    DBSERVNIV.entidad == entidad,
                    DBSERVNIV.Servicio == servicio_nombre
                ).first()
                
                if existe:
                    servicios_fallidos.append({
                        'servicio': servicio_nombre,
                        'error': 'Ya existe'
                    })
                    continue
                
                # 
                nuevo = DBSERVNIV(
                    entidad=entidad,
                    Servicio=servicio_nombre,
                    nivel='1',
                    indauth='1',
                    indcost='A',
                    indmon='0',
                    indexc='N',
                    impmax= 0.0,
                    usuario_alt='SISTEMA',
                    status='A'
                    
                )
                
                nuevos_registros.append(nuevo)
                servicios_creados.append(servicio_nombre)
                
            except Exception as e:
                servicios_fallidos.append({
                    'servicio': servicio_nombre,
                    'error': str(e)
                })
        
        # Realizar bulk insert
        if nuevos_registros:
            try:
                db.bulk_save_objects(nuevos_registros)
                db.commit()
                print(f"  Bulk insert exitoso: {len(nuevos_registros)} registros")
            except Exception as e:
                db.rollback()
                print(f"  Error en bulk insert: {str(e)}")
                servicios_fallidos.append({
                    'servicio': 'BULK_INSERT',
                    'error': f"Error en inserción masiva: {str(e)}"
                })
                servicios_creados = []
        
        return {
            'creados': servicios_creados,
            'fallidos': servicios_fallidos,
            'total_procesados': len(configuraciones),
            'total_creados': len(servicios_creados),
            'total_fallidos': len(servicios_fallidos)
        }

    def regdgenper(self, datos , user, db):
            result = []
            estado = True
            access_token = ''
            message = datos
            message = str(message).replace('"','')
            if not settings.PASSCYPH or settings.PASSCYPH == "default-password":
                logger.error("PASSCYPH no configurado en .env")
                return None, 500
            else:
                password = settings.PASSCYPH
                
                decriptMsg , estado = decrypt_message(password, message)
                print('decriptMsg :', decriptMsg)
                app_state.jsonrec = decriptMsg

            if estado :
                dataJSON = json.loads(decriptMsg)
                param_keys = ['id_persona','tipo_id','nombre' , 'ap_paterno','ap_materno', 'genero','tipo_per','tipo_cte','fecha_nac_const','ocupacion','giro',
                            'pais_nac_const','nacionalidad','estado_civil','num_reg_mercantil','ind_pers_migrada','entidad']
                if not all (key in dataJSON for key in param_keys):
                    estado = False
                    result.append({'response':'Algun elemento de la transacción esta faltando'})                     
                    rc = 400 
                else:
                    id_persona          = dataJSON["id_persona"]   
                    tipo_id             = dataJSON["tipo_id"] 
                    nombre              = dataJSON["nombre"]   
                    ap_paterno          = dataJSON["ap_paterno"]
                    ap_materno          = dataJSON["ap_materno"]
                    genero              = dataJSON["genero"]   
                    tipo_per            = dataJSON["tipo_per"]
                    tipo_cte            = dataJSON["tipo_cte"]
                    fecha_nac_const     = dataJSON["fecha_nac_const"] 
                    ocupacion           = dataJSON["ocupacion"]
                    giro                = dataJSON["giro"]
                    pais_nac_const      = dataJSON["pais_nac_const"] 
                    nacionalidad        = dataJSON["nacionalidad"]
                    estado_civil        = dataJSON["estado_civil"] 
                    num_reg_mercantil   = dataJSON["num_reg_mercantil"] 
                    ind_pers_migrada    = dataJSON["ind_pers_migrada"]   
                    entidad             = dataJSON["entidad"]              
            else:
                estado = False
                result = 'informacion incorrecta'
                rc = 400

            if estado:
                validations = {
                    'id_persona': ('long', 9),
                    'tipo_id': ('long', 3),
                    'nombre': ('long', 60),
                    'ap_paterno': ('long', 30),
                    'ap_materno': ('long', 30),
                    'genero': ('long', 1),
                    'tipo_per': ('long', 1),
                    'tipo_cte': ('long', 1),
                    'fecha_nac_const': ('date', 10),
                    'ocupacion': ('long', 30),
                    'giro': ('long', 30),
                    'pais_nac_const': ('long', 30),
                    'nacionalidad': ('long', 30),
                    'estado_civil': ('long', 1),
                    'num_reg_mercantil': ('long', 25),
                    'ind_pers_migrada': ('long', 1),
                    'entidad': ('long', 4)
                } 
                for key, (tipo, limite) in validations.items():
                        if not valcampo(tipo, dataJSON[key], limite):
                            response_key = key
                            result = [{'response': response_key}]
                            rc = 400
                            message = json.dumps(result)
                            app_state.jsonenv = message
                            return result, rc
                        

            if estado :
                try:  
                    cadena_tid_es, cadena_tid_pt = tipos_id_se_validan(db)
                    print('cadena_tid_es', cadena_tid_es)
                    # Determinar si se debe validar el ID basado en el país y tipo_id
                    validar_id = False
            
                    if pais_nac_const == 'España' and tipo_id in cadena_tid_es:
                        validar_id = True
                    elif pais_nac_const == 'Portugal' and tipo_id in cadena_tid_pt:
                        validar_id = True

                    # Validar el ID solo si corresponde según el país y tipo
                    if validar_id:    
                        id_valido = validar_id_persona(id_persona)
                        #print('id_valido',id_valido)
                        if id_valido.get('valido'):
                            tipo_id = id_valido.get('tipo', 'Desconocido')
                        else:
                            # Si 'valido' es False, se maneja el error.
                            # Se usa .get() para evitar errores si la clave 'error' no existe.
                            error_msg = id_valido.get('error', 'id de persona no válido o formato no reconocido')
                            result.append({'response': error_msg})
                            estado = False
                            rc = 400
                            return result, rc  
                except Exception as e:
                    print ('Error :', e)
                    rc = 400  
                    result.append({'response':'Error al identificar'})  

            if estado :    
                if tipo_per == 'F':
                    if not es_mayor_de_edad(fecha_nac_const):
                        result.append({'response':'la persona fisica no es mayor de edad'}) 
                        estado = False
                        rc = 400
                        return result , rc 
            if estado :
                if entidad != '':
                    valent =  db.query(DBENTIDAD).where(DBENTIDAD.entidad== entidad, DBENTIDAD.status=='A').first() 
                    
                    if valent:
                        print( 'entidad validada  ', entidad)
                    else:
                        result = [{'response': 'entidad inexistente'}]
                        rc = 400
                        message = json.dumps(result)
                        app_state.jsonenv = message
                        return result, rc

                else:
                    entidad = obtener_entidad_unica(db)
                    print (' entidad: ', entidad)

            if estado :
                iban_interno, enmascarv = generar_iban_interno()
                print('iban interno:', iban_interno, 'dato enmascarado',enmascarv)
                datos_cta_encrypted, encrypt_estado = encrypt_message(password, iban_interno)
                if not encrypt_estado:
                    result.append({'response': 'Error al encriptar el campo datos.'})
                    rc = 500 # Internal Server Error
                    return result, rc
                
            if estado :
                try:
                    num_id        = creaNum10(db)
                    id_persona    = id_persona
                    tipo_id       = tipo_id
                    estatus       = 'A'
                    avanreg       = 25
                    ind_pers_migrada = ind_pers_migrada
                    fecha_state   = obtener_fecha_actual()  
                    fecha_mod     = datetime.min 
                    usuario_mod   = ' ' 
                    usuario_alta  = user  
                    #print ('fecha_state',fecha_state)
                    nuevo_token = str(uuid.uuid4())               

                    if tipo_per == 'F':
                        ap_paterno   = ap_paterno
                        ap_materno   = ap_materno
                        genero       = genero
                        ocupacion    = ocupacion
                        giro         = ' '            
                        estado_civil = estado_civil  
                        num_reg_mercantil = 0
                    else:
                        ap_paterno   = ' '
                        ap_materno   = ' '
                        genero       = ' '
                        ocupacion    = ' '
                        giro         = giro
                        estado_civil = ' '  
                        num_reg_mercantil = num_reg_mercantil

                    #datos de cuenta interna
                    pais  ='ES'
                    moneda = 'EUR'
                    entban = '9999'
                    tipo  = '001'
                    alias ='CTA INTRNA'
                    datos = datos_cta_encrypted
                    indoper = 'CS'
                    enmascar = enmascarv
                    categoria = 'NINGUNA'
                    nombEnt = nombre + ' ' + ap_paterno + ' ' + ap_materno 
                    status  = 'A'
                    regEntit   = DBENTIDAD(entidad, num_id ,nombEnt, status, usuario_alta, usuario_mod)
                    db.add(regEntit)
                                                            
                    regdgenPer = DBDGENPERS(num_id,entidad,nuevo_token,id_persona,tipo_id,nombre,ap_paterno,ap_materno,genero,tipo_per,tipo_cte,fecha_nac_const,ocupacion,giro,pais_nac_const,
                    nacionalidad,estado_civil,num_reg_mercantil,ind_pers_migrada,avanreg,estatus,fecha_state,usuario_alta,fecha_mod,usuario_mod)
                    #regdgenPer.tknper = nuevo_token
                    db.add(regdgenPer) 

                    regctaint = DBCTAPERS(entidad,nuevo_token,pais,moneda,entban,tipo,alias,datos,indoper,estatus,enmascar,categoria,usuario_alta,fecha_mod,usuario_mod)
                    db.add(regctaint) 
                    db.commit() 

                    self.implemenService(db,entidad)

                    result.append({'response':'Exito'
                                        })        
                    rc = 201 
                    #print('result :', result) 
                    message = json.dumps(result)
                    app_state.jsonenv = message 
                    #print('message :', message) 
                    encripresult , estado = encrypt_message(password, message)
                    #print('encripresult :', encripresult)
                    result = encripresult
                except Exception as e:
                    print ('Error :', e)
                    rc = 400  
                    result.append({'response':'Error Fatal'})            

            return result , rc   
    
    def seldgenper(self, datos , user, db):
        result = []
        estado = True
        access_token = ''
        message = datos
        message = str(message).replace('"','')
        if not settings.PASSCYPH or settings.PASSCYPH == "default-password":
            logger.error("PASSCYPH no configurado en .env")
            return None, 500
        else:
            password = settings.PASSCYPH
            
            decriptMsg , estado = decrypt_message(password, message)
            #print('decriptMsg :', decriptMsg)
            app_state.jsonrec = decriptMsg

        if estado:
            dataJSON = json.loads(decriptMsg)
        
        # Lista de campos posibles
            param_keys = ['num_id','id_persona', 'nombre', 'ap_paterno', 'ap_materno']
        
        # Verificar que al menos un campo esté presente
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response': 'Algun elemento de la transacción esta faltando'})
                rc = 400
            else:
            # Extraer campos (pueden ser None si no están presentes)
                num_id     = dataJSON.get("num_id")
                id_persona = dataJSON.get("id_persona")
                nombre     = dataJSON.get("nombre")
                ap_paterno = dataJSON.get("ap_paterno")
                ap_materno = dataJSON.get("ap_materno")
        else:
            estado = False            
            result.append({'response': 'informacion incorrecta'}) 
            rc = 400
    
    # Validaciones individuales de campos
        print ('sale de informacion incorrecta' )
        if estado:
            validations = {
                'num_id': ('long', 10),
                'id_persona': ('long', 9)
            } 
            for key, (tipo, limite) in validations.items():
                    if not valcampo(tipo, dataJSON[key], limite):
                        response_key = key
                        result = [{'response': response_key}]
                        rc = 400
                        message = json.dumps(result)
                        app_state.jsonenv = message
                        return result, rc
        # # Validar num_id si está presente
        #     if not valcampo('long', num_id, 10):
        #         result.append({'response': 'num_id inválido'}) 
        #         estado = False
        #         rc = 400
        #     if not valcampo('long',id_persona,9):
        #         result.append({'response': 'id_persona inválido'}) 
        #         estado = False
        #         rc = 400
        print ('sale de validator' )
        if estado:
            try:  
            # Construir consulta dinámica basada en los campos proporcionados
                query = db.query(DBDGENPERS).filter(DBDGENPERS.estatus == 'A')
            
            # Agregar filtros solo para los campos que tienen valor
                if num_id != '':
                    query = query.filter(DBDGENPERS.num_id == num_id)

                if id_persona != '':
                    query = query.filter(DBDGENPERS.id_persona == id_persona)
            
                if nombre  != '':
                    query = query.filter(DBDGENPERS.nombre.ilike(f'%{nombre}%'))
            
                if ap_paterno  != '':
                    query = query.filter(DBDGENPERS.ap_paterno.ilike(f'%{ap_paterno}%'))
            
                if ap_materno  != '':
                    query = query.filter(DBDGENPERS.ap_materno.ilike(f'%{ap_materno}%'))

            
            # Ordenar y obtener resultados
                personas = query.order_by(DBDGENPERS.id.desc()).all()
                print ('sale de quieries')
                if personas:
                    for per in personas:
                        print ('si hay datos')
                        # Convertir los objetos de fecha y datetime a string
                        fecha_alta_str = per.fecha_alta.isoformat() if isinstance(per.fecha_alta, (date, datetime)) else str(per.fecha_alta)
                        fecha_nac_const_str = per.fecha_nac_const.isoformat() if isinstance(per.fecha_nac_const, (date, datetime)) else str(per.fecha_nac_const)
                        acteconp = '   '
                        if per.tipo_per == 'M':
                            giro_buscar = per.giro
                            acteconp, actecons = busca_activ_econ(giro_buscar,db)    
                            print('acteconp:', acteconp)
                        print ('sale de giro')
                        result.append({ 
                            'num_id'    : per.num_id,
                            'entidad'   : per.entidad,
                            'id_persona': per.id_persona,
                            'tipo_id'   : per.tipo_id,
                            'nombre'    : per.nombre,
                            'ap_paterno': per.ap_paterno,
                            'ap_materno': per.ap_materno,
                            'genero'    : per.genero,   
                            'tipo_per'  : per.tipo_per,
                            'tipo_cte'  : per.tipo_cte,
                            'fecha_nac_const': fecha_nac_const_str,
                            'ocupacion'     : per.ocupacion,
                            'actecon'       : acteconp,
                            'giro'          : per.giro,
                            'pais_nac_const': per.pais_nac_const,
                            'nacionalidad'  : per.nacionalidad,
                            'estado_civil'  : per.estado_civil,
                            'num_reg_mercantil': per.num_reg_mercantil,
                            'ind_pers_migrada' : per.ind_pers_migrada,
                            'avanreg'   : per.avanreg,
                            'fecha_alta': fecha_alta_str,
                            'tknper'    : per.tknper                                                                                              
                        })    
                    rc = 201
                
                    message = json.dumps(result)
                    app_state.jsonenv = message 
                    #print('message :', message) 
                    encripresult, estado = encrypt_message(password, message)
                    if estado:
                        result = encripresult
                    else:
                    # Manejar el caso si la encriptación falla
                        result = [{'response': 'Error encriptando el resultado'}]
                        rc = 500
                else:
                    result.append({'response': 'No existen datos que coincidan con los criterios de búsqueda'})                 
                    rc = 404
                        
            except Exception as e:
                print('Error :', e)
                rc = 500  
                result.append({'response': 'Error interno del servidor'}) 
                message = json.dumps(result)
                app_state.jsonenv = message           

        return result, rc
    
    def upddgenper(self, datos , user, db):
        result = []
        estado = True
        access_token = ''
        message = datos
        message = str(message).replace('"','')
        if not settings.PASSCYPH or settings.PASSCYPH == "default-password":
            logger.error("PASSCYPH no configurado en .env")
            return None, 500
        else:
            password = settings.PASSCYPH
            
            decriptMsg , estado = decrypt_message(password, message)
            #print('decriptMsg :', decriptMsg)
            app_state.jsonrec = decriptMsg

        if estado :
            dataJSON = json.loads(decriptMsg)
            param_keys = ['num_id','id_persona','tipo_id','nombre' , 'ap_paterno','ap_materno', 'genero','tipo_per','tipo_cte','fecha_nac_const','ocupacion','giro','pais_nac_const',
                          'nacionalidad','estado_civil', 'num_reg_mercantil','ind_pers_migrada']       
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'})                     
                rc = 400 
            else:                
                num_id     = dataJSON["num_id"]
                id_persona = dataJSON["id_persona"]           
                tipo_id    = dataJSON["tipo_id"]          
                nombre     = dataJSON["nombre"]   
                ap_paterno = dataJSON["ap_paterno"]
                ap_materno = dataJSON["ap_materno"]
                genero     = dataJSON["genero"]   
                tipo_per   = dataJSON["tipo_per"]
                tipo_cte   = dataJSON["tipo_cte"]
                fecha_nac_const   = dataJSON["fecha_nac_const"] 
                ocupacion         = dataJSON["ocupacion"]
                giro              = dataJSON["giro"]
                pais_nac_const    = dataJSON["pais_nac_const"] 
                nacionalidad      = dataJSON["nacionalidad"]
                estado_civil      = dataJSON["estado_civil"] 
                num_reg_mercantil = dataJSON["num_reg_mercantil"] 
                ind_pers_migrada  = dataJSON["ind_pers_migrada"] 
        else:
            estado = False
            result = 'informacion incorrecta'
            rc = 400
        if estado:
            validations = {
                'num_id': ('long', 10),
                'id_persona': ('long', 9),
                'tipo_id': ('long', 3),
                'nombre': ('long', 60),
                'ap_paterno': ('long', 30),
                'ap_materno': ('long', 30),
                'genero': ('long', 1),
                'tipo_per': ('long', 1),
                'tipo_cte': ('long', 1),
                'fecha_nac_const': ('date', 10),
                'ocupacion': ('long', 30),
                'giro': ('long', 30),
                'pais_nac_const': ('long', 30),
                'nacionalidad': ('long', 30),
                'estado_civil': ('long', 1),
                'num_reg_mercantil': ('long', 25),
                'ind_pers_migrada': ('long', 1)
            } 
            for key, (tipo, limite) in validations.items():
                    if not valcampo(tipo, dataJSON[key], limite):
                        response_key = key
                        result = [{'response': response_key}]
                        rc = 400
                        message = json.dumps(result)
                        app_state.jsonenv = message
                        return result, rc
            # if not valcampo('long',num_id,10):
            #     result.append({'response':'num_id'})
            #     estado = False
            #     rc = 400
            #     #print('result :', result)
            #     return result , rc
            # if not valcampo('long',id_persona,9):
            #     result.append({'response':'id_persona'})
            #     estado = False
            #     rc = 400
            #     #print('result :', result)
            #     return result , rc
            # if not valcampo('long',tipo_id,3):
            #     result.append({'response':'tipo_id'})
            #     estado = False
            #     rc = 400
            #     print('result :', result)
            #     return result , rc
            # if not valcampo('long',nombre,60):
            #     result.append({'response':'nombre'})
            #     estado = False
            #     rc = 400
            #     #print('result :', result)
            #     return result , rc
            # if not valcampo('long',ap_paterno,30):
            #     result.append({'response':'ap_paterno'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc
            # if not valcampo('long',ap_materno,30):
            #     result.append({'response':'ap_materno'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc  
            # if not valcampo('long',genero,1):
            #     result.append({'response':'genero'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc               
            # if not valcampo('long',tipo_per,1):
            #     result.append({'response':'tipo_per'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc    
            # if not valcampo('long',tipo_cte,1):
            #     result.append({'response':'tipo_cte'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc
            # if not valcampo('date',fecha_nac_const,10):
            #     result.append({'response':'fecha_nac_const'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc                                             
            # if not valcampo('long',ocupacion,30):
            #     result.append({'response':'ocupacion'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc
            # if not valcampo('long',giro,30):
            #     result.append({'response':'giro'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc                 
            # if not valcampo('long',pais_nac_const,30):
            #     result.append({'response':'pais_nac_const'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc          
            # if not valcampo('long',nacionalidad,30):
            #     result.append({'response':'nacionalidad'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc    
            # if not valcampo('long',estado_civil,1):
            #     result.append({'response':'estado_civil'}) 
            #     estado = False
            #     rc = 400
            # if not valcampo('long',num_reg_mercantil,25):
            #     result.append({'response':'num_reg_mercantil'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc   
            # if not valcampo('long',ind_pers_migrada,1):
            #     result.append({'response':'ind_pers_migrada'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc                             

        if estado :
            try:  
                cadena_tid_es, cadena_tid_pt = tipos_id_se_validan(db)
        
                # Determinar si se debe validar el ID basado en el país y tipo_id
                validar_id = False
        
                if pais_nac_const == 'España' and tipo_id in cadena_tid_es:
                    validar_id = True
                elif pais_nac_const == 'Portugal' and tipo_id in cadena_tid_pt:
                    validar_id = True

                # Validar el ID solo si corresponde según el país y tipo
                if validar_id:    
                    id_valido = validar_id_persona(id_persona)
                    #print('id_valido',id_valido)
                    if id_valido.get('valido'):
                        tipo_id = id_valido.get('tipo', 'Desconocido')
                    else:
                        # Si 'valido' es False, se maneja el error.
                        # Se usa .get() para evitar errores si la clave 'error' no existe.
                        error_msg = id_valido.get('error', 'id de persona no válido o formato no reconocido')
                        result.append({'response': error_msg})
                        estado = False
                        rc = 400
                        return result, rc  
            except Exception as e:
                print ('Error :', e)
                rc = 400  
                result.append({'response':'Error al identificar'})                       
                                   
        if estado :                                                          
            per =  db.query(DBDGENPERS).where(DBDGENPERS.num_id==num_id, DBDGENPERS.estatus=='A').first()            
            if per:
                try:
                # 1. Crear copia histórica (estatus 'M')
                    campos_originales = {}
                    for col in per.__table__.columns:
                        if col.name not in ['id','tknper','fecha_alta']:
                            campos_originales[col.name] = getattr(per, col.name)
                
                    campos_originales['estatus'] = 'M'  # Status histórico
                    campos_originales['usuario_alta'] = user  # Usuario que crea el histórico

                    #print('campos_originales :', campos_originales)
                
                    dgpers_historico = DBDGENPERS(**campos_originales)
                    db.add(dgpers_historico)

                # 2. ACTUALIZAR registro original (mantener status 'A')
                    per.id_persona       = id_persona
                    per.tipo_id          = tipo_id
                    per.nombre           = nombre
                    per.tipo_cte         = tipo_cte
                    per.fecha_nac_const  = fecha_nac_const
                    per.pais_nac_const   = pais_nac_const 
                    per.nacionalidad     = nacionalidad 
                    per.ind_pers_migrada = ind_pers_migrada
                    per.usuario_mod      = user
                    per.fecha_mod        = obtener_fecha_actual() 
                    if tipo_per == 'F':
                        per.ap_paterno   = ap_paterno
                        per.ap_materno   = ap_materno
                        per.genero       = genero
                        per.ocupacion    = ocupacion
                        per.giro         = ' '              
                        per.estado_civil = estado_civil 
                        per.num_reg_mercantil  = 0 
                    else:
                        per.ap_paterno   = ' '
                        per.ap_materno   = ' '
                        per.genero       = ' '
                        per.ocupacion    = ' '
                        per.giro         = giro
                        per.ind_pep      = ' '
                        per.estado_civil = ' '
                        per.num_reg_mercantil = num_reg_mercantil
                    
                    db.add(per)     
                    db.commit() 
                    result.append({'response':'Exito'
                                })        
                    rc = 201 
                    #print('result :', result) 
                    message = json.dumps(result)
                    app_state.jsonenv = message  
                    #print('message :', message) 
                    encripresult , estado = encrypt_message(password, message)
                    #print('encripresult :', encripresult)
                    if estado:
                        return encripresult, rc
                    else:
                        result = [{'response': 'Error encriptando la respuesta'}]
                        return result, 500
                    #result = encripresult
                except Exception as e:
                    print ('e',e)
                    result.append({'response':'Error DB'}) 
                    estado = False
                    rc = 400      
            else:
                result.append({'response':'registro inexistente'}) 
                estado = False
                rc = 400                            
        return result , rc
    
    def delper(self, datos , user, db):
        result = []
        estado = True
        access_token = ''
        message = datos
        message = str(message).replace('"','')
        if not settings.PASSCYPH or settings.PASSCYPH == "default-password":
            logger.error("PASSCYPH no configurado en .env")
            return None, 500
        else:
            password = settings.PASSCYPH
            
            decriptMsg , estado = decrypt_message(password, message)
            #print('decriptMsg :', decriptMsg)
            app_state.jsonrec = decriptMsg

        if estado :
            dataJSON = json.loads(decriptMsg)
            param_keys = ['num_id']
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'})                     
                rc = 400 
            else:                
                num_id = dataJSON["num_id"]             
        else:
            estado = False
            result = 'informacion incorrecta'
            rc = 400
        if estado:
            if not valcampo('longEQ',num_id, 10):
                result.append({'response':'num_id'})
                estado = False
                rc = 400
                #print('result :', result)
                return result , rc
                                   
        if estado :                                               
            per  = db.query(DBDGENPERS).filter_by(num_id=num_id, estatus='A').first()    
            if per:
            # Baja lógica de la persona
                per.estatus = 'B'
                per.fecha_mod = obtener_fecha_actual()
                per.usuario_mod = user
                db.add(per)

            # Baja lógica de los datos complementarios de la persona
                dcomp = db.query(DBDCOMPERS).filter_by(num_id=num_id, estatus='A').first()    
                if dcomp:
                    dcomp.estatus = 'B'
                    dcomp.fecha_mod = obtener_fecha_actual()
                    dcomp.usuario_mod = user
                    db.add(dcomp)

            # Baja lógica de las direcciones asociadas
                direcciones = db.query(DBDIRPERS).filter(DBDIRPERS.num_id == num_id,DBDIRPERS.estatus == 'A').all()
                if direcciones:
                    for dir in direcciones:
                        dir.estatus = 'B'
                        dir.fecha_mod = obtener_fecha_actual()
                        dir.usuario_mod = user
                        db.add(dir)

            # Baja lógica de las relaciones asociadas
                relaciones = db.query(DBRELPERS).filter(DBRELPERS.num_id_princ == num_id,DBRELPERS.estatus == 'A').all()
                if relaciones:
                    for rel in relaciones:
                        rel.estatus = 'B'
                        rel.fecha_mod = obtener_fecha_actual()
                        rel.usuario_mod = user
                        db.add(rel)

            # Baja lógica de los documentos asociadas
                documentos = db.query(DBDOCPERS).filter(DBDOCPERS.num_id == num_id,DBDOCPERS.estatus == 'A').all()
                if documentos:
                    for doc in documentos:
                        doc.estatus = 'B'
                        doc.fecha_mod = obtener_fecha_actual()
                        doc.usuario_mod = user
                        db.add(doc)

             # Baja lógica de las cuentas asociadas
                cuentas = db.query(DBCTAPERS).filter(DBCTAPERS.tknper == per.tknper,DBCTAPERS.estatus == 'A').all()
                if cuentas:
                    for cta in cuentas:
                        cta.estatus = 'B'
                        cta.fecha_mod = obtener_fecha_actual()
                        cta.usuario_mod = user
                        db.add(cta)
                
                # # Confirma todos los cambios en la base de datos
                db.commit()

                result.append({'response': 'Exito'})
                rc = 201
                message = json.dumps(result)
                app_state.jsonenv = message
                encripresult, estado = encrypt_message(password, message)
                if estado:
                    return encripresult, rc
                else:
                    result = [{'response': 'Error encriptando la respuesta'}]
                    return result, 500
            else:
                result.append({'response': 'persona inexistente'})
                estado = False
                rc = 400                          
        return result , rc 
    