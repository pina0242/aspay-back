
from app.core.models import DBCTAPERS
from app.core.cypher import encrypt_message , decrypt_message
from app.core.cai import valcampo
from app.core.config import settings
from app.core.interfaz import login_al_banco, registra_movto
from app.entidad.services import EntService
from app.core.state import app_state
import logging
import json 



logger = logging.getLogger(__name__)


class TraspService:
    
    def __init__(self, db_session=None):
        self.db = db_session
    
    def realtrasp(self, datos , db): 
        result = []
        estado = True
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
            param_keys = ['entidad' , 'cta_ori', 'alias_ori', 'tipo_ori', 'cta_des', 
                          'alias_des' , 'tipo_des', 'importe', 'fecha_movto', 'concepto']
            if not all (key in dataJSON for key in param_keys):
                estado = False   
                result.append({'response':'Algun elemento de la transacción esta faltando'})    
                message = json.dumps(result)
                app_state.jsonenv = message                                 
                rc = 400   
            else:                                                           
                entidad =  dataJSON["entidad"]
                cta_ori  =  dataJSON["cta_ori"]
                #cta_ori , estado = encrypt_message(password, cta_ori1)
                alias_ori  =  dataJSON["alias_ori"]
                tipo_ori  =  dataJSON["tipo_ori"]
                cta_des  =  dataJSON["cta_des"]
                #cta_des , estado = encrypt_message(password, cta_des1)
                alias_des  =  dataJSON["alias_des"]
                tipo_des  =  dataJSON["tipo_des"]
                importe  =  dataJSON["importe"]
                fecha_movto  =  dataJSON["fecha_movto"]
                concepto  =  dataJSON["concepto"]

        else:
            estado = False
            result = 'informacion incorrecta'
            message = json.dumps(result)
            app_state.jsonenv = message   
            rc = 400

        if estado:
            entidad_usu = app_state.entidad
            valenti = EntService.selent(entidad, db)
            if valenti:
                #entidad_usu = '4000'
                if entidad_usu != '0001':
                    if entidad_usu != entidad:
                        estado = False   
                        result.append({'response':'Solo puede operar su propia entidad'})    
                        message = json.dumps(result)
                        app_state.jsonenv = message                                 
                        rc = 400   

            else:
                if entidad_usu != '0001':
                    estado = False   
                    result.append({'response':'No existe entidad'})    
                    message = json.dumps(result)
                    app_state.jsonenv = message                                 
                    rc = 400   

        if estado:
            if cta_ori == cta_des:
                estado = False   
                result.append({'response':'Traspaso no permitido a la misma cuenta'})    
                message = json.dumps(result)
                app_state.jsonenv = message                                 
                rc = 400   

        if estado :
            validations = {
            'entidad': ('long', 8),
            'cta_ori': ('long', 500),
            'alias_ori': ('long', 10),
            'tipo_ori': ('long', 3),
            'cta_des': ('long', 500),
            'alias_des': ('long', 10),
            'tipo_des': ('long', 3),
            'importe': ('float', 1),
            'fecha_movto': ('date', 10),
            'concepto': ('long', 30)
            }
            for key, (tipo, limite) in validations.items():
                if not valcampo(tipo, dataJSON[key], limite):
                    response_key = key
                    result = [{'response': response_key}]
                    rc = 400
                    message = json.dumps(result)
                    app_state.jsonenv = message
                    return result, rc  
        if estado:
             
             try:
                 euser = app_state.user_id
                 eIp_Origen = app_state.ip_origen
                 etimestar = app_state.time_star
                 headertokenbnc = login_al_banco(db,euser,eIp_Origen,etimestar)      
                 if not headertokenbnc:
                     print('error de token')
                     result.append({'response': 'No se pudo autenticar contra el banco.'})
                     return result, 400
                 rc1 = registra_movto(db, euser, eIp_Origen, etimestar, headertokenbnc,
                                      datosin=cta_ori, alias=alias_ori, tipo=tipo_ori, signo='D',
                                      fecha_movto=fecha_movto, concepto=concepto,
                                      importe=importe, http_local=None)
                 rcc = rc1[0]
                 if rcc not in (200, 201): 
                     result.append({"err": f"Cargo falló (rc={rc1[1]})"})
                     return result, 400
                 rc2 = registra_movto(db, euser, eIp_Origen, etimestar, headertokenbnc,
                                      datosin=cta_des, alias=alias_des, tipo=tipo_des, signo='H',
                                      fecha_movto=fecha_movto, concepto=concepto,
                                      importe=importe, http_local=None)
                 rca = rc2[0]
                 if rca not in (200, 201): 
                     result.append({"err": f"Abono falló (rc={rc2[1]})"})
                     return result, 400
                 
                       
             except Exception as ex:
                        print('error ', ex)
                        result.append({"response": f" Excepción: {ex}"})
                        rc = 400
                        return result, 400    
             result.append({'response':'Exito'})        
             rc = 201 
             message = json.dumps(result)
             app_state.jsonenv = message
             encripresult , estado = encrypt_message(password, message)
             result = encripresult
        return result , rc  
    
    def selctatras(self, datos , db):
        result = []
        estado = True
        message = datos
        message = str(message).replace('"','')
        if not settings.PASSCYPH or settings.PASSCYPH == "default-password":
            logger.error("PASSCYPH no configurado en .env")
            return None, 500
        else:
            password = settings.PASSCYPH
            
            decriptMsg , estado = decrypt_message(password, message)
            app_state.jsonrec = decriptMsg

        if estado:
            dataJSON = json.loads(decriptMsg)
            entidad = dataJSON.get("entidad")

            if not entidad:
                estado = False
                result.append({'response': 'Debe proporcionar entidad.'})
                rc = 400
        else:
            estado = False
            result = 'informacion incorrecta'
            rc = 400

        if estado:
            # with self.session_scope() as session:
                                                
            ctas_entries = db.query(DBCTAPERS).order_by(DBCTAPERS.id.desc()).where(DBCTAPERS.entidad == entidad,
                                                                                   DBCTAPERS.estatus=='A',
                                                                                   DBCTAPERS.indoper.in_(['CO', 'CS'])).all() 

            if ctas_entries:
                for entry in ctas_entries:
                    # Desencriptar el campo 'datos' después de recuperarlo de la DB
                    datos_decrypted, decrypt_estado = decrypt_message(password, entry.datos)
                    if not decrypt_estado:
                        datos_decrypted = "Error al desencriptar datos" # Manejar error de desencriptación

                    result.append({
                        'id': entry.id,
                        'entidad':entry.entidad,
                        'entban': entry.entban,
                        'tipo': entry.tipo,
                        'alias': entry.alias,
                        'datos': datos_decrypted, # Devolver datos desencriptados
                        'indoper': entry.indoper,
                        'estatus': entry.estatus,
                        })
                rc = 201
                message = json.dumps(result)
                app_state.jsonenv = message
                encripresult, estado = encrypt_message(password, message)
                result = encripresult
            else:
                result.append({'response': 'No existen datos a listar con los criterios proporcionados.'})
                rc = 400
        return result, rc 