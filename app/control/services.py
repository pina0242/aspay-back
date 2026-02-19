

from app.core.models import DBTRNAUT ,User , DBUSETRAN , DBALERTS ,DBENTIDAD, DBTRNAUTUSR , DBSERVAUT ,DBCOSTTRAN ,DB_BATCH_CONFIG
from app.core.cypher import encrypt_message , decrypt_message
from app.core.cai import valcampo , obtener_fecha_actual ,cargar_costos, calcula_costo_txs
from app.core.config import settings
from app.core.state import app_state
from app.entidad.services import EntService
from sqlalchemy import  func , distinct , and_

from datetime import datetime , date



import logging
import json 

import pyotp

logger = logging.getLogger(__name__)
class ControlService:
    
    def __init__(self, db_session=None):
        self.db = db_session
    

    
    def lisauts(self,datos, user ,db):
        result = []
        estado = True
        message = datos
        message = str(message).replace('"','')
        print(' user :', user)
        print (' self.entidad :', app_state.entidad)
        if not settings.PASSCYPH or settings.PASSCYPH == "default-password":
            logger.error("PASSCYPH no configurado en .env")
            return None, 500
        else:
            password = settings.PASSCYPH
            decriptMsg , estado = decrypt_message(password, message)
            app_state.jsonrec = decriptMsg

        if estado :
            dataJSON = json.loads(decriptMsg)
            param_keys = ['fecha_inicio' , 'fecha_fin']
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'})                     
                rc = 400 
            else:                
                fecha_inicio = dataJSON["fecha_inicio"]   
                fecha_fin = dataJSON["fecha_fin"]                                                                              
                print ('fecs :', fecha_inicio  , fecha_fin )
        else:
            estado = False
            result = 'informacion incorrecta'
            rc = 400
        if estado:
            validations = {
                'fecha_inicio': ('date', 10),
                'fecha_fin': ('date', 10)
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
            entidad =  app_state.entidad
            usuarID = 0
            selUsr =  db.query(User).where(User.clvcol==user , User.tipo=='A').first()
            if selUsr:
                usuarID = selUsr.id
                print(' usuarID:', usuarID)


            auths = db.query(DBTRNAUT).filter(
                DBTRNAUT.entidad == entidad,
                func.date(DBTRNAUT.fecha_alta).between(fecha_inicio, fecha_fin)
            ).order_by(
                DBTRNAUT.id.desc()
            ).all()

            if auths:
                for selauths in auths:
                    #print ('ok')
                    result.append({ 
                        'id':selauths.id,
                        'folauth':selauths.folauth, 
                        'Servicio':selauths.Servicio,
                        'status':selauths.status,
                        'DatosIn':selauths.DatosIn,
                        'emailauth':selauths.emailauth,
                        'fecha_alta':str(selauths.fecha_alta),    
                        'usuario_alt':str(selauths.usuario_alt)                                                                                                    
                            })
                rc = 201 
                #print('result :', result) 
                message = json.dumps(result) 
                app_state.jsonenv = message

                encripresult , estado = encrypt_message(password, message)
                #print('encripresult :', encripresult)
                result = encripresult
            else:
                result.append({'response':'No existen datos a listar'})                  
                rc = 400         
        return result , rc 
    
    def listusrauts(self,datos, user ,db):
        result = []
        estado = True
        message = datos
        message = str(message).replace('"','')
        print(' user :', user)
        print (' self.entidad :', app_state.entidad)
        if not settings.PASSCYPH or settings.PASSCYPH == "default-password":
            logger.error("PASSCYPH no configurado en .env")
            return None, 500
        else:
            password = settings.PASSCYPH
            decriptMsg , estado = decrypt_message(password, message)
            app_state.jsonrec = decriptMsg

        if estado :
            dataJSON = json.loads(decriptMsg)
            param_keys = ['fecha_inicio' , 'fecha_fin']
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'})                     
                rc = 400 
            else:                
                fecha_inicio = dataJSON["fecha_inicio"]   
                fecha_fin = dataJSON["fecha_fin"]                                                                              
                print ('fecs :', fecha_inicio  , fecha_fin )
        else:
            estado = False
            result = 'informacion incorrecta'
            rc = 400
        if estado:
            validations = {
                'fecha_inicio': ('date', 10),
                'fecha_fin': ('date', 10)
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
            entidad =  app_state.entidad

            usrauths = db.query(DBTRNAUTUSR).filter(
                DBTRNAUTUSR.entidad == entidad,
                DBTRNAUTUSR.status == 'A',
                func.date(DBTRNAUTUSR.fecha_alta).between(fecha_inicio, fecha_fin)
            ).order_by(
                DBTRNAUTUSR.id.desc()
            ).all()
            
            if usrauths:
                for selauths in usrauths:
                    #print ('ok')
                    result.append({ 
                        'id':selauths.id,
                        'entidad':selauths.entidad,
                        'iduser':selauths.iduser,
                        'Servicio':selauths.Servicio,
                        'status':selauths.status,
                        'emailauth':selauths.emailauth,    
                        'fecha_alta':str(selauths.fecha_alta)                                                                                                    
                            })
                rc = 201 
                print('result :', result) 
                message = json.dumps(result) 
                app_state.jsonenv = message

                encripresult , estado = encrypt_message(password, message)
                #print('encripresult :', encripresult)
                result = encripresult
            else:
                result.append({'response':'No existen datos a listar'}) 
                print('result :', result)                  
                rc = 400    
                message = json.dumps(result)
                app_state.jsonenv = message     
        return result , rc 
    
    def regusraut(self,datos, user, db):
            result = []
            estado = True
            message = datos
            message = str(message).replace('"','')
            print('message :', message)
            if not settings.PASSCYPH or settings.PASSCYPH == "default-password":
                logger.error("PASSCYPH no configurado en .env")
                return None, 500
            else:
                password = settings.PASSCYPH
                decriptMsg , estado = decrypt_message(password, message)
                app_state.jsonrec = decriptMsg
                print('decriptMsg :', decriptMsg)
            if estado :
                #entidad, iduser ,Servicio ,status ,emailauth,usuario_alt
                dataJSON = json.loads(decriptMsg)
                param_keys = ['Servicio' , 'emailauth']
                if not all (key in dataJSON for key in param_keys):
                    estado = False
                    result.append({'response':'Algun elemento de la transacción esta faltando'}) 
                    message = json.dumps(result)
                    app_state.jsonenv = message                      
                    rc = 400 
                else:                  
                    Servicio = dataJSON["Servicio"]
                    emailauth = dataJSON["emailauth"]         
            else:
                estado = False
                result = 'informacion incorrecta'
                message = json.dumps(result)
                app_state.jsonenv = message  
                rc = 400



            if estado:
                validations = {
                'Servicio': ('long', 20),
                'emailauth': ('long', 10)
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
                status      = 'A'  
                usuario_alt =   user    
                iduser      = 0   
                entidad     =  app_state.entidad 
                fecha_mod   = '0001-01-01'   
                usuario_mod = ''                              
                regUsrAut = DBTRNAUTUSR(entidad, iduser ,Servicio ,status ,emailauth,usuario_alt,fecha_mod,usuario_mod)
                db.add(regUsrAut) 
                db.commit()

                result.append({'response':'Exito'
                                    })        
                rc = 201 
                message = json.dumps(result)
                app_state.jsonenv = message
                encripresult , estado = encrypt_message(password, message)
                result = encripresult

            return result , rc 

    def updusrauts(self,datos, user, db):
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

        if estado :
            dataJSON = json.loads(decriptMsg)
            param_keys = ['id','Servicio' , 'emailauth']
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'})  
                message = json.dumps(result)
                app_state.jsonenv = message                     
                rc = 400 
            else:                
                id = dataJSON["id"] 
                Servicio = dataJSON["Servicio"]  
                emailauth = dataJSON["emailauth"]                 
        else:
            estado = False
            result = 'informacion incorrecta'
            message = json.dumps(result)
            app_state.jsonenv = message  
            rc = 400

        if estado :
            validations = {
                'id': ('num', 1),
                'Servicio': ('long', 20),
                'emailauth': ('long', 10)
            }
            for key, (tipo, limite) in validations.items():
                    if not valcampo(tipo, dataJSON[key], limite):
                        response_key = key

                        result = [{'response': response_key}]
                        rc = 400
                        message = json.dumps(result)
                        app_state.jsonenv = message
                        return result, rc   
        if  estado:
                lla = db.query(DBTRNAUTUSR).where(DBTRNAUTUSR.id==id,DBTRNAUTUSR.status=='A').first()
                if lla:
                    campos_originales = {}
                    for col in lla.__table__.columns:
                        if col.name not in ['id','fecha_alta']:
                            campos_originales[col.name] = getattr(lla, col.name)
                    campos_originales['status'] = 'M'
                    campos_originales['usuario_mod'] = user
                    print('campos_originales:', campos_originales)
                    tcorp_historico = DBTRNAUTUSR(**campos_originales)
                    db.add(tcorp_historico)

                    lla.entidad          = app_state.entidad
                    lla.Servicio         = Servicio
                    lla.emailauth        = emailauth
                    lla.fecha_mod        = obtener_fecha_actual()
                    lla.usuario_mod      = app_state.user_id
                    db.add(lla)
                    db.commit() 
                    result.append({'response':'Exito'})   
                    rc = 201 
                    #print('result :', result) 
                    message = json.dumps(result) 
                    app_state.jsonenv = message
                    encripresult , estado = encrypt_message(password, message)
                    #print('encripresult :', encripresult)
                    result = encripresult         
            
        return result ,rc
    
    def delusrauts(self,datos, user, db):
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
        if estado :
            dataJSON = json.loads(decriptMsg)
            param_keys = ['id']
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'}) 
                message = json.dumps(result)
                app_state.jsonenv = message                      
                rc = 400 
            else:                
                id = dataJSON["id"]             
        else:
            estado = False
            result = 'informacion incorrecta'
            message = json.dumps(result)
            app_state.jsonenv = message  
            rc = 400

        if estado:
            if not valcampo('num',id,1):
                result.append({'response':'id'})
                estado = False
                rc = 400
                message = json.dumps(result)
                app_state.jsonenv = message  
                print('result :', result)
                return result , rc
                                   
        if estado :                                                          
            delUsr =  db.query(DBTRNAUTUSR).where(DBTRNAUTUSR.id==id,DBTRNAUTUSR.status=='A').first()   
            if delUsr:

                campos_originales = {}
                for col in delUsr.__table__.columns:
                    if col.name not in ['id','fecha_alta']:
                        campos_originales[col.name] = getattr(delUsr, col.name)
                campos_originales['status'] = 'B'
                #campos_originales['usuario_mod'] = user
                #print('campos_originales:', campos_originales)
                tcorp_historico = DBTRNAUTUSR(**campos_originales)
                db.add(tcorp_historico)

                db.delete(delUsr)
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
                result = encripresult
            else:
                result.append({'response':'registro inexistente'}) 
                message = json.dumps(result)
                app_state.jsonenv = message  
                estado = False
                rc = 400                            
        return result , rc 


    def valotp(self,datos, user ,db):
        result = []
        message = datos
        message = str(message).replace('"','')
        print(' message :', message)
        if not settings.PASSCYPH or settings.PASSCYPH == "default-password":
            logger.error("PASSCYPH no configurado en .env")
            return None, 500
        else:
            password = settings.PASSCYPH
            decriptMsg , estado = decrypt_message(password, message)
            app_state.jsonrec = decriptMsg
        print( 'decriptMsg :', decriptMsg)
        if estado :
            dataJSON = json.loads(decriptMsg)
            param_keys = ['codigo']
            if not all (key in dataJSON for key in param_keys):
                estado = False   
                result.append({'response':'Algun elemento de la transacción esta faltando'})                                  
                rc = 400    
            else:                
                codigo = dataJSON["codigo"]                                                            
        else:
            estado = False
            result = 'informacion incorrecta'
            rc = 400

        if estado:
            if not valcampo('long',codigo,6):
                result.append({'response':'codigo'}) 
                estado = False
                rc = 400
                message = json.dumps(result)
                app_state.jsonenv = message
                return result , rc 
        if estado : 
            user = db.query(User).where(User.clvcol==user , User.tipo=='A').first()
            
            if user:
                sk = user.sk
                estado = True
            else:
                result.append({'response':'Usuario Inválido'}) 
                estado = False
                rc = 400
                return result , rc 

        if estado :
                secret_key = sk
                print('secret_key :', secret_key)

                access = secret_key
                totp = pyotp.TOTP(access)
                current_otp = totp.now()
                print(f"Código TOTP actual: {current_otp}")
                if totp.verify(codigo):
                    print("Código correcto. ¡Autenticación exitosa!")
                              
                    rc = 201
                    respOTP = 'ok'
                else:
                    print("Código incorrecto. Inténtalo de nuevo.")
                    result.append({'response':'Código incorrecto. Inténtalo de nuevo.'}) 
                    message = json.dumps(result)
                    app_state.jsonenv = message
                    estado = False
                    rc = 400
                    return result , rc 


                result.append({'response':respOTP
                                        })        
                 
                #    print('result :', result) 
                message = json.dumps(result)
                app_state.jsonenv = message

                encripresult , estado = encrypt_message(password, message)
                #print('encripresult :', encripresult)
                result = encripresult
                #    result = response
        else:
            result = []
            result.append({'response':'Usuario Inválido'}) 
            estado = False
            rc = 400

        return result , rc 
    
    def valauts(self,db,user,emailauth , Servicio , folauth):
        iduser = 0
        status = 'A'
        stat = True
        msgaut = 'Exito'
        print(' folauth :', folauth)
        entidad =  app_state.entidad
        try:
            aprobadores = db.query(func.count()).filter(
            DBTRNAUTUSR.entidad == entidad,
            DBTRNAUTUSR.status == 'A',
            DBTRNAUTUSR.Servicio == Servicio
            ).scalar()

            print ( ' aprobadores :', aprobadores)



            usrauths = db.query(DBTRNAUTUSR).filter(
                DBTRNAUTUSR.entidad == entidad,
                DBTRNAUTUSR.Servicio == Servicio,
                DBTRNAUTUSR.emailauth ==user,
                DBTRNAUTUSR.status == 'A'
            ).order_by(
                DBTRNAUTUSR.id.desc()
            ).all()
            
            if usrauths:
                for selauths in usrauths:
                    print (' Servicio: ' , selauths.Servicio)
                    emailauth = user
                    existe = db.query(DBSERVAUT).filter(
                        and_(
                            DBSERVAUT.entidad == entidad,
                            DBSERVAUT.folauth == folauth,
                            DBSERVAUT.Servicio == Servicio,
                            DBSERVAUT.status == status,
                            DBSERVAUT.emailauth == emailauth
                        )
                    ).first()
                    if existe:
                        stat = False
                        msgaut = 'autorización ya registrada'
                        print ('')
                    else:
                        regServAut = DBSERVAUT(entidad, iduser, folauth ,Servicio ,status ,emailauth)
                        db.add(regServAut) 
                        db.commit()
            else:
                stat = False
                msgaut = 'No es un usuario autorizador'
            if stat:
                aprobados = db.query(func.count()).filter(
                DBSERVAUT.entidad == entidad,
                DBSERVAUT.status == 'A',
                DBSERVAUT.folauth == folauth,
                DBSERVAUT.Servicio == Servicio
                ).scalar()
                if aprobadores != aprobados:
                    stat = False
                    msgaut = 'Tiene autorizaciones pendiente'


                print ( ' aprobados :', aprobados)
                    

        except Exception as e:
            stat = False
            print('Error:', e)
            msgaut = 'ooops ocurrió un error'
            


        return stat , msgaut


    def resaut(self,datos, user ,db):
        result = []
        estado = True
        message = datos
        message = str(message).replace('"','')
        print(' user :', user)

        if not settings.PASSCYPH or settings.PASSCYPH == "default-password":
            logger.error("PASSCYPH no configurado en .env")
            return None, 500
        else:
            password = settings.PASSCYPH
            decriptMsg , estado = decrypt_message(password, message)
            app_state.jsonrec = decriptMsg

        if estado :
            dataJSON = json.loads(decriptMsg)
            param_keys = ['folauth']
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'})                     
                rc = 400 
            else:                
                folauth = dataJSON["folauth"]   
        else:
            estado = False
            result = 'informacion incorrecta'
            rc = 400
        if estado:
            validations = {
                'folauth': ('num', 1)
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
            selaut =  db.query(DBTRNAUT).where( DBTRNAUT.folauth==folauth, DBTRNAUT.status=='P').first()
            if selaut:
                print ('va a llamar a validacion de autorizaciones')
                autoriza, msgaut=self.valauts(db,user,selaut.emailauth,selaut.Servicio ,selaut.folauth )
                if autoriza:
                    selaut.status       = 'R'
                    selaut.emailauth    =  user  
                    db.add(selaut) 
                    db.commit() 
                    result.append({ 
                            'respose':'Autorización Resuelta'                                                                                                 
                                })
                    rc = 201 
                    #print('result :', result) 
                    message = json.dumps(result) 
                    app_state.jsonenv = message

                    encripresult , estado = encrypt_message(password, message)
                    #print('encripresult :', encripresult)
                    result = encripresult
                else:
                    result.append({'response':msgaut }) 
                    message = json.dumps(result)
                    app_state.jsonenv = message
                    estado = False
                    rc = 400
                    return result , rc 

            else:

                result.append({'response':'No existen datos a listar'}) 
                message = json.dumps(result)
                app_state.jsonenv = message                 
                rc = 400         
        return result , rc 


    
    def listmon(self,datos, user ,db):
        result = []
        estado = True
        message = datos
        message = str(message).replace('"','')
        
        entidad =  app_state.entidad
        if not settings.PASSCYPH or settings.PASSCYPH == "default-password":
            logger.error("PASSCYPH no configurado en .env")
            return None, 500
        else:
            password = settings.PASSCYPH
            decriptMsg , estado = decrypt_message(password, message)
            app_state.jsonrec = decriptMsg

        if estado :
            dataJSON = json.loads(decriptMsg)
            param_keys = ['fecha_inicio' , 'fecha_fin']
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'})                     
                rc = 400 
            else:                
                fecha_inicio = dataJSON["fecha_inicio"]   
                fecha_fin = dataJSON["fecha_fin"]                                                                              
                print ('fecs :', fecha_inicio  , fecha_fin )
        else:
            estado = False
            result = 'informacion incorrecta'
            rc = 400
        if estado:
            validations = {
                'fecha_inicio': ('date', 10),
                'fecha_fin': ('date', 10)
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

            stmt = db.query(
                DBALERTS.entidad,
                DBALERTS.indmon,
                DBALERTS.Servicio,
                func.count().label("cantidad")
            ).filter(
                DBALERTS.entidad == entidad,
                func.date(DBALERTS.fecha_alta) >= fecha_inicio,
                func.date(DBALERTS.fecha_alta) <= fecha_fin
            ).group_by(
                DBALERTS.entidad,
                DBALERTS.indmon,
                DBALERTS.Servicio
            ).all()



            if stmt:
                for selusertran in stmt:
                    #print ('ok')
                    result.append({ 
                        'entidad':selusertran.entidad,
                        'indmon':selusertran.indmon, 
                        'Servicio':selusertran.Servicio,
                        'cantidad':selusertran.cantidad                                                                                                   
                            })
                rc = 201 
                #print('result :', result) 
                message = json.dumps(result) 
                app_state.jsonenv = message

                encripresult , estado = encrypt_message(password, message)
                #print('encripresult :', encripresult)
                result = encripresult
            else:
                result.append({'response':'No existen datos a listar'})                  
                rc = 400         
        return result , rc 

    def listent(self,datos, user ,db):
        result = []
        estado = True
        message = datos
        message = str(message).replace('"','')
        
        entidad =  app_state.entidad
        if not settings.PASSCYPH or settings.PASSCYPH == "default-password":
            logger.error("PASSCYPH no configurado en .env")
            return None, 500
        else:
            password = settings.PASSCYPH
            decriptMsg , estado = decrypt_message(password, message)
            app_state.jsonrec = decriptMsg

        if estado :
            dataJSON = json.loads(decriptMsg)
            param_keys = ['fecha_inicio' , 'fecha_fin']
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'})                     
                rc = 400 
            else:                
                fecha_inicio = dataJSON["fecha_inicio"]   
                fecha_fin = dataJSON["fecha_fin"]                                                                              
                print ('fecs :', fecha_inicio  , fecha_fin )
        else:
            estado = False
            result = 'informacion incorrecta'
            rc = 400
        if estado:
            validations = {
                'fecha_inicio': ('date', 10),
                'fecha_fin': ('date', 10)
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

            stmt = (
                db.query(
                    distinct(DBENTIDAD.entidad),
                    DBENTIDAD.nombre
                )
            )
            regsent = stmt.all()

            if regsent:
                for selent in regsent:
                    
                    result.append({
                        'entidad': selent[0],  
                        'nombre': selent[1]    
                    })
                rc = 201 
                #print('result :', result) 
                message = json.dumps(result) 
                app_state.jsonenv = message

                encripresult , estado = encrypt_message(password, message)
                #print('encripresult :', encripresult)
                result = encripresult
            else:
                result.append({'response':'No existen datos a listar'})                  
                rc = 400         
        return result , rc 
    
    def listustran(self,datos, user ,db):
        result = []
        estado = True
        message = datos
        message = str(message).replace('"','')
        
        entidad =  app_state.entidad
        if not settings.PASSCYPH or settings.PASSCYPH == "default-password":
            logger.error("PASSCYPH no configurado en .env")
            return None, 500
        else:
            password = settings.PASSCYPH
            decriptMsg , estado = decrypt_message(password, message)
            app_state.jsonrec = decriptMsg

        if estado :
            dataJSON = json.loads(decriptMsg)
            param_keys = ['fecha_inicio' , 'fecha_fin']
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'})                     
                rc = 400 
            else:                
                fecha_inicio = dataJSON["fecha_inicio"]   
                fecha_fin = dataJSON["fecha_fin"]                                                                              
                print ('fecs :', fecha_inicio  , fecha_fin )
        else:
            estado = False
            result = 'informacion incorrecta'
            rc = 400
        if estado:
            validations = {
                'fecha_inicio': ('date', 10),
                'fecha_fin': ('date', 10)
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

            stmt = db.query(
                DBUSETRAN.entidad,
                DBUSETRAN.indcost,
                DBUSETRAN.Servicio,
                func.count().label("cantidad")
            ).filter(
                DBUSETRAN.entidad == entidad,
                func.date(DBUSETRAN.fecha_alta) >= fecha_inicio,
                func.date(DBUSETRAN.fecha_alta) <= fecha_fin
            ).group_by(
                DBUSETRAN.entidad,
                DBUSETRAN.indcost,
                DBUSETRAN.Servicio
            ).all()


            if stmt:
                costos = cargar_costos(entidad, db)
                costo_total = 0
                result = []
                
                # Verificar si hay indcost 'E'
                costo_e_valor = 0
                tiene_indcost_e = False
                
                if not costos.empty:
                    e_data = costos[costos['indcost'] == 'E']
                    if not e_data.empty:
                        tiene_indcost_e = True
                        costo_e_valor = float(e_data.iloc[0]['costo_tx'])
                
                for selusertran in stmt:
                    if tiene_indcost_e:
                        # Si hay 'E', todos los costos son 0
                        costo = 0
                    else:
                        # Si no hay 'E', calcular normalmente (excluyendo 'A')
                        if not costos.empty and selusertran.indcost != 'A':
                            costo = calcula_costo_txs(costos, selusertran.indcost, selusertran.cantidad)
                        else:
                            costo = 0
                        
                        costo_total += costo
                    
                    result.append({ 
                       'entidad': selusertran.entidad,
                       'indcost': selusertran.indcost, 
                       'Servicio': selusertran.Servicio,
                       'cantidad': selusertran.cantidad,
                       'costo': round(costo, 2)
                       })
                
                # Establecer total final
                if tiene_indcost_e:
                    costo_total = costo_e_valor

                result.append({
                    'entidad': 'TOTAL',
                    'indcost': '',
                    'Servicio': '',
                    'cantidad': '',
                    'costo': round(costo_total, 2)
                    })
                rc = 201 
                message = json.dumps(result) 
                app_state.jsonenv = message
                encripresult, estado = encrypt_message(password, message)
                result = encripresult
            else:
                result = [{'response': 'No existen datos a listar'}]                  
                rc = 400     
        return result , rc 



    def regcosto(self,datos, user, db):
            result = []
            estado = True
            message = datos
            message = str(message).replace('"','')
            print('message :', message)
            if not settings.PASSCYPH or settings.PASSCYPH == "default-password":
                logger.error("PASSCYPH no configurado en .env")
                return None, 500
            else:
                password = settings.PASSCYPH
                decriptMsg , estado = decrypt_message(password, message)
                app_state.jsonrec = decriptMsg
                print('decriptMsg :', decriptMsg)
            if estado :
                #entidad, iduser ,Servicio ,status ,emailauth,usuario_alt
                dataJSON = json.loads(decriptMsg)
                param_keys = ['entidad' , 'indcost', 'num_txs_libres', 'costo_tx']
                if not all (key in dataJSON for key in param_keys):
                    estado = False
                    result.append({'response':'Algun elemento de la transacción esta faltando'}) 
                    message = json.dumps(result)
                    app_state.jsonenv = message                      
                    rc = 400 
                else:                  
                    entidad = dataJSON["entidad"]
                    indcost = dataJSON["indcost"]    
                    num_txs_libres = dataJSON["num_txs_libres"]    
                    costo_tx = dataJSON["costo_tx"]         
            else:
                estado = False
                result = 'informacion incorrecta'
                message = json.dumps(result)
                app_state.jsonenv = message  
                rc = 400

            if estado:
                validations = {
                'entidad': ('long',8),
                'indcost': ('long', 1),
                'num_txs_libres': ('long', 10),
                'costo_tx': ('float', 15)
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
                    #entidad      =  app_state.entidad
                    # Buscar si ya existe un costo para el mismo indicador de tipo de transaccion
                    cost_existente = db.query(DBCOSTTRAN).where(
                        DBCOSTTRAN.entidad == entidad,
                        DBCOSTTRAN.indcost == indcost,
                        DBCOSTTRAN.estatus == 'A'
                        ).all()
                
                    if cost_existente:
                        estado = False
                        result.append({'response': f'Ya existe registro para tipo de transaccion {indcost} para esta entidad'})
                        rc = 400
                        return result, rc
                    
                except Exception as e:
                    print('Error en validación de costo existente:', e)
                    estado = False
                    result.append({'response': 'Error en busqueda costo'})
                    rc = 400
                    return result, rc

            if estado :                
                estatus      = 'A'          
                usuario_alta =  user      
                usuario_mod = ' '           
                fecha_mod    = datetime.min
                regcosto = DBCOSTTRAN(entidad, indcost, num_txs_libres, costo_tx,estatus,usuario_alta, fecha_mod, usuario_mod)
                db.add(regcosto) 
                db.commit()

                result.append({'response':'Exito'
                                    })        
                rc = 201 
                message = json.dumps(result)
                app_state.jsonenv = message
                encripresult , estado = encrypt_message(password, message)
                result = encripresult

            return result , rc 
    
    def listcosto(self, datos , user, db):
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
            entidad = dataJSON.get("entidad")

            if not entidad:
                #entidad      =  app_state.entidad
                estado = False
                result.append({'response': 'Debe proporcionar entidad.'})
                rc = 400
        else:
            estado = False
            result = 'informacion incorrecta'
            rc = 400

        if estado:                                               
            costos_ent  = db.query(DBCOSTTRAN).order_by(DBCOSTTRAN.id.desc()).where(DBCOSTTRAN.entidad == entidad,DBCOSTTRAN.estatus=='A').all() 

            if costos_ent :
                for entry in costos_ent :
                    
                    fecha_alta_str = entry.fecha_alta.isoformat() if isinstance(entry.fecha_alta, (date, datetime)) else str(entry.fecha_alta)
                    fecha_mod_str = entry.fecha_mod.isoformat() if isinstance(entry.fecha_mod, (date, datetime)) else str(entry.fecha_mod)
                    result.append({
                        'id': entry.id,
                        'entidad': entry.entidad,
                        'indcost': entry.indcost,
                        'num_txs_libres': entry.num_txs_libres,
                        'costo_tx': entry.costo_tx,
                        'fecha_alta': fecha_alta_str,
                        'usuario_alta': entry.usuario_alta,
                        'fecha_mod': fecha_mod_str,
                        'usuario_mod': entry.usuario_mod
                        })
                rc = 201
                message = json.dumps(result)
                app_state.jsonenv = message
                encripresult, estado = encrypt_message(password, message)
                result = encripresult
            else:
                result.append({'response': 'No existen datos a listar.'})
                rc = 400
        return result, rc 
    
    def updcosto(self, datos , user, db):
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
            param_keys = ['id','entidad','indcost','num_txs_libres','costo_tx']
            if not all(key in dataJSON for key in param_keys):
                estado = False
                result.append({'response': 'Algun elemento de la transacción esta faltando.'})
                rc = 400
            else:
                id             = dataJSON["id"]
                entidad        = dataJSON["entidad"]
                indcost        = dataJSON["indcost"]
                num_txs_libres = dataJSON["num_txs_libres"]
                costo_tx       = dataJSON["costo_tx"]
        else:
            estado = False
            result = 'informacion incorrecta'
            rc = 400

        if estado:
            validations = {
                'id': ('float', 10),
                'entidad': ('long', 8),
                'indcost': ('long', 1),
                'num_txs_libres': ('long', 10),
                'costo_tx': ('float', 10)
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
            # with self.session_scope() as session:
            entidad = app_state.entidad
            costo_ent = db.query(DBCOSTTRAN).where(
                        DBCOSTTRAN.entidad == entidad,
                        DBCOSTTRAN.indcost == indcost,
                        DBCOSTTRAN.estatus == 'A'
                    ).first()
            if costo_ent:
                try:
                # 1. Crear copia histórica (estatus 'M')
                    campos_originales = {}
                    for col in costo_ent.__table__.columns:
                        if col.name not in ['id', 'fecha_alta']:
                            campos_originales[col.name] = getattr(costo_ent, col.name)
                
                    campos_originales['estatus'] = 'M'  # eStatus histórico
                    campos_originales['usuario_alta'] = user  # Usuario que crea el histórico

                    print('campos_originales :', campos_originales)
                
                    cto_historico = DBCOSTTRAN(**campos_originales)
                    db.add(cto_historico)

                # 2. ACTUALIZAR registro original (mantener estatus 'A')
                    #costo_ent.entidad = entidad
                    #costo_ent.indcost = indcost
                    costo_ent.num_txs_libres = num_txs_libres
                    costo_ent.costo_tx = costo_tx
                    costo_ent.fecha_mod = obtener_fecha_actual()
                    costo_ent.usuario_mod = user
                    db.add(costo_ent)
                    db.commit()
                    result.append({'response': 'Exito'})
                    rc = 201
                    message = json.dumps(result)
                    app_state.jsonenv = message
                    encripresult, estado = encrypt_message(password, message)
                    result = encripresult
                except Exception as e:
                    print ('e',e)
                    result.append({'response':'Error DB'}) 
                    estado = False
                    rc = 400   
            else:
                result.append({'response': 'registro inexistente'})
                estado = False
                rc = 400
        return result, rc   
    
    def delcosto(self, datos , user, db):
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
            param_keys = ['id']
            if not all(key in dataJSON for key in param_keys):
                estado = False
                result.append({'response': 'Algun elemento de la transacción esta faltando.'})
                rc = 400
            else:
                id = dataJSON["id"]
        else:
            estado = False
            result = 'informacion incorrecta'
            rc = 400

        if estado:
            if not valcampo('float',id,1):
                result.append({'response':'id'})
                estado = False
                rc = 400
                print('result :', result)
                return result , rc

        if estado:
            costo_ent  = db.query(DBCOSTTRAN).where(DBCOSTTRAN.id==id).first() 
            if  costo_ent :
                costo_ent .estatus = 'B'
                costo_ent .fecha_mod = obtener_fecha_actual()
                costo_ent .usuario_mod = user
                db.add(costo_ent )
                db.commit()
                result.append({'response': 'Exito'})
                rc = 201
                message = json.dumps(result)
                app_state.jsonenv = message
                encripresult, estado = encrypt_message(password, message)
                result = encripresult
            else:
                result.append({'response': 'registro inexistente'})
                estado = False
                rc = 400
        return result, rc

    def regsched(self,datos, user, db):
            result = []
            estado = True
            message = datos
            message = str(message).replace('"','')
            print('message :', message)
            if not settings.PASSCYPH or settings.PASSCYPH == "default-password":
                logger.error("PASSCYPH no configurado en .env")
                return None, 500
            else:
                password = settings.PASSCYPH
                decriptMsg , estado = decrypt_message(password, message)
                app_state.jsonrec = decriptMsg
                print('decriptMsg :', decriptMsg)
            if estado :
                #entidad, iduser ,Servicio ,status ,emailauth,usuario_alt
                dataJSON = json.loads(decriptMsg)
                param_keys = ['entidad' , 'nombre_proceso', 'task_path', 'configs']
                if not all (key in dataJSON for key in param_keys):
                    estado = False
                    result.append({'response':'Algun elemento de la transacción esta faltando'}) 
                    message = json.dumps(result)
                    app_state.jsonenv = message                      
                    rc = 400 
                else:                  
                    entidad = dataJSON["entidad"]
                    nombre_proceso = dataJSON["nombre_proceso"]    
                    task_path = dataJSON["task_path"]    
                    configs = dataJSON["configs"]         
            else:
                estado = False
                result = 'informacion incorrecta'
                message = json.dumps(result)
                app_state.jsonenv = message  
                rc = 400

            if estado:
                validations = {
                'entidad': ('long',8),
                'nombre_proceso': ('long', 100),
                'task_path': ('long', 200)
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


            if estado :                
                try:
                    for item in configs:
                        if not (
                            (item['minuto'] == '*' or 0 <= int(item['minuto']) <= 59) and
                            (item['hora'] == '*' or 0 <= int(item['hora']) <= 23)     and
                            (item['diaMes'] == '*' or 1 <= int(item['diaMes']) <= 31) and
                            (item['mes'] == '*' or 1 <= int(item['mes']) <= 12)       and
                            (item['diaSemana'] == '*' or 0 <= int(item['diaSemana']) <= 7)   # 0 o 7 es Domingo
                             ):
                            raise ValueError("Rangos de cron no válidos")
                        cron_str = f"{item['minuto']} {item['hora']} {item['diaMes']} {item['mes']} {item['diaSemana']}"
                        
                        sched= DB_BATCH_CONFIG(
                            entidad=entidad,
                            nombre_proceso=nombre_proceso,
                            task_path=task_path,
                            cron_expression=cron_str,
                            is_active=True,
                            ultima_ejecucion=None,
                            ultimo_resultado=None
                        )
                        db.add(sched)
                    db.commit()
                except Exception as e:
                        db.rollback()
                        result.append({'response': f'Error al registrar: {str(e)}'})
                        rc = 400
                        return result , rc 
                result.append({'response':'Exito'})        
                rc = 201 
                message = json.dumps(result)
                app_state.jsonenv = message
                encripresult , estado = encrypt_message(password, message)
                result = encripresult
                
            return result , rc 

    def listsched(self,datos, user, db):
            result = []
            estado = True
            message = datos
            message = str(message).replace('"','')
            print('message :', message)
            if not settings.PASSCYPH or settings.PASSCYPH == "default-password":
                logger.error("PASSCYPH no configurado en .env")
                return None, 500
            else:
                password = settings.PASSCYPH
                decriptMsg , estado = decrypt_message(password, message)
                app_state.jsonrec = decriptMsg
                print('decriptMsg :', decriptMsg)
            if estado :
                #entidad, iduser ,Servicio ,status ,emailauth,usuario_alt
                dataJSON = json.loads(decriptMsg)
                param_keys = ['entidad' , 'task_path']
                if not all (key in dataJSON for key in param_keys):
                    estado = False
                    result.append({'response':'Algun elemento de la transacción esta faltando'}) 
                    message = json.dumps(result)
                    app_state.jsonenv = message                      
                    rc = 400 
                else:                  
                    entidad = dataJSON["entidad"]
                    task_path = dataJSON["task_path"]            
            else:
                estado = False
                result = 'informacion incorrecta'
                message = json.dumps(result)
                app_state.jsonenv = message  
                rc = 400

            if estado:
                validations = {
                'entidad': ('long',8),
                'task_path': ('long', 200)
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


            if estado :                
                registros = db.query(DB_BATCH_CONFIG).filter(
                            DB_BATCH_CONFIG.entidad == entidad,
                            DB_BATCH_CONFIG.task_path == task_path,
                            DB_BATCH_CONFIG.is_active == True
                            ).all()
                proceso_agrupado = {}
                for r in registros:
                    partes = r.cron_expression.split(' ')
                    config_obj = {
                        "id": r.id, 
                        "minuto": partes[0],
                        "hora": partes[1],
                        "diaMes": partes[2],
                        "mes": partes[3],
                        "diaSemana": partes[4]
                    }

                    if r.nombre_proceso not in proceso_agrupado:
                        proceso_agrupado[r.nombre_proceso] = {
                            "entidad": r.entidad,
                            "nombre_proceso": r.nombre_proceso,
                            "task_path": r.task_path,
                            "configs": []
                        }
                    
                    proceso_agrupado[r.nombre_proceso]["configs"].append(config_obj)
                lista_final = list(proceso_agrupado.values())
                message = json.dumps(lista_final)
                rc = 201 
                app_state.jsonenv = message
                encripresult , estado = encrypt_message(password, message)
                result = encripresult
                
            return result , rc 

    def updsched(self,datos, user, db):
            result = []
            estado = True
            message = datos
            message = str(message).replace('"','')
            print('message :', message)
            if not settings.PASSCYPH or settings.PASSCYPH == "default-password":
                logger.error("PASSCYPH no configurado en .env")
                return None, 500
            else:
                password = settings.PASSCYPH
                decriptMsg , estado = decrypt_message(password, message)
                app_state.jsonrec = decriptMsg
                print('decriptMsg :', decriptMsg)
            if estado :
                #entidad, iduser ,Servicio ,status ,emailauth,usuario_alt
                dataJSON = json.loads(decriptMsg)
                param_keys = ['entidad', 'nombre_proceso', 'task_path', 'configs']
                if not all (key in dataJSON for key in param_keys):
                    estado = False
                    result.append({'response':'Algun elemento de la transacción esta faltando'}) 
                    message = json.dumps(result)
                    app_state.jsonenv = message                      
                    rc = 400 
                else:                  
                    entidad = dataJSON["entidad"]
                    nombre_proceso = dataJSON["nombre_proceso"]
                    task_path = dataJSON["task_path"]
                    configs = dataJSON["configs"]         
            else:
                estado = False
                result = 'informacion incorrecta'
                message = json.dumps(result)
                app_state.jsonenv = message  
                rc = 400

            if estado:
                validations = {
                'entidad': ('long',8),
                'nombre_proceso': ('long',100),
                'task_path': ('long', 200)
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


            if estado :                
                try:
                    registros_viejos = db.query(DB_BATCH_CONFIG).filter(
                        DB_BATCH_CONFIG.entidad == entidad,
                        DB_BATCH_CONFIG.nombre_proceso == nombre_proceso,
                        DB_BATCH_CONFIG.task_path == task_path,
                        DB_BATCH_CONFIG.is_active == True
                    ).all()
                    
                    for reg in registros_viejos:
                        
                        reg.is_active = False
                        reg.ultima_ejecucion = obtener_fecha_actual()
                        reg.ultimo_resultado = "Se actualizo"                      
                        db.add(reg)

                    for item in configs:
                        if not (
                            (item['minuto'] == '*' or 0 <= int(item['minuto']) <= 59) and
                            (item['hora'] == '*' or 0 <= int(item['hora']) <= 23) and
                            (item['diaMes'] == '*' or 1 <= int(item['diaMes']) <= 31) and
                            (item['mes'] == '*' or 1 <= int(item['mes']) <= 12) and
                            (item['diaSemana'] == '*' or 0 <= int(item['diaSemana']) <= 7)
                        ):
                            raise ValueError(f"Rango cron inválido para {item}")

                        cron_str = f"{item['minuto']} {item['hora']} {item['diaMes']} {item['mes']} {item['diaSemana']}"
                        
                        nuevo_sched = DB_BATCH_CONFIG(
                            entidad=entidad,
                            nombre_proceso=nombre_proceso,
                            task_path=task_path,
                            cron_expression=cron_str,
                            is_active=True,
                            ultima_ejecucion=None,
                            ultimo_resultado=None
                        )
                        db.add(nuevo_sched)

                    db.commit()
                    result.append({'response': 'Exito - Configuración actualizada de proceso'})
                    rc = 201
                    message = json.dumps(result)
                    app_state.jsonenv = message
                    encripresult , estado = encrypt_message(password, message)
                    result = encripresult

                except Exception as e:
                    db.rollback()
                    result.append({'response': f'Error en actualización: {str(e)}'})
                    rc = 400         
                 
            return result , rc 

    def delsched(self,datos, user, db):
                result = []
                estado = True
                message = datos
                message = str(message).replace('"','')
                print('message :', message)
                if not settings.PASSCYPH or settings.PASSCYPH == "default-password":
                    logger.error("PASSCYPH no configurado en .env")
                    return None, 500
                else:
                    password = settings.PASSCYPH
                    decriptMsg , estado = decrypt_message(password, message)
                    app_state.jsonrec = decriptMsg
                    print('decriptMsg :', decriptMsg)
                if estado :
                    #entidad, iduser ,Servicio ,status ,emailauth,usuario_alt
                    dataJSON = json.loads(decriptMsg)
                    param_keys = ['entidad', 'nombre_proceso', 'task_path']
                    if not all (key in dataJSON for key in param_keys):
                        estado = False
                        result.append({'response':'Algun elemento de la transacción esta faltando'}) 
                        message = json.dumps(result)
                        app_state.jsonenv = message                      
                        rc = 400 
                    else:                  
                        entidad = dataJSON["entidad"]
                        nombre_proceso = dataJSON["nombre_proceso"]
                        task_path = dataJSON["task_path"]      
                else:
                    estado = False
                    result = 'informacion incorrecta'
                    message = json.dumps(result)
                    app_state.jsonenv = message  
                    rc = 400

                if estado:
                    validations = {
                    'entidad': ('long',8),
                    'nombre_proceso': ('long',100),
                    'task_path': ('long', 200)
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


                if estado :                
                    try:
                        registros_viejos = db.query(DB_BATCH_CONFIG).filter(
                            DB_BATCH_CONFIG.entidad == entidad,
                            DB_BATCH_CONFIG.nombre_proceso == nombre_proceso,
                            DB_BATCH_CONFIG.task_path == task_path,
                            DB_BATCH_CONFIG.is_active == True
                        ).all()
                        
                        for reg in registros_viejos:
                            campos_originales = {}
                            for col in reg.__table__.columns:
                                if col.name not in ['id','fecha_alta']:
                                    campos_originales[col.name] = getattr(reg, col.name)
                            campos_originales['is_active'] = False
                            campos_originales['ultima_ejecucion'] = obtener_fecha_actual()
                            campos_originales['ultimo_resultado'] = "Se elimina"

                            print('campos_originales :', campos_originales)
                            
                            cred_historico = DB_BATCH_CONFIG(**campos_originales)
                            db.add(cred_historico)
                            db.delete(reg)

                        db.commit()
                        result.append({'response': 'Exito - Configuración eliminada del proceso'})
                        rc = 201
                        message = json.dumps(result)
                        app_state.jsonenv = message
                        encripresult , estado = encrypt_message(password, message)
                        result = encripresult

                    except Exception as e:
                        db.rollback()
                        result.append({'response': f'Error en actualización: {str(e)}'})
                        rc = 400         
                    
                return result , rc 

    def listschent(self,datos, user, db):
            result = []
            estado = True
            message = datos
            message = str(message).replace('"','')
            print('message :', message)
            if not settings.PASSCYPH or settings.PASSCYPH == "default-password":
                logger.error("PASSCYPH no configurado en .env")
                return None, 500
            else:
                password = settings.PASSCYPH
                decriptMsg , estado = decrypt_message(password, message)
                app_state.jsonrec = decriptMsg
                print('decriptMsg :', decriptMsg)
            if estado :
                #entidad, iduser ,Servicio ,status ,emailauth,usuario_alt
                dataJSON = json.loads(decriptMsg)
                param_keys = ['entidad']
                if not all (key in dataJSON for key in param_keys):
                    estado = False
                    result.append({'response':'Algun elemento de la transacción esta faltando'}) 
                    message = json.dumps(result)
                    app_state.jsonenv = message                      
                    rc = 400 
                else:                  
                    entidad = dataJSON["entidad"]          
            else:
                estado = False
                result = 'informacion incorrecta'
                message = json.dumps(result)
                app_state.jsonenv = message  
                rc = 400

            if estado:
                validations = {
                'entidad': ('long',8)
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


            if estado :                
                registros = db.query(DB_BATCH_CONFIG).filter(
                            DB_BATCH_CONFIG.entidad == entidad,
                            DB_BATCH_CONFIG.is_active == True
                            ).all()
                if registros:
                    procesos_map = {}
                    for r in registros:
                        if r.nombre_proceso not in procesos_map:
                            procesos_map[r.nombre_proceso] = {
                                "entidad": r.entidad,
                                "nombre_proceso": r.nombre_proceso}
                            result.append({ 
                                    'entidad':r.entidad, 
                                    'nombre_proceso':r.nombre_proceso,
                                    'task_path':r.task_path,
                                    'is_active':r.is_active
                                        })
                            
                    rc = 201 
                    message = json.dumps(result) 
                    app_state.jsonenv = message
                    encripresult , estado = encrypt_message(password, message)
                    result = encripresult
                else: 
                    result.append({'response':'No existen datos a listar'})                  
                    message = json.dumps(result)
                    app_state.jsonenv = message   
                    rc = 400         
                
            return result , rc 
