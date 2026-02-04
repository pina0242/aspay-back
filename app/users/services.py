
from app.core.models import User , DBDGENPERS
from app.core.cypher import encrypt_message , decrypt_message
from app.core.cai import valcampo
from app.core.config import settings
from app.core.state import app_state
from app.core.auth import create_access_token
from app.core.security import hash_password, verify_password
from sqlalchemy import  func
from datetime import timedelta
import logging
import json 
#------------------------------------
import pyotp
import qrcode
from PIL import Image
import io
import base64
#--------------


logger = logging.getLogger(__name__)


class UserService:
    
    def __init__(self, db_session=None):
        self.db = db_session
    
    def asplogin(self, datos , db_session):
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
            param_keys = ['email' , 'password']
            if not all (key in dataJSON for key in param_keys):
                estado = False   
                result.append({'response':'Algun elemento de la transacción esta faltando'})  
                message = json.dumps(result)
                app_state.jsonenv = message                                  
                rc = 400                                                              
        else:
            estado = False
            result = 'informacion incorrecta'
            message = json.dumps(result)
            app_state.jsonenv = message  
            rc = 400
        if estado :
            email =  dataJSON["email"]
            password_provided =  dataJSON["password"]
                   
            user = db_session.query(User).where(User.email==email, User.tipo== 'A').first()
            print ('email', email , ' User:', user.password)    
            if user:
                print('si esta el registro')
                if verify_password(password_provided, user.password):
                    print('Password validado')
                    additional_claims = {
                        'user_id': user.id, 
                        'role': user.nivel,
                        'clvcol':user.clvcol,
                        'entidad':user.entidad        
                    }
                    access_token = create_access_token(
                        identity=user.name,
                        additional_claims=additional_claims,
                        expires_delta=timedelta(hours=24)
                    )            
                    rc = 201
                    result = 'Bearer ' + access_token
                    app_state.jsonenv = result
                else:
                    print (' password Incorrecto')
                    result = 'usuario password Incorrecto'
                    message = json.dumps(result)
                    app_state.jsonenv = message  
                    rc = 400
            else:
                print ('usuario password Incorrecto')
                result = 'usuario password Incorrecto'
                message = json.dumps(result)
                app_state.jsonenv = message  
                rc = 400

        return result , rc   
    
    def listusr(self,datos, db):
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
            param_keys = ['fecha_inicio' , 'fecha_fin']
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'})    
                message = json.dumps(result)
                app_state.jsonenv = message                   
                rc = 400 
            else:                
                fecha_inicio = dataJSON["fecha_inicio"]   
                fecha_fin = dataJSON["fecha_fin"]                                                                              
                print ('fecs :', fecha_inicio  , fecha_fin )
        else:
            estado = False
            result = 'informacion incorrecta'
            message = json.dumps(result)
            app_state.jsonenv = message  
            rc = 400
        if estado:
            validations = {
                'fecha_inicio': ('date', 1),
                'fecha_fin': ('date', 1)
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
            user = db.query(User).filter(
                func.date(User.fecha_alta).between(fecha_inicio, fecha_fin),
                User.tipo == 'A' 
            ).order_by(
                User.id.desc()
            ).all()

            if user:
                for seluser in user:
                    #print ('ok')
                    result.append({ 
                        'id':seluser.id,
                        'entidad':seluser.entidad,
                        'claveColab':seluser.clvcol, 
                        'name':seluser.name,
                        'email':seluser.email,
                        'tel':seluser.tel,
                        'nivel':seluser.nivel,
                        'fecha_alta':str(seluser.fecha_alta)                                                                                                       
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
                message = json.dumps(result)
                app_state.jsonenv = message                 
                rc = 400         
        return result , rc 
    

    def regusr(self,datos, user,entidad, db):
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
                dataJSON = json.loads(decriptMsg)
                param_keys = ['claveColab' , 'name' ,'email','tel','tipo','password','nivel','usuario_alt']
                if not all (key in dataJSON for key in param_keys):
                    estado = False
                    result.append({'response':'Algun elemento de la transacción esta faltando'}) 
                    message = json.dumps(result)
                    app_state.jsonenv = message                      
                    rc = 400 
                else:                
                    claveColab = dataJSON["claveColab"]   
                    name = dataJSON["name"]
                    email = dataJSON["email"] 
                    tel = dataJSON["tel"]
                    tipo = dataJSON["tipo"] 
                    passusr = dataJSON["password"] 
                    nivel = dataJSON["nivel"] 
                    usuario_alt = dataJSON["usuario_alt"]              
            else:
                estado = False
                result = 'informacion incorrecta'
                message = json.dumps(result)
                app_state.jsonenv = message  
                rc = 400



            if estado:
                validations = {
                'claveColab': ('long', 10),
                'name': ('long', 60),
                'email': ('long', 250),
                'tel': ('long', 15),
                'tipo': ('long', 1),
                'password': ('long', 256),
                'nivel': ('float', 1),
                'usuario_alt': ('long', 8)
                }

                for key, (tipo, limite) in validations.items():
                        if not valcampo(tipo, dataJSON[key], limite):
                            response_key = key
                            result = [{'response': response_key}]
                            rc = 400
                            message = json.dumps(result)
                            app_state.jsonenv = message
                            return result, rc   
            if nivel == 0 and entidad != '0001':
                estado = False
                result.append({'response':'Tipo no permitido'}) 
                message = json.dumps(result)
                app_state.jsonenv = message                      
                rc = 400

            if estado:
                per = db.query(DBDGENPERS).where(DBDGENPERS.num_id==claveColab,DBDGENPERS.estatus=='A').first()
                if per:
                    entidad = per.entidad
                else:
                    estado = False
                    result.append({'response':'Persona inexistente o dadada de baja'}) 
                    message = json.dumps(result)
                    app_state.jsonenv = message                      
                    rc = 400

            if estado :
                
                print(f"Password recibido: '{passusr}' (longitud: {len(passusr)})")
                hashed_password = hash_password(passusr)
                passusr = hashed_password
                tipo         = 'A'  
                usuario_alt  =  user    
                sk = ' '                                      
                regUsr = User(entidad,claveColab,name,email,tel,tipo,passusr,nivel,sk,usuario_alt)
                db.add(regUsr) 
                db.commit()

                result.append({'response':'Exito'
                                    })        
                rc = 201 
                message = json.dumps(result)
                app_state.jsonenv = message
                encripresult , estado = encrypt_message(password, message)
                result = encripresult

            return result , rc 

    
    def updusr(self,datos, user,db):
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
            #print('decriptMsg :', decriptMsg)
        if estado :
            dataJSON = json.loads(decriptMsg)
            param_keys = ['idcolab','clvcol','name','email','tel','passuser','nivel','usuario_alt']
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'})  
                message = json.dumps(result)
                app_state.jsonenv = message                     
                rc = 400 
            else:                
                idcolab = dataJSON["idcolab"]
                clvcol = dataJSON["clvcol"] 
                name = dataJSON["name"] 
                email = dataJSON["email"] 
                tel = dataJSON["tel"] 
                passuser = dataJSON["passuser"] 
                nivel = dataJSON["nivel"] 
                usuario_alt = dataJSON["usuario_alt"]              
        else:
            estado = False
            result = 'informacion incorrecta'
            message = json.dumps(result)
            app_state.jsonenv = message  
            rc = 400

        if estado:
            validations = {
                'idcolab': ('float', 1),
                'clvcol': ('long', 10),
                'name': ('long', 60),
                'email': ('long', 256),
                'tel': ('long', 15), 
                'passuser': ('long', 15),
                'nivel': ('float', 1),
                'usuario_alt': ('long', 8)
            }

            for key, (tipo, limite) in validations.items():
                    if not valcampo(tipo, dataJSON[key], limite):
                        response_key = key
                        result = [{'response': response_key}]
                        rc = 400
                        message = json.dumps(result)
                        app_state.jsonenv = message
                        print('result :', result)
                        return result, rc   
            

        if estado :                                                          
            updUsr =  db.query(User).where(User.id==idcolab , User.tipo=='A').first()            
            if updUsr:

                campos_originales = {}
                for col in updUsr.__table__.columns:
                    if col.name not in ['id','fecha_alta']:
                        campos_originales[col.name] = getattr(updUsr, col.name)
                campos_originales['usuario_alt'] = user  
                campos_originales['tipo'] = 'H'
                print('campos_originales :', campos_originales)
                
                cred_historico = User(**campos_originales)
                db.add(cred_historico)

                updUsr.clvcol = clvcol
                updUsr.name   = name
                updUsr.email  = email
                updUsr.tel  = tel
                if passuser != "":
                    print("Cambia PSW" , passuser)
                    updUsr.password = hash_password(passuser)
                updUsr.nivel    = nivel
                updUsr.usuario_alt = user
                db.add(updUsr) 
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
                estado = False
                message = json.dumps(result)
                app_state.jsonenv = message  
                rc = 400                            
        return result , rc
    
    def delusr(self,datos, user,db):
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
            #print('decriptMsg :', decriptMsg)
        if estado :
            dataJSON = json.loads(decriptMsg)
            param_keys = ['idcolab']
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'}) 
                message = json.dumps(result)
                app_state.jsonenv = message                      
                rc = 400 
            else:                
                idcolab = dataJSON["idcolab"]             
        else:
            estado = False
            result = 'informacion incorrecta'
            message = json.dumps(result)
            app_state.jsonenv = message  
            rc = 400

        if estado:
            if not valcampo('float',idcolab,1):
                result.append({'response':'idcolab'})
                estado = False
                rc = 400
                message = json.dumps(result)
                app_state.jsonenv = message  
                print('result :', result)
                return result , rc  
            

        if estado :                                                          
            delUsr =  db.query(User).where(User.id==idcolab , User.tipo=='A').first()            
            if delUsr:

                campos_originales = {}
                for col in delUsr.__table__.columns:
                    if col.name not in ['id','fecha_alta']:
                        campos_originales[col.name] = getattr(delUsr, col.name)
                campos_originales['usuario_alt'] = user  # Usuario que crea el histórico
                campos_originales['tipo'] = 'B'
                print('campos_originales :', campos_originales)
                
                cred_historico = User(**campos_originales)
                db.add(cred_historico)
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
                estado = False
                message = json.dumps(result)
                app_state.jsonenv = message  
                rc = 400                            
        return result , rc

    def registotp(sef, datos , user ,db):
        result = []
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
            param_keys = ['email']
            if not all (key in dataJSON for key in param_keys):
                estado = False   
                result.append({'response':'Algun elemento de la transacción esta faltando'})  
                message = json.dumps(result)
                app_state.jsonenv = message                                  
                rc = 400    
            else:                
                email = dataJSON["email"]                                            
        else:
            estado = False
            result = 'informacion incorrecta'
            message = json.dumps(result)
            app_state.jsonenv = message  
            rc = 400

        if estado:
            if not valcampo('long',email,250):
                result.append({'response':'email'}) 
                estado = False
                message = json.dumps(result)
                app_state.jsonenv = message  
                rc = 400
                return result , rc

        if estado :  
            secret_key = pyotp.random_base32()
            username = email
            app_name = "ASPAY"

            # Generar URI de aprovisionamiento
            uri = pyotp.totp.TOTP(secret_key).provisioning_uri(name=username, issuer_name=app_name)
            
            # Generar código QR
            qr = qrcode.QRCode(
                version=1,
                error_correction=qrcode.constants.ERROR_CORRECT_L,
                box_size=10,
                border=4,
            )
            qr.add_data(uri)
            qr.make(fit=True)
            
            # Crear imagen usando Pillow
            img = qr.make_image(fill_color="black", back_color="white")
            
            # Guardar imagen
            #img.save("2fa_qr_code.png") 

            # Crear respuesta de imagen
            img_io = io.BytesIO()
            img.save(img_io, 'PNG')
            img_io.seek(0)
            response = (img_io.read()) 

        if estado :              
            usuario_alt  =  user 
            sk = secret_key 
                 
            valUsr =  db.query(User).where(User.email==email, User.tipo=='A').first()
            if  valUsr:
                valUsr.sk           = sk
                valUsr.usuario_alt  = usuario_alt
                db.add(valUsr) 
                db.commit() 

            
                image_base64 = base64.b64encode(response).decode('utf-8')

                result.append({'response':image_base64
                                    })        
                rc = 201 
                message = json.dumps(result)
                app_state.jsonenv = message 
                encripresult , estado = encrypt_message(password, message)
                result = encripresult
            else:
                result.append({'response':'email'}) 
                estado = False
                message = json.dumps(result)
                app_state.jsonenv = message  
                rc = 400

        return result , rc 