
from app.core.models import MasivoItem
from app.core.cypher import encrypt_message , decrypt_message
from app.core.cai import valcampo, obtener_fecha_actual
from app.core.config import settings
from app.core.state import app_state
from sqlalchemy import  func
import logging
import json 



logger = logging.getLogger(__name__)


class EnvService:
    
    def __init__(self, db_session=None):
        self.db = db_session
    
    def regs_Por_fecha(db,fecha):
       
        grouped_records = {}

        all_records = db.query(MasivoItem).order_by(MasivoItem.id, MasivoItem.fecha_ejec==fecha,
                                                    MasivoItem.accion in (4, 5)).all()

        for record in all_records:
            if record.id not in grouped_records:
                grouped_records[record.id] = []
            grouped_records[record.id].append(record)
        return grouped_records

    def transaccs(dirTrans,id_prop,tipo_servicio,tipo_movto,importe,cta,bic,nombre,dirProp):
        transaccs = {
            'id_prop': id_prop,
            'tipo_servicio': tipo_servicio,
            'tipo_movto': tipo_movto,
            'importe': importe,
            'cta': cta,
            'bic': bic,
            'nombre': nombre,
            'dirProp': dirProp
        }    
        dirTrans.append(transaccs) 
        return dirTrans


    def sepaxml(session, dirTrans):
        #print ('dirTrans :', dirTrans)
        hoy = obtener_fecha_actual()
        aboRent = False
        carRent = False
        car002  = False
        car003  = False
        transactions_data = []
        #abono a cliente de la renta 
        abono_renta = ''
        for transaction in dirTrans:
            # Verifica si el diccionario actual cumple ambas condiciones
            if transaction.get('tipo_servicio') == '001' and \
            transaction.get('tipo_movto') == 'H':
                abono_renta=transaction
            
        if abono_renta != '':
            #print ('abono_renta:', abono_renta) 
            id_prop             = abono_renta['id_prop'] 
            mandate_id          = abono_renta['bic']
            instructed_amount   = abono_renta['importe']
            aboRent = True
        #Cargo a cliente de la renta 
        cargo_renta = ''
        for transaction in dirTrans:
            # Verifica si el diccionario actual cumple ambas condiciones
            if transaction.get('tipo_servicio') == '001' and \
            transaction.get('tipo_movto') == 'D':
                cargo_renta=transaction
            
        if cargo_renta != '':
            #print ('cargo_renta:', cargo_renta)  
            debtor_bic              = cargo_renta['bic']
            debtor_name             = cargo_renta['nombre']
            debtor_address_line     = cargo_renta['dirProp']
            debtor_iban             = cargo_renta['cta']
            remittance_information  = 'Servicio 001 '
            carRent = True          

    

        if aboRent and carRent:
            transactions_data.append (
                {
                    'end_to_end_id': 'INV-2025-001',
                    'instructed_amount': instructed_amount,
                    'currency': 'EUR',
                    'mandate_id': mandate_id,
                    'date_of_signature': hoy.strftime("%Y-%m-%d"),
                    'debtor_bic': debtor_bic,
                    'debtor_name': debtor_name,
                    'debtor_country': 'ES',
                    'debtor_address_line': debtor_address_line,
                    'debtor_iban': debtor_iban,
                    'remittance_information': remittance_information
                })
        #print ('transactions_data:', transactions_data)

        

        sepa_xml = generate_sepa_direct_debit_xml(
            msg_id="MSG-20250724-789",
            creation_date_time=datetime(2025, 7, 24, 10, 30, 0),
            number_of_transactions=2,
            control_sum=350.00,
            initiating_party_name="Anexia S.A.",
            initiating_party_id="ES12345678A",
            payment_info_id="PMTINF-001",
            requested_collection_date=hoy.strftime("%Y-%m-%d"),
            creditor_name="Anexia .",
            creditor_iban="ES7921000813610123456789",
            creditor_bic="CAIXESBBXXX",
            creditor_scheme_id="ES777777",
            transactions=transactions_data
        )
        sepaxml = sepa_xml
        regSEPA = DBXMLSEPA(id_prop,sepaxml)
        session.add(regSEPA) 
        session.commit()   
        return sepa_xml


    def envio(self, datos , db):
        result = []
        rc=400
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
            param_keys = ['entidad' , 'nombre']
            if not all (key in dataJSON for key in param_keys):
                estado = False   
                result.append({'response':'Algun elemento de la transacción esta faltando'}) 
                message = json.dumps(result)
                app_state.jsonenv = message                                  
                rc = 400   
            else:                                                           
                fecha =  dataJSON["fecha"]

        else:
            estado = False
            result = 'informacion incorrecta'
            message = json.dumps(result)
            app_state.jsonenv = message 
            rc = 400
        if estado :
            if not valcampo('date',fecha,10):
                result.append({'response':'entdad'})
                estado = False
                message = json.dumps(result)
                app_state.jsonenv = message 
                rc = 400
                print('result :', result)
                return result , rc
            
            if estado:
                data_agrupada = self.regs_Por_fecha(db,fecha)
                num_grupos = len(data_agrupada)
                for id_prop, registros_del_grupo in data_agrupada.items():
                    dirTrans = [] 
                    for registro in registros_del_grupo:
                        id_prop ='1'
                        tipo_servicio ='001'
                        tipo_movto ='D'
                        importe = registro.importe
                        cta     = registro.tkncliori
                        bic     = registro.tkncliori
                        nombre  = registro.ordenante
                        dirProp = '1'
                        dirTrans = self.transaccs(dirTrans,id_prop,tipo_servicio,tipo_movto,importe,cta,bic,nombre,dirProp) 
                        id_prop ='1'
                        tipo_servicio ='001'
                        tipo_movto ='h'
                        importe = registro.importe
                        cta     = registro.tknclides
                        bic     = registro.tknclides
                        nombre  = registro.beneficiario
                        dirProp = '1'
                        dirTrans = self.transaccs(dirTrans,id_prop,tipo_servicio,tipo_movto,importe,cta,bic,nombre,dirProp) 
                    #print ('dirTrans:', dirTrans)
                    self.sepaxml(db,dirTrans)  

            
        return result , rc   
    
