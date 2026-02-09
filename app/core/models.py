# models.py 
from sqlalchemy import Column, Integer, String, DateTime, Text, Float, Boolean, ForeignKey, Index, UniqueConstraint , \
                LargeBinary ,Date
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime, timedelta
from app.core.database import Base  # Importar Base desde tu database.py
from sqlalchemy.orm import relationship

import uuid

class DBDGENPERS(Base):
    __tablename__       = 'DBDGENPERS'
    id                  = Column(Integer, primary_key=True)          
    num_id              = Column(String(10), nullable=False)
    entidad             = Column(String(8), nullable=False)
    tknper              = Column(String(36), nullable=False, default=lambda: str(uuid.uuid4()))
    id_persona          = Column(String(9), nullable=False)
    tipo_id             = Column(String(3), nullable=False)
    nombre              = Column(String(60), nullable=False)
    ap_paterno          = Column(String(30), nullable=False)
    ap_materno          = Column(String(30), nullable=False)    
    genero              = Column(String(1) , nullable=False)
    tipo_per            = Column(String(1) , nullable=False)
    tipo_cte            = Column(String(1) , nullable=False)
    fecha_nac_const     = Column(DateTime(), nullable=False)        
    ocupacion           = Column(String(30), nullable=False)
    giro                = Column(String(30), nullable=False)
    pais_nac_const      = Column(String(30), nullable=False)
    nacionalidad        = Column(String(30), nullable=False)
    estado_civil        = Column(String(1) ,nullable=False)
    num_reg_mercantil   = Column(String(25) ,nullable=False)
    ind_pers_migrada    = Column(String(1), nullable=False)
    avanreg             = Column(Float, nullable=False)
    estatus             = Column(String(1) ,nullable=False)
    fecha_state         = Column(DateTime(),nullable=False)
    fecha_alta          = Column(DateTime())
    usuario_alta        = Column(String(10) ,nullable=False)
    fecha_mod           = Column(DateTime())
    usuario_mod         = Column(String(10))

    def __init__(self,num_id,entidad ,tknper,id_persona, tipo_id,nombre,ap_paterno,ap_materno,genero,tipo_per,tipo_cte,fecha_nac_const,ocupacion,giro,pais_nac_const,nacionalidad,
                 estado_civil,num_reg_mercantil,ind_pers_migrada,avanreg,estatus,fecha_state,usuario_alta,fecha_mod,usuario_mod):
        self.num_id                 = num_id
        self.entidad                = entidad
        self.tknper                 = tknper
        self.id_persona             = id_persona
        self.tipo_id                = tipo_id
        self.nombre                 = nombre
        self.ap_paterno             = ap_paterno
        self.ap_materno             = ap_materno
        self.genero                 = genero
        self.tipo_per               = tipo_per
        self.tipo_cte               = tipo_cte
        self.fecha_nac_const        = fecha_nac_const
        self.ocupacion              = ocupacion
        self.giro                   = giro
        self.pais_nac_const         = pais_nac_const
        self.nacionalidad           = nacionalidad
        self.estado_civil           = estado_civil
        self.num_reg_mercantil      = num_reg_mercantil
        self.ind_pers_migrada       = ind_pers_migrada
        self.avanreg                = avanreg
        self.estatus                = estatus
        self.fecha_state            = fecha_state
        self.fecha_alta             = STATIC.obtener_fecha_actual()
        self.usuario_alta           = usuario_alta
        self.fecha_mod              = fecha_mod
        self.usuario_mod            = usuario_mod

class DBDCOMPERS(Base):
    __tablename__       = 'DBDCOMPERS'
    id                  = Column(Integer, primary_key=True)          
    num_id              = Column(String(10), nullable=False)
    entidad             = Column(String(8), nullable=False)
    email_princ         = Column(String(50) ,nullable=False)
    email_alt           = Column(String(50) ,nullable=False)
    num_tel1            = Column(String(10) ,nullable=False)
    num_tel2            = Column(String(10) ,nullable=False)
    ind_pep             = Column(String(1) ,nullable=False)
    ingreso_max         = Column(Float, nullable=False)
    period_ingreso      = Column(String(1), nullable=False)
    moneda_ingreso      = Column(String(3), nullable=False)
    volumen_tx          = Column(Float, nullable=False)
    alias_nom_comer     = Column(String(50) ,nullable=False)
    pagina_web          = Column(String(50) ,nullable=False)
    red_social1         = Column(String(50) ,nullable=False)
    red_social2         = Column(String(50) ,nullable=False)
    red_social3         = Column(String(50) ,nullable=False)
    direcc_ip           = Column(String(20), nullable=False)
    direcc_mac          = Column(String(20), nullable=False)
    estatus             = Column(String(1) ,nullable=False)
    fecha_state         = Column(DateTime(),nullable=False)
    fecha_alta          = Column(DateTime())
    usuario_alta        = Column(String(10) ,nullable=False)
    fecha_mod           = Column(DateTime())
    usuario_mod         = Column(String(10))

    def __init__(self,num_id,entidad,email_princ,email_alt,num_tel1,num_tel2,ind_pep,ingreso_max,period_ingreso,moneda_ingreso,volumen_tx,alias_nom_comer,pagina_web,
                 red_social1,red_social2,red_social3,direcc_ip, direcc_mac,estatus,fecha_state,usuario_alta,fecha_mod,usuario_mod):
        self.num_id                 = num_id
        self.entidad                = entidad
        self.email_princ            = email_princ
        self.email_alt              = email_alt
        self.num_tel1               = num_tel1
        self.num_tel2               = num_tel2
        self.ind_pep                = ind_pep
        self.ingreso_max            = ingreso_max
        self.period_ingreso         = period_ingreso
        self.moneda_ingreso         = moneda_ingreso
        self.volumen_tx             = volumen_tx
        self.alias_nom_comer        = alias_nom_comer
        self.pagina_web             = pagina_web
        self.red_social1            = red_social1
        self.red_social2            = red_social2
        self.red_social3            = red_social3
        self.direcc_ip              = direcc_ip
        self.direcc_mac             = direcc_mac
        self.estatus                = estatus
        self.fecha_state            = fecha_state
        self.fecha_alta             = STATIC.obtener_fecha_actual()
        self.usuario_alta           = usuario_alta
        self.fecha_mod              = fecha_mod
        self.usuario_mod            = usuario_mod

class DBDIRPERS(Base):
    __tablename__               = 'DBDIRPERS'
    id                          = Column(Integer, primary_key=True)          
    num_id                      = Column(String(10), nullable=False)
    entidad                     = Column(String(8), nullable=False)
    tipo_dir                    = Column(String(1), nullable=False)
    direccion                   = Column(String(50), nullable=False)
    cod_postal                  = Column(String(5) , nullable=False)
    ciudad                      = Column(String(30), nullable=False)    
    pais                        = Column(String(15) , nullable=False)
    latitud                     = Column(Float, nullable=False)
    longitud                    = Column(Float, nullable=False)
    estatus                     = Column(String(1), nullable=False)
    fecha_alta                  = Column(DateTime(), nullable=False)
    usuario_alta                = Column(String(10) ,nullable=False)
    fecha_mod                   = Column(DateTime())
    usuario_mod                 = Column(String(10))
    

    def __init__(self,num_id,entidad,tipo_dir,direccion,cod_postal,ciudad,pais,latitud,longitud,estatus,usuario_alta,fecha_mod,usuario_mod ):
        self.num_id                    = num_id
        self.entidad                   = entidad
        self.tipo_dir                  = tipo_dir
        self.direccion                 = direccion
        self.cod_postal                = cod_postal
        self.ciudad                    = ciudad
        self.pais                      = pais
        self.latitud                   = latitud
        self.longitud                  = longitud
        self.estatus                   = estatus
        self.fecha_alta                = STATIC.obtener_fecha_actual()
        self.usuario_alta              = usuario_alta
        self.fecha_mod                 = fecha_mod
        self.usuario_mod               = usuario_mod


class DBRELPERS(Base):
    __tablename__               = 'DBRELPERS'
    id                          = Column(Integer, primary_key=True)         
    num_id_princ                = Column(String(10), nullable=False)
    num_id_relac                = Column(String(10), nullable=False)
    entidad                     = Column(String(8), nullable=False) 
    tipo_relac                  = Column(String(4), nullable=False)
    nivel_relac                 = Column(String(1), nullable=False)
    porcentaje_partic           = Column(Float, nullable=True)
    fecha_ini_rel               = Column(DateTime())
    fecha_fin_rel               = Column(DateTime())
    docto_referencia            = Column(String(50), nullable=True)
    estatus                     = Column(String(1), nullable=False)
    fecha_alta                  = Column(DateTime(), nullable=False)
    usuario_alta                = Column(String(10) ,nullable=False)
    fecha_mod                   = Column(DateTime())
    usuario_mod                 = Column(String(10))
    

    def __init__(self,num_id_princ, num_id_relac,entidad, tipo_relac, nivel_relac, porcentaje_partic, fecha_ini_rel, fecha_fin_rel, docto_referencia, estatus,
                 usuario_alta,fecha_mod,usuario_mod ):
        self.num_id_princ               = num_id_princ        
        self.num_id_relac               = num_id_relac  
        self.entidad                    = entidad      
        self.tipo_relac                 = tipo_relac          
        self.nivel_relac                = nivel_relac         
        self.porcentaje_partic          = porcentaje_partic   
        self.fecha_ini_rel              = fecha_ini_rel       
        self.fecha_fin_rel              = fecha_fin_rel       
        self.docto_referencia           = docto_referencia
        self.estatus                    = estatus             
        self.fecha_alta                 = STATIC.obtener_fecha_actual()
        self.usuario_alta               = usuario_alta
        self.fecha_mod                  = fecha_mod
        self.usuario_mod                = usuario_mod

class DBCTAPERS(Base):
    __tablename__ = 'DBCTAPERS'
    id                  = Column(Integer,     primary_key=True)
    entidad             = Column(String(8), nullable=False)
    tknper              = Column(String(36),  nullable=False)
    pais                = Column(String(2),   nullable=False)
    moneda              = Column(String(3), nullable=False)
    entban              = Column(String(4),   nullable=False)
    tipo                = Column(String(3),   nullable=False)
    alias               = Column(String(10),  nullable=False)
    datos               = Column(Text) 
    indoper             = Column(String(1),  nullable=False)
    estatus             = Column(String(1),  nullable=False)
    enmascar            = Column(String(50), nullable=False)
    categoria           = Column(String(20), nullable=False) 
    fecha_alta          = Column(DateTime())
    usuario_alta        = Column(String(10) ,nullable=False)
    fecha_mod           = Column(DateTime())
    usuario_mod         = Column(String(10) ,nullable=False)

    def __init__(self,entidad,tknper,pais,moneda,entban,tipo,alias,datos,indoper,estatus,enmascar,categoria,usuario_alta,fecha_mod,usuario_mod):
        self.entidad            = entidad
        self.tknper             = tknper
        self.pais               = pais
        self.moneda             = moneda
        self.entban             = entban
        self.tipo               = tipo
        self.alias              = alias
        self.datos              = datos
        self.indoper            = indoper
        self.estatus            = estatus
        self.enmascar           = enmascar
        self.categoria          = categoria        
        self.fecha_alta         = STATIC.obtener_fecha_actual()
        self.usuario_alta       = usuario_alta
        self.fecha_mod          = fecha_mod
        self.usuario_mod        = usuario_mod


class DBDOCPERS(Base):
    __tablename__               = 'DBDOCPERS'
    id                          = Column(Integer, primary_key=True)          
    num_id                      = Column(String(10), nullable=False)
    entidad                     = Column(String(8), nullable=False)
    tipo_docto                  = Column(String(15), nullable=False)
    pais_emis_docto             = Column(String(2), nullable=False)
    numero_docto                = Column(String(50), nullable=True)
    nombre_archivo              = Column(String(255), nullable=False)
    image_docto                 = Column(LargeBinary)
    fecha_caducidad_docto       = Column(Date, nullable=True)
    estatus_validacion_docto    = Column(String(20), nullable=False)
    motivo_rechazo              = Column(String(100), nullable=True)
    # Metadata para compliance europeo
    ind_verificado_electronico  = Column(Boolean, default=False)
    nivel_autenticacion         = Column(String(10), nullable=True)  # baja, sustancial, alta
    sello_tiempo                = Column(DateTime, nullable=True)
    estatus                     = Column(String(1), nullable=False)
    fecha_alta                  = Column(DateTime(), nullable=False)
    usuario_alta                = Column(String(10) ,nullable=False)
    fecha_mod                   = Column(DateTime())
    usuario_mod                 = Column(String(10))
    

    def __init__(self,num_id, entidad,tipo_docto, pais_emis_docto, numero_docto, nombre_archivo, image_docto, fecha_caducidad_docto, estatus_validacion_docto, motivo_rechazo, 
                 ind_verificado_electronico,nivel_autenticacion, sello_tiempo, estatus,usuario_alta, fecha_mod,usuario_mod ):
        self.num_id                      = num_id  
        self.entidad                     = entidad                  
        self.tipo_docto                  = tipo_docto                
        self.pais_emis_docto             = pais_emis_docto           
        self.numero_docto                = numero_docto              
        self.nombre_archivo              = nombre_archivo            
        self.image_docto                 = image_docto               
        self.fecha_caducidad_docto       = fecha_caducidad_docto     
        self.estatus_validacion_docto    = estatus_validacion_docto  
        self.motivo_rechazo              = motivo_rechazo                        
        self.ind_verificado_electronico  = ind_verificado_electronico  
        self.nivel_autenticacion         = nivel_autenticacion         
        self.sello_tiempo                = sello_tiempo             
        self.estatus                     = estatus             
        self.fecha_alta                  = STATIC.obtener_fecha_actual()
        self.usuario_alta                = usuario_alta
        self.fecha_mod                   = fecha_mod
        self.usuario_mod                 = usuario_mod


class DBKYCPERS(Base):
    __tablename__               = 'DBKYCPERS'
    id                          = Column(Integer, primary_key=True)          
    num_id                      = Column(String(10), nullable=False)
    entidad                     = Column(String(8), nullable=False)
    edad                        = Column(Integer,    nullable=False)
    riesgo_geog                 = Column(String(10),  nullable=False)
    riesgo_edad                 = Column(String(10),  nullable=False)
    riesgo_act_econ             = Column(String(10),  nullable=False)
    riesgo_total                = Column(String(10),  nullable=False)
    puntuac_fico                = Column(Integer,    nullable=False)  
    calif_fico                  = Column(String(10),  nullable=False)
    tx_frecuentes               = Column(Integer,    nullable=False)
    tx_mismo_impte              = Column(Integer,    nullable=False)
    tx_benef_sospech            = Column(Integer,    nullable=False)
    tx_alto_valor               = Column(Integer,    nullable=False)
    tx_paises_altrzgo           = Column(Integer,    nullable=False)
    tx_con_estruct              = Column(Integer,    nullable=False)
    tx_sospechosas              = Column(Integer,    nullable=False)
    aprob_kyc                   = Column(String(5),  nullable=False)
    razon_aprob                 = Column(String(250), nullable=False)
    estatus                     = Column(String(1),  nullable=False)
    fecha_alta                  = Column(DateTime())
    usuario_alta                = Column(String(10) ,nullable=False)
    fecha_mod                   = Column(DateTime())
    usuario_mod                 = Column(String(10) ,nullable=False) 


    def __init__(self,num_id,entidad, edad, riesgo_geog, riesgo_edad, riesgo_act_econ,riesgo_total, puntuac_fico, calif_fico, tx_frecuentes, 
                 tx_mismo_impte, tx_benef_sospech, tx_alto_valor, tx_paises_altrzgo, tx_con_estruct, tx_sospechosas, 
                 aprob_kyc, razon_aprob,estatus,usuario_alta,fecha_mod,usuario_mod):
        self.num_id               = num_id     
        self.entidad              = entidad        
        self.edad                 = edad   
        self.riesgo_geog          = riesgo_geog 
        self.riesgo_edad          = riesgo_edad
        self.riesgo_act_econ      = riesgo_act_econ
        self.riesgo_total         = riesgo_total
        self.puntuac_fico         = puntuac_fico
        self.calif_fico           = calif_fico
        self.tx_frecuentes        = tx_frecuentes
        self.tx_mismo_impte       = tx_mismo_impte
        self.tx_benef_sospech     = tx_benef_sospech
        self.tx_alto_valor        = tx_alto_valor
        self.tx_paises_altrzgo    = tx_paises_altrzgo
        self.tx_con_estruct       = tx_con_estruct
        self.tx_sospechosas       = tx_sospechosas
        self.aprob_kyc            = aprob_kyc
        self.razon_aprob          = razon_aprob  
        self.estatus              = estatus
        self.fecha_alta           = STATIC.obtener_fecha_actual()
        self.usuario_alta         = usuario_alta
        self.fecha_mod            = fecha_mod
        self.usuario_mod          = usuario_mod


class DBTCORP(Base):
    __tablename__ = 'DBTCORP'
    
    id = Column(Integer, primary_key=True) 
    entidad = Column(String(8), nullable=False)   
    llave = Column(String(8), nullable=False)         
    clave = Column(String(15), nullable=False)
    datos = Column(String(150), nullable=False)
    status = Column(String(1), nullable=False)
    fecha_alta = Column(DateTime)
    usuario_alta = Column(String(16), nullable=False)
    fecha_mod = Column(DateTime)
    usuario_mod = Column(String(16), nullable=False)
    
    def __init__(self, entidad, llave, clave, datos, status, usuario_alta, usuario_mod):
        self.entidad = entidad
        self.llave = llave
        self.clave = clave
        self.datos = datos
        self.status = status
        self.fecha_alta = STATIC.obtener_fecha_actual()
        self.usuario_alta = usuario_alta
        self.fecha_mod = STATIC.obtener_fecha_actual()
        self.usuario_mod = usuario_mod

class User(Base):
    __tablename__ = 'user'
    
    id = Column(Integer, primary_key=True)
    entidad = Column(String(8), nullable=False)   
    clvcol = Column(String(10), nullable=False)
    name = Column(String(60), nullable=False)
    email = Column(String(256), nullable=False)
    tel = Column(String(15), nullable=False)
    tipo = Column(String(1), nullable=False)
    password = Column(String(256), nullable=False)
    nivel = Column(Integer, default=0)
    sk = Column(String(48), nullable=False)
    fecha_alta = Column(DateTime)
    usuario_alt = Column(String(10), default='')

    
    def __init__(self, entidad, clvcol, name, email, tel, tipo, password, nivel, sk,usuario_alt):
        self.entidad    = entidad
        self.clvcol     = clvcol
        self.name       = name
        self.email      = email
        self.tel        = tel
        self.tipo       = tipo
        self.password   = password  
        self.nivel      = nivel
        self.sk         = sk
        self.fecha_alta = datetime.now()
        self.usuario_alt  = usuario_alt



class DBOTPUSER(Base):
    __tablename__ = 'DBOTPUSER'
    
    id = Column(Integer, primary_key=True)
    entidad = Column(String(8), nullable=False) 
    email = Column(String(256), nullable=False)
    sk = Column(String(48), nullable=False)
    fecha_alta = Column(DateTime)
    usuario_alt = Column(String(8), default='')
    
    def __init__(self, entidad, email, sk, usuario_alt):
        self.entidad = entidad
        self.email = email
        self.sk = sk
        self.fecha_alta = datetime.now()
        self.usuario_alt = usuario_alt

class DBLOGENTRY(Base):
    __tablename__ = 'DBLOGENTRY'
    
    id = Column(Integer, primary_key=True)
    entidad = Column(String(8))  
    timestar = Column(DateTime, default=datetime.now)
    timeend = Column(DateTime, default=datetime.now)
    log_level = Column(String(10))
    funcion = Column(String(20))
    respcod = Column(String(10))
    nombre = Column(String(40))
    Ip_Origen = Column(String(20))
    Servicio = Column(String(20), nullable=False)
    Metodo = Column(String(20), nullable=False)
    DatosIn = Column(Text) 
    DatosOut = Column(Text)
    
    def __init__(self, entidad, timestar, timeend, log_level, funcion, respcod, nombre, Ip_Origen, Servicio, Metodo, DatosIn, DatosOut):
        self.entidad = entidad
        self.timestar = timestar
        self.timeend = timeend
        self.log_level = log_level
        self.funcion = funcion
        self.respcod = respcod
        self.nombre = nombre
        self.Ip_Origen = Ip_Origen
        self.Servicio = Servicio
        self.Metodo = Metodo
        self.DatosIn = DatosIn
        self.DatosOut = DatosOut

class DBLOGWAF(Base):
    __tablename__ = 'DBLOGWAF'
    
    id = Column(Integer, primary_key=True)
    entidad = Column(String(8), nullable=False)  
    status = Column(String(1))
    timestar = Column(DateTime, default=datetime.now)
    timeend = Column(DateTime, default=datetime.now)
    Ip_Origen = Column(String(20))
    Servicio = Column(String(20), nullable=False)
    Metodo = Column(String(20), nullable=False)
    respcod = Column(String(10))
    duration = Column(Float, nullable=False)
    DatosIn = Column(Text) 
    DatosOut = Column(Text)
    
    def __init__(self, entidad, status, timestar, timeend, Ip_Origen, Servicio, Metodo, respcod, duration, DatosIn, DatosOut):
        self.entidad = entidad
        self.status = status
        self.timestar = timestar
        self.timeend = timeend
        self.Ip_Origen = Ip_Origen
        self.Servicio = Servicio
        self.Metodo = Metodo
        self.respcod = respcod
        self.duration = duration
        self.DatosIn = DatosIn
        self.DatosOut = DatosOut

class DBSERVNIV(Base):
    __tablename__ = 'DBSERVNIV'
    
    id = Column(Integer, primary_key=True)
    entidad = Column(String(8), nullable=False)  
    Servicio = Column(String(20), nullable=False)
    nivel = Column(Integer, default=0)
    indauth = Column(String(1))
    indcost = Column(String(1))
    indmon = Column(String(1))
    indexc = Column(String(1))
    impmax = Column(Float, nullable=False)
    fecha_alta = Column(DateTime)
    usuario_alt = Column(String(10), default='')
    status = Column(String(1), default='A')
    
    def __init__(self, entidad, Servicio, nivel, indauth,indcost,indmon,indexc,impmax ,usuario_alt, status):
        self.entidad    = entidad
        self.Servicio   = Servicio
        self.nivel      = nivel
        self.indauth    = indauth
        self.indcost    = indcost
        self.indmon     = indmon
        self.indexc     = indexc
        self.impmax     = impmax 
        self.fecha_alta = datetime.now()
        self.usuario_alt = usuario_alt
        self.status     = status

class DBTRNAUT(Base):
    __tablename__ = 'DBTRNAUT'
    id          = Column(Integer, primary_key=True)
    entidad     = Column(String(8), nullable=False)  
    iduser      = Column(Integer, nullable=False) 
    folauth     = Column(Integer, nullable=False) 
    Servicio    = Column(String(20), nullable=False)
    status      = Column(String(1), nullable=False)   
    DatosIn     = Column(Text) 
    emailauth   = Column(String(256), nullable=False)
    fecha_alta  = Column(DateTime())
    fecha_auth  = Column(DateTime())
    usuario_alt = Column(String(8), default=False)
    def __init__(self, entidad, iduser, folauth ,Servicio ,status ,DatosIn,emailauth,fecha_auth,usuario_alt):
        self.entidad     = entidad
        self.iduser     = iduser
        self.folauth    = folauth
        self.Servicio   = Servicio
        self.status     = status
        self.DatosIn    = DatosIn
        self.emailauth  = emailauth
        self.fecha_alta = datetime.now()
        self.fecha_auth = fecha_auth
        self.usuario_alt = usuario_alt 

class DBTRNAUTUSR(Base):
    __tablename__ = 'DBTRNAUTUSR'
    id          = Column(Integer, primary_key=True)
    entidad     = Column(String(8), nullable=False)  
    iduser      = Column(Integer, nullable=False) 
    Servicio    = Column(String(20), nullable=False)
    status      = Column(String(1), nullable=False)   
    emailauth   = Column(String(256), nullable=False)
    fecha_alta  = Column(DateTime())
    usuario_alt = Column(String(10), default=False)
    fecha_mod = Column(DateTime)
    usuario_mod = Column(String(16), nullable=False)
    def __init__(self, entidad, iduser ,Servicio ,status ,emailauth,usuario_alt,fecha_mod,usuario_mod):
        self.entidad     = entidad
        self.iduser     = iduser
        self.Servicio   = Servicio
        self.status     = status
        self.emailauth  = emailauth
        self.fecha_alta = datetime.now()
        self.usuario_alt = usuario_alt 
        self.fecha_mod = fecha_mod
        self.usuario_mod = usuario_mod


class DBSERVAUT(Base):
    __tablename__ = 'DBSERVAUT'
    id          = Column(Integer, primary_key=True)
    entidad     = Column(String(8), nullable=False)  
    iduser      = Column(Integer, nullable=False) 
    folauth     = Column(Integer, nullable=False) 
    Servicio    = Column(String(20), nullable=False)
    status      = Column(String(1), nullable=False)   
    emailauth   = Column(String(256), nullable=False)
    fecha_auth  = Column(DateTime())
    def __init__(self, entidad, iduser, folauth ,Servicio ,status ,emailauth):
        self.entidad     = entidad
        self.iduser     = iduser
        self.folauth    = folauth
        self.Servicio   = Servicio
        self.status     = status
        self.emailauth  = emailauth
        self.fecha_auth = datetime.now()



class Folio(Base):
    __tablename__ = 'folio'   
    id = Column(Integer, primary_key=True)          
    numtranc = Column(Integer, nullable=False, unique=True)
    
    def __init__(self, numtranc):
        self.numtranc = numtranc


class MasivoBatch(Base):
    __tablename__ = "DBMSBGENE"

    id         = Column(Integer, primary_key=True)
    entidad    = Column(String(8), nullable=False)      
    cliente    = Column(String(8), nullable=False, index=True)
    filename   = Column(Text, nullable=False)
    total      = Column(Integer, nullable=False, default=0)
    processed  = Column(Integer, nullable=False, default=0)
    successful = Column(Integer, nullable=False, default=0)
    failed     = Column(Integer, nullable=False, default=0)
    status     = Column(String(10), nullable=False, default="ACTIVE")  # ACTIVE|PAUSED
    created_at = Column(DateTime, default=datetime.now)

    items = relationship("MasivoItem", back_populates="batch")

class MasivoItem(Base):
    __tablename__ = "DBMSBDETA"

    id           = Column(Integer, primary_key=True)
    entidad     = Column(String(8), nullable=False)  
    batch_id     = Column(Integer, ForeignKey("DBMSBGENE.id"), nullable=False, index=True)
    cliente      = Column(String(8), nullable=False, index=True)
    filename     = Column(Text, nullable=False)
    line_no      = Column(Integer, nullable=False, default=0)

    accion       = Column(String(1),  nullable=False)
    tkncliori    = Column(String(36),  nullable=False)
    aliasori     = Column(String(10),  nullable=False)
    tipoori      = Column(String(3),   nullable=False)
    ordenante    = Column(String(60), nullable=False)

    tknclides    = Column(String(36),  nullable=False)
    aliasdes     = Column(String(10),  nullable=False)
    tipodes      = Column(String(3),   nullable=False)
    beneficiario = Column(String(60), nullable=False)

    concepto     = Column(String(30),  nullable=False)
    importe      = Column(Float,       nullable=False)
    fecha_ejec   = Column(DateTime())

    status       = Column(String(12), nullable=False)  # PENDING|SUCCESS|FAILED|CANCELLED|RETURNED|VOIDED
    error        = Column(Text, nullable=True)
    attempts     = Column(Integer, nullable=False, server_default="0")

    idempotency_key = Column(Text, nullable=False, unique=True)
    idempotency_val = Column(Text, nullable=False)

    saldo_before = Column(Float,       nullable=True)
    saldo_after  = Column(Float,       nullable=True)

    processed_at = Column(DateTime())
    created_at   = Column(DateTime())
    updated_at   = Column(DateTime())

    batch = relationship("MasivoBatch", back_populates="items")

    __table_args__ = (
        Index("idx_items_cliente_status", "cliente", "status"),
        UniqueConstraint("idempotency_key", name="uq_DBMSBDETA_idem"),
    )


class DBLAYOUT(Base):
    __tablename__ = 'DBLAYOUT'
    
    id = Column(Integer, primary_key=True) 
    entidad = Column(String(8), nullable=False)   
    llave = Column(String(8), nullable=False)         
    clave = Column(String(15), nullable=False)
    datos = Column(String(150), nullable=False)
    status = Column(String(1), nullable=False)
    fecha_alta = Column(DateTime)
    usuario_alta = Column(String(16), nullable=False)
    fecha_mod = Column(DateTime)
    usuario_mod = Column(String(16), nullable=False)
    
    def __init__(self, entidad, llave, clave, datos, status, usuario_alta, fecha_mod,usuario_mod):
        self.entidad = entidad
        self.llave = llave
        self.clave = clave
        self.datos = datos
        self.status = status
        self.fecha_alta = STATIC.obtener_fecha_actual()
        self.usuario_alta = usuario_alta
        self.fecha_mod = fecha_mod
        self.usuario_mod = usuario_mod        

class DBOPERDIA(Base):
    __tablename__ = 'DBOPERDIA' 
    
    id = Column(Integer, primary_key=True) 
    entidad = Column(String(8), nullable=False)  
    #num_transacc = Column(String(35), nullable=False, unique=True, index=True) recomendable busquedas rapidas
    num_transacc =  Column(String(35), nullable=False) #clave unica de lote
    tipo_oper = Column(String(10), nullable=False) # COBRO o ABONO
    fecha_ejec = Column(DateTime)
    importe = Column(Float,       nullable=False)
    concepto = Column(String(30),  nullable=False)
    status = Column(String(12), nullable=False)
    iban_benef = Column(String(34),  nullable=False)
    bic_benef = Column(String(12),  nullable=False)
    nombre_benef = Column(String(60), nullable=False)
    id_acreedor = Column(String(35),  nullable=False)
    id_mandate = Column(String(35),  nullable=False)
    iban_mandate = Column(String(34),  nullable=False)
    bic_mandate = Column(String(12),  nullable=False)
    nombre_mandate = Column(String(60), nullable=False)
    fecha_mandate = Column(DateTime)
    clve_mandate = Column(String(4), nullable=False)
    cod_rechazo = Column(String(4), nullable=False)
    num_intentos = Column(Integer, nullable=False) 
    fecha_alta = Column(DateTime)
    usuario_alta = Column(String(16), nullable=False)
    fecha_mod = Column(DateTime)
    usuario_mod = Column(String(16), nullable=False)
    
    def __init__(self, entidad, num_transacc, tipo_oper, fecha_ejec, importe, concepto,
                 status,iban_benef, bic_benef, nombre_benef, id_acreedor,
                 id_mandate, iban_mandate, bic_mandate, nombre_mandate, 
                 fecha_mandate, clve_mandate, cod_rechazo, num_intentos, usuario_alta, fecha_mod, usuario_mod):
        self.entidad = entidad
        self.num_transacc = num_transacc
        self.tipo_oper = tipo_oper
        self.fecha_ejec = fecha_ejec
        self.importe = importe
        self.concepto = concepto
        self.status = status
        self.iban_benef = iban_benef
        self.bic_benef = bic_benef
        self.nombre_benef = nombre_benef
        self.id_acreedor = id_acreedor
        self.id_mandate = id_mandate
        self.iban_mandate = iban_mandate
        self.bic_mandate = bic_mandate
        self.nombre_mandate = nombre_mandate
        self.fecha_mandate = fecha_mandate
        self.clve_mandate = clve_mandate
        self.cod_rechazo = cod_rechazo
        self.num_intentos = num_intentos
        self.fecha_alta = STATIC.obtener_fecha_actual()
        self.usuario_alta = usuario_alta
        self.fecha_mod = fecha_mod
        self.usuario_mod = usuario_mod

class DBSALDOS(Base):
    __tablename__ = 'DBSALDOS'
    id                  = Column(Integer,     primary_key=True)
    entidad             = Column(String(8), nullable=False)
    tkncli              = Column(String(36),  nullable=False)
    datos               = Column(Text) 
    tipo                = Column(String(3),   nullable=False)
    alias               = Column(String(10),  nullable=False)
    saldo               = Column(Float,       nullable=False)    
    fecha_saldo         = Column(DateTime())
    fecha_alta          = Column(DateTime())
    usuario_alta        = Column(String(10) ,nullable=False)
    fecha_mod           = Column(DateTime())
    usuario_mod         = Column(String(10) ,nullable=False)

    def __init__(self,entidad,tkncli,datos,tipo,alias,saldo,fecha_saldo,fecha_alta,usuario_alta,fecha_mod,usuario_mod):
        self.entidad            = entidad
        self.tkncli             = tkncli
        self.datos              = datos
        self.tipo               = tipo
        self.alias              = alias
        self.saldo              = saldo
        self.fecha_saldo        = fecha_saldo
        self.fecha_alta         = fecha_alta
        self.usuario_alta       = usuario_alta
        self.fecha_mod          = fecha_mod
        self.usuario_mod        = usuario_mod
        

class DBMOVTOS(Base):
    __tablename__ = 'DBMOVTOS'
    id                  = Column(Integer,     primary_key=True)
    entidad             = Column(String(8), nullable=False)
    tkncli              = Column(String(36),  nullable=False)
    datos               = Column(Text) 
    alias               = Column(String(10),  nullable=False)
    signo               = Column(String(1),   nullable=False)
    fecha_movto         = Column(DateTime())
    concepto            = Column(String(30),  nullable=False)
    importe             = Column(Float,       nullable=False)   
    num_transacc        = Column(String(35), nullable=False) 
    status              = Column(String(1), nullable=False)  # A = Aplicada | R = Rechazada
    fecha_alta          = Column(DateTime())
    usuario_alta        = Column(String(10) ,nullable=False)
    fecha_mod           = Column(DateTime())
    usuario_mod         = Column(String(10) ,nullable=False)

    def __init__(self,entidad,tkncli,datos,alias,signo,fecha_movto,concepto,importe,num_transacc,status,fecha_alta,usuario_alta,fecha_mod,usuario_mod):
        self.entidad            = entidad
        self.tkncli             = tkncli
        self.datos              = datos
        self.alias              = alias
        self.signo              = signo
        self.fecha_movto        = fecha_movto
        self.concepto           = concepto
        self.importe            = importe
        self.num_transacc       = num_transacc
        self.status             = status
        self.fecha_alta         = fecha_alta
        self.usuario_alta       = usuario_alta
        self.fecha_mod          = fecha_mod
        self.usuario_mod        = usuario_mod

class MasivoIngestSummary(Base):
    __tablename__ = "DBMSLGENE"

    id                      = Column(Integer, primary_key=True)
    entidad                 = Column(String(8), nullable=False)  
    filename                = Column(String(255), nullable=False)
    total                   = Column(Integer, nullable=False, default=0)
    invalid                 = Column(Integer, nullable=False, default=0)
    duplicates_in_file      = Column(Integer, nullable=False, default=0)
    inserted                = Column(Integer, nullable=False, default=0)
    skipped_existing        = Column(Integer, nullable=False, default=0)
    reinstated_failed       = Column(Integer, nullable=False, default=0)
    started_at              = Column(DateTime())
    finished_at             = Column(DateTime())
    raw_summary             = Column(Text)     
    created_at              = Column(DateTime())
    status                  = Column(String(20), nullable=False, default="Pendiente")

    logs = relationship("MasivoIngestLog", backref="ingest", cascade="all, delete-orphan")


class MasivoIngestLog(Base):
    __tablename__ = "DBMSLDETA"

    id                  = Column(Integer, primary_key=True)
    entidad             = Column(String(8), nullable=False)  
    ingest_id           = Column(Integer, ForeignKey('DBMSLGENE.id', ondelete='CASCADE'), nullable=False, index=True)
    batch_id            = Column(Integer, ForeignKey('DBMSBGENE.id'), nullable=True)
    cliente             = Column(String(8), nullable=True)
    filename            = Column(String(255), nullable=False)
    line_no             = Column(Integer, nullable=True)
    idempotency_key     = Column(String(64), nullable=True)  # sha256 hex (64)
    code                = Column(String(32), nullable=False) # INVALID, DUP_IN_FILE, REINSTATED, INSERTED, SKIPPED, ERROR
    message             = Column(Text, nullable=True)
    detail              = Column(Text)                    
    created_at          = Column(DateTime())


class DBENTIDAD(Base):
    __tablename__ = 'DBENTIDAD'
    
    id = Column(Integer, primary_key=True) 
    entidad         = Column(String(8), nullable=False)
    num_id          = Column(String(10), nullable=False)   
    nombre          = Column(String(50), nullable=False)         
    status          = Column(String(1), nullable=False)
    fecha_alta      = Column(DateTime)
    usuario_alta    = Column(String(16), nullable=False)
    fecha_mod       = Column(DateTime)
    usuario_mod     = Column(String(16), nullable=False)
    
    def __init__(self, entidad, num_id ,nombre, status, usuario_alta, usuario_mod):
        self.entidad    = entidad
        self.num_id     = num_id 
        self.nombre     = nombre
        self.status     = status
        self.fecha_alta = STATIC.obtener_fecha_actual()
        self.usuario_alta = usuario_alta
        self.fecha_mod  = STATIC.obtener_fecha_actual()
        self.usuario_mod = usuario_mod

class DBUSETRAN(Base):
    __tablename__ = 'DBUSETRAN'
    
    id = Column(Integer, primary_key=True) 
    entidad         = Column(String(8), nullable=False)
    Servicio        = Column(String(20), nullable=False)
    indcost         = Column(String(1))
    num_id          = Column(String(10), nullable=False)   
    fecha_alta      = Column(DateTime)
    
    def __init__(self, entidad, Servicio,indcost, num_id ):
        self.entidad    = entidad
        self.Servicio   = Servicio
        self.indcost    = indcost
        self.num_id     = num_id 
        self.fecha_alta = STATIC.obtener_fecha_actual()
class DBCOSTTRAN(Base):
    __tablename__ = 'DBCOSTTRAN'
    
    id              = Column(Integer, primary_key=True) 
    entidad         = Column(String(8), nullable=False)
    indcost         = Column(String(1))
    num_txs_libres  = Column(String(10), nullable=False)   
    costo_tx        = Column(Float,       nullable=True)
    estatus         = Column(String(1), nullable=False)
    fecha_alta      = Column(DateTime)
    usuario_alta    = Column(String(16), nullable=False)
    fecha_mod       = Column(DateTime)
    usuario_mod     = Column(String(16), nullable=False)
    
    def __init__(self, entidad, indcost, num_txs_libres, costo_tx, estatus, usuario_alta, fecha_mod,usuario_mod):
        self.entidad         = entidad
        self.indcost         = indcost
        self.num_txs_libres  = num_txs_libres 
        self.costo_tx        = costo_tx
        self.estatus         = estatus
        self.fecha_alta      = STATIC.obtener_fecha_actual()
        self.usuario_alta    = usuario_alta
        self.fecha_mod       = fecha_mod
        self.usuario_mod     = usuario_mod
        
class DBALERTS(Base):
    __tablename__ = 'DBALERTS'
    
    id = Column(Integer, primary_key=True) 
    entidad         = Column(String(8), nullable=False)
    Servicio        = Column(String(20), nullable=False)
    indmon          = Column(String(1))
    num_id          = Column(String(10), nullable=False)   
    fecha_alta      = Column(DateTime)
    
    def __init__(self, entidad, Servicio,indmon, num_id ):
        self.entidad    = entidad
        self.Servicio   = Servicio
        self.indmon     = indmon
        self.num_id     = num_id 
        self.fecha_alta = STATIC.obtener_fecha_actual()


class DBCATEG(Base):
    __tablename__ = 'DBCATEG'
    id           = Column(Integer, primary_key=True)
    entidad      = Column(String(8), nullable=False)    
    categoria    = Column(String(20), nullable=False) 
    nombre       = Column(String(256), nullable=False) 
    status      = Column(String(1), nullable=False)   
    fecha_alta  = Column(DateTime())
    usuario_alt = Column(String(10), default=False)
    def __init__(self,entidad ,categoria, nombre  ,status ,usuario_alt):
        self.entidad   = entidad
        self.categoria = categoria
        self.nombre    = nombre
        self.status     = status
        self.fecha_alta = datetime.now()
        self.usuario_alt = usuario_alt      


class DBCALIF(Base):
    __tablename__               = 'DBCALIF'
    id                          = Column(Integer, primary_key=True)         
    entidad                     = Column(String(8), nullable=False)
    docto_id                    = Column(String(20), nullable=False) 
    tipo_id                     = Column(String(20), nullable=False) 
    riesgo_geog                 = Column(String(1),  nullable=False)
    riesgo_act_econ             = Column(String(1),  nullable=False)
    riesgo_pep                  = Column(String(1),  nullable=False) 
    riesgo_list_sanc            = Column(String(1),  nullable=False)
    riesgo_med_adv              = Column(String(1),  nullable=False)
    tx_alto_valor               = Column(Integer,    nullable=False)
    tx_sospechosas              = Column(Integer,    nullable=False)
    riesgo_movs                 = Column(String(1), nullable=False)
    razon_riesgo_movs           = Column(String(250), nullable=False)
    score_crediticio            = Column(Integer,    nullable=False)
    razon_score_cred            = Column(String(250), nullable=False)
    cuota_max_sugerida          = Column(Float, nullable=False)
    estatus                     = Column(String(1),  nullable=False)
    fecha_alta                  = Column(DateTime())
    usuario_alta                = Column(String(10) ,nullable=False)
    fecha_mod                   = Column(DateTime())
    usuario_mod                 = Column(String(10) ,nullable=False) 


    def __init__(self, entidad, docto_id, tipo_id, riesgo_geog, riesgo_act_econ, riesgo_pep, riesgo_list_sanc,riesgo_med_adv, tx_alto_valor, tx_sospechosas,  
                 riesgo_movs, razon_riesgo_movs, score_crediticio, razon_score_cred, cuota_max_sugerida, estatus, usuario_alta, fecha_mod, usuario_mod):
        self.entidad              = entidad        
        self.docto_id             = docto_id     
        self.tipo_id              = tipo_id 
        self.riesgo_geog          = riesgo_geog 
        self.riesgo_act_econ      = riesgo_act_econ
        self.riesgo_pep           = riesgo_pep
        self.riesgo_list_sanc     = riesgo_list_sanc
        self.riesgo_med_adv       = riesgo_med_adv
        self.tx_alto_valor        = tx_alto_valor
        self.tx_sospechosas       = tx_sospechosas
        self.riesgo_movs          = riesgo_movs  
        self.razon_riesgo_movs    = razon_riesgo_movs  
        self.score_crediticio     = score_crediticio
        self.razon_score_cred     = razon_score_cred
        self.cuota_max_sugerida   = cuota_max_sugerida
        self.estatus              = estatus
        self.fecha_alta           = STATIC.obtener_fecha_actual()
        self.usuario_alta         = usuario_alta
        self.fecha_mod            = fecha_mod
        self.usuario_mod          = usuario_mod     

class STATIC:
    @staticmethod
    def obtener_fecha_actual():
        print ('obteniendo fecha')
        return datetime.now() - timedelta(hours=6)
