# --- interfaz.py ---

import requests, threading
import json
import logging
from app.core.cypher import encrypt_message, decrypt_message
from app.core.cai    import obtener_fecha_actual
from app.core.models import DBLOGENTRY
from app.core.config import settings
from app.core.state import app_state

# ===========================================================
# 1) encripta_peticion: usar http_local pasado desde el hilo
# ===========================================================
def encripta_peticion(db, http_local, euser, eIp_Origen, etimestar, oper: str, payload: dict, headertoken: str = ""):
    """
    Encripta y llama al banco usando Session() keep-alive del hilo (http_local).
    Retorna (rc, resp_text).
    """
    body = json.dumps(payload, ensure_ascii=False)
    encrip, _ = encrypt_message(settings.PASSCYPH, body)
    # http_local debe venir desde el hilo; si viene None, crearlo (último recurso)
    if http_local is None:
        http_local = threading.local()
    rc, resp = callapi_alive(db, http_local, oper, encrip, headertoken, euser, eIp_Origen, etimestar)
    return rc, resp

# ===========================================================
# 2) login_al_banco: puede crear http_local propio (no crítico)
# ===========================================================
def login_al_banco(db, euser, eIp_Origen, etimestar, http_local=None) -> str:
    try:
        login_body = {"email": "lcastaneda@example.net", "password": "user"}
        rc, resp = encripta_peticion(db, http_local, euser, eIp_Origen, etimestar, 'asplogin', login_body, "")
        tokenDatlog = json.loads(resp)
        token = tokenDatlog["access_token"]
        return token
    except Exception:
        return ""

# ===========================================================
# 3) consulta_saldo/registra_movto: aceptar http_local y pasarlo
# ===========================================================
def consulta_saldo(db, euser, eIp_Origen, etimestar, headertoken: str, item, http_local=None):
    payload = {
        "tkncli": item.tkncliori,
        "alias":  item.aliasori
    }
    rc, resp = encripta_peticion(db, http_local, euser, eIp_Origen, etimestar, 'selsaldos', payload, headertoken)
    saldo = None
    if rc == 201:
        decriptMsg, estado = decrypt_message(settings.PASSCYPH, resp)
        data_sald = json.loads(decriptMsg)[0]
        saldo = data_sald.get("saldo")
    else:
        resp = 'Falla en el servidor.'
        rc = 400
    ok = (rc in (200, 201)) and (saldo is not None)
    return ok, (saldo or 0.0), {"rc": rc, "resp": resp}

def registra_movto(db, euser, eIp_Origen, etimestar, headertoken: str, *, tkncli: str, alias: str, tipo: str,
                   concepto: str, importe: float, signo: str, fecha_movto: str = "", http_local=None):
    payload = {
        "tkncli": tkncli,
        "alias":  alias,
        "tipo":   tipo,
        "signo":  signo,
        "fecha_movto": fecha_movto,
        "concepto": concepto,
        "importe": float(importe)
    }
    return encripta_peticion(db, http_local, euser, eIp_Origen, etimestar, 'regmovto', payload, headertoken)

def consulta_sdomov(db, euser, eIp_Origen, etimestar, headertoken: str, tkncli: str, alias: str, http_local=None):
    """
    Recupera saldos y movimientos de un cliente.
    Siempre retorna dict con claves: 'rc', 'resp', 'rc1', 'resp1'
    """
    decriptMsg = []
    rc = 0


    payload = {"tkncli": tkncli, "alias": alias}


    rc, resp = encripta_peticion(db, http_local, euser, eIp_Origen, etimestar, 'selsaldos', payload, headertoken)
    
    if rc == 201:
        decriptMsg_raw, estado = decrypt_message(settings.PASSCYPH, resp)
        if estado:
            try:
                decriptMsg = json.loads(decriptMsg_raw)
            except Exception:
                decriptMsg = []
        else:
            decriptMsg = []
    else:
        decriptMsg = []

    return {"rc": rc, "resp": decriptMsg}
def consulta_movagre(db, euser, eIp_Origen, etimestar, headertoken: str, tkncli: str, alias: str, http_local=None):

    decriptMsg = []
    rc = 0


    payload = {
        "tkncli": tkncli,
        "alias":  alias,
        "fecha_ini":  '0001-01-01',
        "fecha_fin": '9999-12-31',
          }


    rc, resp = encripta_peticion(db, http_local, euser, eIp_Origen, etimestar, 'selmovtos', payload, headertoken)
    if rc == 201:
        decriptMsg_raw, estado = decrypt_message(settings.PASSCYPH, resp)
        if estado:
            try:
                decriptMsg = json.loads(decriptMsg_raw)
            except Exception:
                decriptMsg = []
        else:
            decriptMsg = []
    else:
        decriptMsg = []

    return {"rc": rc, "resp": decriptMsg}


# ===========================================================
# 4) callapi_alive: normalizar Authorization + no referenciar r en except
# ===========================================================
def callapi_alive(db, http_local, oper: str, payload_encrypted: str, headertoken: str, euser, eIp_Origen, etimestar,
                  timeout=(2, 10)):
    base = settings.call_bank
    entidad = app_state.entidad
    url = base + oper
    headers = {"Content-Type": "application/json"}
    headers['Authorization'] = headertoken
    try:
        r = crea_ses(http_local).post(url, data=payload_encrypted, headers=headers, timeout=timeout)
        print(f"Cuerpo de la respuesta: {r.text}")
        log_terceros(db, oper, payload_encrypted, r.text, r.status_code, euser, eIp_Origen, etimestar, entidad)
        return r.status_code, r.text
    except requests.RequestException as e:
        err = f'{{"response":"conexion rechazada: {str(e)}"}}'
        log_terceros(db, oper, payload_encrypted, err, 503, euser, eIp_Origen, etimestar,entidad)
        return 503, err

def crea_ses(http_local) -> requests.Session:
    s = getattr(http_local, "session", None)
    if s is None:
        s = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=200,
            pool_maxsize=200,
            max_retries=0
        )
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        http_local.session = s
    return s

def log_terceros(db, servicio, datosin, datosout, rc, user, ip, stime, entidad):
    try:
        log_level_name = logging.getLevelName(logging.getLogger().getEffectiveLevel())
        timestar = obtener_fecha_actual()
        timeend = obtener_fecha_actual()
        log_entry = DBLOGENTRY(
            entidad=entidad,
            timestar=timestar, timeend=timeend, log_level=log_level_name, funcion='',
            respcod=rc, nombre=user, Ip_Origen=ip, Servicio=servicio, Metodo='POST',
            DatosIn=str(datosin), DatosOut=str(datosout)
        )
        db.add(log_entry)
        return True
    except Exception as e:
        print("log_terceros error:", e)
        return False
