class AppState:
    def __init__(self):
        self._jsonenv = None
        self._jsonrec = None
        self._user_id = None
        self.time_star = None
        self.ip_origen = None
        self.entidad   = None
    
    @property
    def jsonenv(self):
        return self._jsonenv
    
    @jsonenv.setter
    def jsonenv(self, value):
        self._jsonenv = value

    @property
    def jsonrec(self):
        return self._jsonrec
    
    @jsonrec.setter
    def jsonrec(self, value):
        self._jsonrec = value

    @property
    def user_id(self):
        return self._user_id
    
    @user_id.setter
    def user_id(self, value):
        self._user_id = value

    @property
    def entidad(self):
        return self._entidad
    
    @entidad.setter
    def entidad(self, value):
        self._entidad = value

app_state = AppState()