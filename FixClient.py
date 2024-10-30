import quickfix as fix 

class FixClient(fix.Application): 

    settingsDic = {} 
    TRADE_SESS = None 
    QUOTE_SESS = None 

    def onCreate(self, sessionID): 
        '''Improve this function later. Right now it expects 
        exactly two sessions which contain specific strings''' 
        
        num_sess = self.session_settings.size() 
        if sessionID.toString().lower().find('quote') != -1: 
            #if the sessionID contains the word 'quote' assume it is a quote session 
            self.QUOTE_SESS = sessionID 
        elif sessionID.toString().lower().find('trade') !=-1: 
            #if sessionID contains 'trade' assume it is a trade session. 
            self.TRADE_SESS = sessionID 
            self.settingsDic[sessionID.toString()] = self.session_settings.get(sessionID) 
        return 

    def onLogon(self, sessionID): 
        self.sessionID = sessionID 
        return 

    def onLogout(self, sessionID): 
        return 

    def toAdmin(self, message, sessionID): 
        msg_type = message.getHeader().getField(fix.MsgType().getField()) 
        if msg_type == fix.MsgType_Logon: 
            username = self.settingsDic[sessionID.toString()].getString('SenderCompID') 
            password = self.settingsDic[sessionID.toString()].getString('Password') 
            message.setField(fix.Username(username)) 
            message.setField(fix.Password(password)) 
        return 

    def fromAdmin(self, message, sessionID): 
        fix_str = unicode_fix(message.toString())
        self.decoder.print_report(message) 
        return 

    def toApp(self, message, sessionID): 
        fix_str = unicode_fix(message.toString()) 
        return 

    def fromApp(self, message, sessionID): 
        '''Capture Messages coming from the counterparty''' 
        fix_str = unicode_fix(message.toString())
        self.decoder.print_report(message) 
        return 
        
def unicode_fix(string): 
    """Take string and replace characters FIX '|' characters 
       to ones that appear correctly in the terminal and 
       return it""" 
    bar_in_unicode = '\x01' # '|' from FIX messages in unicode 
    new_str = string.replace(bar_in_unicode, '|') 
        
    return new_str