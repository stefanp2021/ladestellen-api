import keyring

def setbasicauth():
    keyring.set_password('basicauth','stefanpirker', 'xWQ24m2z3AqKP4HcarZz')

def getbasicauth():
    keyring.get_password('basicauth','stefanpirker')     