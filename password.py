import keyring

def setbasicauth():
    keyring.set_password('basicauth','stefanpirker', 'xWQ24m2z3AqKP4HcarZz')

def getbasicauth():
    a=keyring.get_password('basicauth','stefanpirker')
    return(a)