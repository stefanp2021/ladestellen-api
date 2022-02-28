

import pandas


def func_dump(obj):
    """To get all items from an Object"""
    for attr in dir(obj):
        print("obj.%s = %r" % (attr, getattr(obj, attr)))

def func_iterateLists(takelist):
    return_list = []
    """ iterate over lists in a list because some values are saved in that way"""
    for list in takelist:
        for number in list:
            print(type(number))
            if number:
                return_list.append(number)
    return(return_list)


def func_GetRidofNone(wert):
    if((wert is None) or (wert == "Null") or (wert=="NULL") or (wert == "None") or (wert == "NONE")):
        wert=""
        return(wert)
    else:
        return(wert)


def func_not_so_magic_search(srchs, col, df):
    """ search whole DataFrame where the DeviatingOperator matches"""
    bools = pandas.concat([df[col].apply(lambda x: srch in x) for srch in srchs],axis=1)
    return df[bools.sum(axis=1) > 0]
