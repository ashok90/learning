## Imports 

import pandas as pd
from pandas.io.json import json_normalize
import json
import os
import ast

############### casting functions #################
def _filter_column_name(colLst,inclStr='!@#$%',exclStr='!@#$%'):
    """Function to filter list with include/exclude pattern"""
    tmp=[]
    for i in colLst:
        if i.find(inclStr)!=-1 and i.find(exclStr)==-1:
            tmp.append(i)
    return tmp


def _cast_df_col(dtfrm,collst,casttype):
    """Function to cast dataframe columns int/floats"""
    newcol=[]
    for col in collst:
        if col in dtfrm.columns:
            newcol.append(col)
            
    if casttype=='int64': 
        dtfrm[newcol]=dtfrm[newcol].progress_apply(lambda a: pd.to_numeric(a,downcast="integer"))     
    elif casttype=='float':
        dtfrm[newcol]=dtfrm[newcol].progress_apply(lambda a: pd.to_numeric(a,downcast=casttype))
    else:
        for col in newcol:
            dtfrm[newcol]=dtfrm[newcol].astype(casttype)
            
    return dtfrm       
        
def _cast_df_col_dt(dtfrm,collst,casttype):
    """Function to cast dataframe columns date formattings"""
    for col in collst:
        if col in dtfrm.columns:
            dtfrm[col]=pd.to_datetime(df[col]).dt.strftime(casttype).astype(str)
    return dtfrm

    
def _stringtoType(datafrm,collst,default="list"):
    """Function to typecase string to given type"""
    if default=="list":
        def_val="[]"
    elif default=="dict":
        def_val="{}"
    else:
        def_val=""    
    for col in collst:
        datafrm.loc[datafrm[col] == "", col]= def_val
        datafrm.loc[datafrm[col] != "", col]= datafrm[col].astype(str)#.replace("\\", "")
        datafrm[col]=datafrm[col].progress_apply(ast.literal_eval)#.apply(eval(default))
    return datafrm

def _stringtoliteral(datafrm,collst,default="list"):
    """Function to typecase string to given type"""
    if default=="list":
        def_val="[]"
    elif default=="dict":
        def_val="{}"
    else:
        def_val=""
    for col in collst:
        datafrm.loc[datafrm[col] == "", col]= def_val
        #df.loc[df[col] != "", col]= df[col].astype(str)#.replace("\\", "")
        datafrm[col]=datafrm[col].progress_apply(ast.literal_eval)#.apply(eval(default))
    return datafrm

def dictpop(dictval,popKey):
    try:
        dictval.pop(popKey)
    except:
        pass;
    return dictval

############### json functions #################
def _jsonifyN(x):
    
    tmp=len(x)
    tmp1=0
    for n,i in enumerate(x):
        if is_empty(i):
            #print(x)
            tmp1+=1
    #print(tmp1,tmp)
    if tmp1==tmp:
        return ""
    else:
        return json.loads(x.to_json(orient="columns").replace("\\", ""))    
    
    
def _replaceColName(colName):
    replaceStr=[]
    t=colName
    for i in replaceStr:
        #print("-----------")
        if colName.find(i)!=-1:
            #print(colNm,colNm.find(i),i)
            t=colName.replace(i,'')
        elif colName[-1]=="_":
            t=colName[:-1]
        else:
            pass;
        #print("-----------")
    return t
    
def _createCols_withJson2(datafrm,withCols,newcolNm,defaultType):
    print(withCols)
    tmpDF=datafrm[withCols].rename(columns=lambda x: _replaceColName(x.strip())).copy()
    #print(tmpDF.columns)
    if defaultType=="list":
        datafrm[newcolNm]=tmpDF.progress_apply(_jsonify1,axis=1)
    else:
        datafrm[newcolNm]=tmpDF.progress_apply(_jsonifyN,axis=1)
    datafrm[newcolNm]=datafrm[newcolNm].astype(str)
    _stringtoliteral(datafrm,[newcolNm],default=defaultType)
    #_stringtoType(datafrm,[newcolNm],defaultType)
    datafrm.drop(withCols, axis=1,inplace=True)
    #print(tmpDF)
    return datafrm,tmpDF



def flattenjson( b, delim="_" ):
    val = {}
    for i in b.keys():
        if isinstance( b[i], dict ):
            get = flattenjson( b[i], delim )
            for j in get.keys():
                val[ i + delim + j ] = get[j]
        else:
            val[i] = b[i]
    return val

def writeStr(st,flnm):
    f= open(wrkng_dir+"\\"+flnm,"a+")
    f.write(st+"\n")
    f.close
    return 

def formatMSG(x):
    try:
        #print(list(x[0].items()))
        t=[]
        d={}
        for n,i in x[0].items():
            d[n]=i
        t.append(d)
        return t
        
    except:
        return list(x[0])

def mergedicts(x,y):
    t={}
    t['dict1']=x
    t['dict2']=y
    return [t]
