## Imports 

import pandas as pd
from pandas.io.json import json_normalize
import json
import os
import ast

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
