#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 19 11:50:49 2020

@author: deepti
"""

import os
import sys
import csv
import numpy as np
import re
import itertools as it

Aggregate_function=["min","max","avg","sum","count","distinct"]
operation_perform=["select","from","where"]
Relation_function=[">","<","<=",">=","<>","="]
schema={}
###store_list=[]

def end(err):
    print(err)
    exit(-1)

def datatype(a):
    try:
        _=int(a)
        return True
    except:
        return False
    


def load_table(file_name):
    return np.genfromtxt(file_name,dtype=int,delimiter=',')


def output(store_list):
    
    alias2tb=store_list[1]
    inter_column=store_list[5]
    tables=store_list[0]
    conditions=store_list[3]
    cond_op=store_list[4]
    project_column=store_list[2]
    
    

    # load all tables and retain only necessary columns
    # also decide the indexes of intermediate table columns
    colindex = {}
    tabletostore=[]
    count = 0
    let=0
    check=0
    all_tables = []
    for t in tables:
        l = load_table("{}.csv".format( alias2tb[t] ))

        idx=[]
        for cname in inter_column[t]:
            idx.append(schema[alias2tb[t]].index(cname))
            
        l = l[:, idx]
        all_tables.append(l.tolist())
        
    
        colindex[t] = {cname: count+i for i, cname in enumerate(inter_column[t])}
        count =count+len(inter_column[t])

    # cartesian product of all tables
    ##This tool computes the cartesian product of input iterables.
    ##It is equivalent to nested for-loops. 
    ##For example, product(A, B) returns the same as ((x,y) for x in A for y in B)
    
    #inter_table=[]
    #for r in it.product(*all_tables):
     #   for tup in r:
      #      for i in list(tup):
       #         inter_table.append(i)
    check_table=[]
    inter_table = [[i for tup in r for i in list(tup)] for r in it.product(*all_tables)]
    check_table=inter_table;
    inter_table = np.array(inter_table)
    File_here=False
    # check for conditions and get reduced table
    if len(conditions):
        np_ones = np.ones((inter_table.shape[0],len(conditions)), dtype=bool)
        
        for idd, (conditional_op, left, right) in enumerate(conditions):
            cols = []
            for tname, cname in [left, right]:
                if(tname == "<literal>"):   # means table 1 or table 2
                    file_here=True
                    cols.append(np.full((inter_table.shape[0]), int(cname)))
                else: 
                    cols.append(inter_table[:, colindex[tname][cname]])

            if (conditional_op=="<="): 
                np_ones[:, idd]=(cols[0] <= cols[1])
            elif (conditional_op==">="): 
                np_ones[:, idd]=(cols[0] >= cols[1])
            elif (conditional_op=="<>"): 
                np_ones[:, idd]=(cols[0] != cols[1])
            elif (conditional_op=="<"): 
                np_ones[:, idd]=(cols[0] < cols[1])
            elif (conditional_op==">"): 
                np_ones[:, idd]=(cols[0] > cols[1])
            elif (conditional_op=="="): 
                np_ones[:, idd]=(cols[0] == cols[1])
                
            else:
                print("UNDEFINED OPERATOR!!!!")

        
        if cond_op == "and": 
            final_take = (np_ones[:, 0] & np_ones[:, 1])
            
        elif cond_op == "or": 
            final_take = (np_ones[:, 0] |  np_ones[:, 1])
        else: 
            final_take = np_ones[:, 0]
        inter_table = inter_table[final_take]
    where_index=[]
    from_index=[]
    select_index=[]
    for tn, cn, aggr in project_column:
        select_index.append(colindex[tn][cn])

    inter_table = inter_table[:, select_index]

    # process for aggregate function
    if project_column[0][2]:
        out_table = []
        dist = False
        ##breakstatement=False
        for idx, (tn, cn, aggrigate) in enumerate(project_column):
            col = inter_table[:, idx]
            if (aggrigate == "min"): 
                out_table.append(min(col))
                
                
            elif (aggrigate == "max"): 
                out_table.append(max(col))
                
                
            elif (aggrigate == "sum"): 
                out_table.append(sum(col))
                
                
            elif (aggrigate == "avg"): 
                out_table.append(sum(col)/col.shape[0])
                
                
            elif (aggrigate == "count"): 
                out_table.append(col.shape[0])
                
                
            elif (aggrigate == "distinct"):
                seen = set()
                out_table = [x for x in col.tolist() if not (x in seen or seen.add(x) )]
                dist = True
            else: 
                if(True):
                    print("INVALID AGGREGATION FUNCTION!!!!")
                    exit(-1)
                
        out_table = np.array([out_table])
        if dist: 
            out_table = np.array(out_table).T
            
        out_header=[]
        for tn, cn, aggrigate in project_column:
            out_header.append("{}.{}".format(tn, cn))
   
    else:
        out_table = inter_table
        out_header=[]
        for tn, cn, aggrigate in project_column:
            out_header.append("{}.{}".format(tn, cn))
    
    out_table=out_table.tolist()  ##converting into list
    return out_header, out_table


def projectionofcolumn(raw_column,tables,alias2tb):
    raw_column="".join(raw_column).split(",")
    project_column=[]
    for rc in raw_column:
        regmatch=re.match("(.+)\((.+)\)",rc)
        
        if(regmatch):
            aggr,rc=regmatch.groups()
        else:
            aggr=None
            
      
        if("." in rc and len(rc.split("."))!=2):
            print("INVALID COLUMN NAME!!!!")
            exit(-1)
        tname=None
        
        if("." in rc):
            tname,cname=rc.split(".")
            if(tname not in alias2tb.keys()):
                print("UNKNOWN FEILD!!!!")
                exit(-1)
            
            
        else:
            cname=rc
            #tname=[]
            if(cname!="*"):
                tname=[t for t in tables if cname in schema[alias2tb[t]]]
                #for t in tables:
                 #   if(cname in schema[alias2tb[t]]):
                  #      tname.append(t)
                if(len(tname)>1):
                    print("DUPLICATE FEILD!!!!")
                    exit(-1)
                
                if(len(tname)==0):
                    print("UNKNOWN FEILD!!!!")
                    exit(-1)
               
                tname=tname[0]
                
            else:
                pass
        project_=[]   
        if(cname=="*"):
            if(aggr!=None):
                print("WE CAN NOT USE AGGREGATE HERE!!!!")
                exit(-1)
            
            if(tname!=None):
                
                #for t in tables:
                 #   if(cname in schema[alias2tb[t]]):
                  #      tname.append(t)
                project_.append([(tname,c,aggr) for c in schema[alias2tb[tname]]])
                project_column.extend([(tname,c,aggr) for c in schema[alias2tb[tname]]])
                
            else:
                for t in tables:
                    project_.append([(t,c,aggr) for c in schema[alias2tb[t]]])
                    project_column.extend([(t,c,aggr) for c in schema[alias2tb[t]]])
                    
        else:
            if(cname not in schema[alias2tb[tname]]):
                print("UNKONOWN FEILD!!!!")
                exit(-1)
            
            project_column.append((tname,cname,aggr))
    
    s=[]
    for t,c,a in project_column:
        s.append(a)
    if(all(s)^any(s)):
        print("AGGREGATED AND NON-AGGREGATED BOTH ARE NOT ALLOWED SIMULTANEOUSLY!!!!")
        exit(-1)
    
    if(any([(a=="distinct") for a in s]) and len(s)!=1):
        print("DISTINCT CANNOT BE USED WITH OTHER COLUMNS!!!!")
        exit(-1)
    
    
    return project_column



def conditionparsing(raw_condition,tables,alias2tb):
    conditions=[]
    cond_op=None
    if(raw_condition):
        raw_condition=" ".join(raw_condition)
        
        if("or" in raw_condition):
            cond_op="or"
            
        elif("and" in raw_condition):
            cond_op="and"
            
        if(cond_op):
            raw_condition=raw_condition.split(cond_op)
            
        else:
            raw_condition=[raw_condition]
            
        for condition in raw_condition:
            
            try:
                if(("=") in condition):
                    op = "="
                elif(("<>") in condition): 
                    op = "<>"
                elif(("<=") in condition):
                    op = "<="
                elif((">=") in condition): 
                    op =">="
                elif((">") in condition): 
                    op = ">"
                elif(("<") in condition): 
                    op = "<"
                else:
                    if(True):
                        print("Error: Invalid Condition you are trying to give : {}".format(condition))
                        exit(-1)
                   
            except:
                print("Something Invalid here!!!")
            

            l,r=condition.split(op)
            l=l.strip()
            
            r=r.strip()

            related_op,left,right=op,l,r
            parsed_condition=[related_op]
            
            for idd,rc in enumerate([left,right]):
                if(datatype(rc)):
                    parsed_condition.append(("<literal>",rc))
                    continue
                    
                if("." in rc):
                    tname,cname=rc.split(".")
                    
                else:
                    cname=rc
                    tname=[t for t in tables if rc in schema[alias2tb[t]]]
                    if(len(tname)>1):
                        print("DUPLICATE FEILD!!!!")
                        exit(-1)
                        
                    if(len(tname)==0):
                        print("UNKNOWN FEILD!!!!")
                        exit(-1)
                   
                    tname=tname[0]
                    
                if((tname not in alias2tb.keys()) or (cname not in schema[alias2tb[tname]])):
                    print("UNKNOWN FEILD")
                    exit(-1)
                
                
                parsed_condition.append((tname,cname))
                
            conditions.append(parsed_condition)
            
    return conditions,cond_op
                


def parsingthequery(query):
    
    tokens=query.lower().split()
    ##print(tokens)   
    ##['select', '*', 'from', 'table1', 'where', 'a=411']
    if(tokens[0]!="select"):
        print("ERROR! QUERY MUST START WITH SELECT!!!")
        exit(-1)
    
    
    select_=[]
    for idd,t in enumerate(tokens):
        if(t=="select"):
            select_.append(idd)
            
    from_=[]
    for idd,t in enumerate(tokens):
        if(t=="from"):
            from_.append(idd)
    
    where_=[]
    for idd,t in enumerate(tokens):
        if(t=="where"):
            where_.append(idd)
    
    
    
    ##print(select_)
    ##print(from_)
    ##print(where_)
    
    if((len(select_)!=1)or(len(from_)!=1)or(len(where_)>1)):
        print("INVALID QUERY!!!!")
        exit(-1)
    
    
    select_,from_=select_[0],from_[0]
    
    where_=where_[0] if len(where_)==1 else None  ## if query contain where
    
    if(from_<=select_):
        print("INVALID QUERY, select must be use before from!!!!")
        exit(-1)
    
    
    if(where_):
        temptables=tokens[from_+1:where_]
        tempcondition=tokens[where_+1:]
        
    else:
        temptables=tokens[from_+1:]
        tempcondition=[]
    
    raw_column=tokens[select_+1:from_]
    
    if(len(temptables)==0):
        print("NO TABLE PROVIDED!!!!")
        exit(-1)
        
    if(where_!=None and len(tempcondition)==0):
        print("NO CONDITION PROVIDED AFTER WHERE!!!!")
        exit(-1)
    
    
    raw_tables,raw_column,raw_condition=temptables,raw_column,tempcondition
    
    ### parse the table....
    raw_tables=" ".join(raw_tables).split(",")
    tables=[]
    alias2tb={}
    for dp in raw_tables:
        t=dp.split()
        if(not(len(t)==1 or (len(t)==3 and t[1] =="as"))):
            print("INVALID TABLE SPECIFICATIONS!!!!")
            exit(-1)
        
        if(len(t)==1):
            tb_name,tb_alias=t[0],t[0]
        else:
            tb_name,tb_alias=t
        
        if(tb_name not in schema.keys()):
            print("INVALID TABLE NAME :{}".format(tb_name))
            exit(-1)
        
        if(tb_alias in alias2tb.keys()):
            print("DUPLICATE ALIAS!!!!")
            exit(-1)
        
        
        tables.append(tb_alias)
        alias2tb[tb_alias]=tb_name
    
    project_column=projectionofcolumn(raw_column,tables,alias2tb) ## to get the columns which are projected
    
    conditions,cond_op=conditionparsing(raw_condition,tables,alias2tb)  ##to get the conditions and conditon operations we are using in where clause
    
    ##choose all needed columns
    
    inter_column={t:set() for t in tables}
    
    for tn,cn,_ in project_column:
        inter_column[tn].add(cn)
        
    list_of_where=[]
    for condition in conditions:
        for tn,cn in condition[1:]:
            if(tn=="<literal>"):
                continue
                
            inter_column[tn].add(cn)
            
    for t in tables:
        inter_column[t]=list(inter_column[t])
    
    return_list=[]
    
    return_list.append(tables)
    return_list.append(alias2tb)
    return_list.append(project_column)
    return_list.append(conditions)
    return_list.append(cond_op)
    return_list.append(inter_column)
    
    """
    print(tables)
    print(alias2tb)
    print(project_column)
    print(conditions)
    print(cond_op)
    print(inter_column)
    
    """
    
    
    return return_list    ## A dictionary which contain all the projections , conditions , all details
    
### STARTING OF MAIN FUNCTION!!!!
    
if __name__ == "__main__":
    
    
    try:
        with open("metadata.txt","r") as f:
            data=f.readlines()
        
        data=[t.strip() for t in data if t.strip()]
        table=None
        for t in data:
            t=t.lower()  #to make case-insensitive
            if(t=="<begin_table>"):
                attrs,table=[],None
            
            elif(t=="<end_table>"):
                pass
        
            elif(not table):
                table,schema[t]=t,[]
            
            else:
                schema[table].append(t)
                
    except:
        print("Sorry!! metadata.txt file is not exist!!!!")
        
        
    if(len(sys.argv) != 2):
        print("ERROR : NOT ENOUGH ARGUMENT PROVIDED!!!!")
        print("USAGE : python {} '<sql query>'".format(sys.argv[0]))
        exit(-1)
    q = sys.argv[1]
    ##q="select * from table1 where a=411;"
    query = q
    sz = len(query)-1

    if query[sz]==';':
        query = query[0:sz]
    else:
        err_string = 'ERROR!!!!QUERY DOES NOT END WITH ";"'   ## if query not ending with ; show error
        end(err_string)
        #exit(-1)

    outputofquery = parsingthequery(query)
    out_header, out_table = output(outputofquery)

    
    print(",".join(map(str, out_header)))
    for row in out_table:
        print(",".join(map(str, row)))
