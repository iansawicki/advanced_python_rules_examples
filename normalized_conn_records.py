#!/usr/bin/python
# -*- coding: utf-8 -*-
import json
from datetime import datetime


ents = {"buynowtrunc": ["max_solddatetime"],
        "liveacutiontrunc": ["max_date"],
        "prebidtrunc": ["max_buyerprebidauctiondatetime"],
        "timedauctiontrunc": ["max_timedauctionstartdatetime"]}

time_fields = ["buynowtrunc_max_solddatetime","liveacutiontrunc_max_date","prebidtrunc_max_buyerprebidauctiondatetime","timedauctiontrunc_max_timedauctionstartdatetime"]

conn = 'Amazon S3: Import'

norm_dict = {}


def most_recent_time(dates):
    return(max(dates, key=lambda d: datetime.strptime(d, '%Y-%m-%d')))



def normalize_mapped_ents(
        traveler,
        connected_record,
        ents=ents,
        conn=conn):
    if traveler.VarDataStorage is None:
        norm_dict = {}
    else:
        norm_dict = json.loads(traveler.VarDataStorage)
    for k, v in ents.items():
        traveler.VarR = str("Trying to get entities.")
        conn_record = connected_record.get_entities(conn, k)
        traveler.VarR = str("Trying to get entities.")
        
        if len(conn_record) > 0 and conn_record is not None:
            traveler.VarR = str("Connected record available.")
            for i in v:
                traveler.VarR = str(getattr(conn_record[0], i))
                norm_dict[k + "_" + i] = getattr(conn_record[0], i)
                traveler.VarDataStorage = json.dumps(norm_dict)
    return
        
def handle_event(
    event,
    lead,
    traveler,
    connected_record,
    resources,
    ):
    traveler.VarR = str("Initialized")
    normalize_mapped_ents(traveler,connected_record)
    time_vals = [v for k,v in json.loads(traveler.VarDataStorage).items() if k in time_fields]
    time_vals = [tv for tv in time_vals if tv!=None]
    traveler.VarR = str("The length of time_vals is {}".format(len(time_vals)))
    if len(time_vals) > 0:
        traveler.VarM = str(most_recent_time(time_vals))
    


handler = NamedHandler(
    handle_event,
    'handle_event')


  
advanced_rule_registry.register(OnChangeToFieldTrigger(conn, 'acusertable','Id'), handler)
advanced_rule_registry.register(OnChangeToFieldTrigger(conn, 'buynowtrunc','Id'), handler)
advanced_rule_registry.register(OnChangeToFieldTrigger(conn, 'liveacutiontrunc','Id'), handler)
advanced_rule_registry.register(OnChangeToFieldTrigger(conn, 'prebidtrunc','Id'), handler)
advanced_rule_registry.register(OnChangeToFieldTrigger(conn, 'timedauctiontrunc','Id'), handler)
