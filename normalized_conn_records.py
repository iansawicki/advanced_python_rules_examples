#!/usr/bin/python
# -*- coding: utf-8 -*-
import json


ents = {"buynowtrunc": ["max_solddatetime"],
        "liveacutiontrunc": ["max_date"],
        "prebidtrunc": ["max_buyerprebidauctiondatetime"],
        "timedauctiontrunc": ["max_timedauctionstartdatetime"]}

#    "timedauctiontrunc": ["max_timedauctionstartdatetime"

conn = 'Amazon S3: Import'

norm_dict = {}


def most_recent_time(dt_list):
    return(max(dt_list))


def normalize_mapped_ents(
        traveler,
        connected_record,
        ents=ents,
        conn=conn):
    if traveler.VarD is None:
        norm_dict = {}
    else:
        norm_dict = json.loads(traveler.VarD)
    for k, v in ents.items():
        traveler.VarR = str("Trying to get entities.")
        conn_record = connected_record.get_entities(conn, k)
        traveler.VarR = str("Trying to get entities.")
        
        if len(conn_record) > 0 and conn_record is not None:
            traveler.VarR = str("Connected record available.")
            for i in v:
                traveler.VarR = str(getattr(conn_record[0], i))
                norm_dict[k + "_" + i] = getattr(conn_record[0], i)
                traveler.VarD = json.dumps(norm_dict)
    return
        
def handle_event(
    event,
    lead,
    traveler,
    connected_record,
    resources,
    ):
    traveler.VarR = str("Initialized")
    return(normalize_mapped_ents(traveler,connected_record))


handler = NamedHandler(
    handle_event,
    'handle_event')


  
advanced_rule_registry.register(OnChangeToFieldTrigger(conn, 'acusertable','Id'), handler)
advanced_rule_registry.register(OnChangeToFieldTrigger(conn, 'buynowtrunc','Id'), handler)
advanced_rule_registry.register(OnChangeToFieldTrigger(conn, 'liveacutiontrunc','Id'), handler)
advanced_rule_registry.register(OnChangeToFieldTrigger(conn, 'prebidtrunc','Id'), handler)
advanced_rule_registry.register(OnChangeToFieldTrigger(conn, 'timedauctiontrunc','Id'), handler)
