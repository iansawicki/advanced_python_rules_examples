#!/usr/bin/python
# -*- coding: utf-8 -*-
a_dict = {}


def handle_event(
    event,
    lead,
    traveler,
    connected_record,
    resources,
    ):
    ast = connected_record.get_entities('Google BigQuery',
            'user_brand_ast')
    du = connected_record.get_entities('Google BigQuery', 'dim_user')

    if ast is not None or len(ast) >= 1:
        brand_ast_list = ast[0].USER_ASTHETICS_RANKED.split(",")
        a_dict['aesthetic1'] = brand_ast_list[0].split('-')[0]
        
    if du is not None or len(du) >= 1:
        a_dict['predictedGender'] = du[0].__um__models__predicted_gender
        traveler.VarI =  str(du[0].user_id)

    if a_dict is not None:
        traveler.VarD = str([a_dict])
        return
    else:
        return


handler = NamedHandler(handle_event, 'handle_event')

# Handle changes to dim_user entity

trigger1 = OnChangeToFieldTrigger('Google BigQuery', 'user_brand_ast','Id')
advanced_rule_registry.register(trigger1, handler)

# Handle changes to dim_user entity

trigger2 = OnChangeToFieldTrigger('Google BigQuery', 'dim_user', 'Id')
advanced_rule_registry.register(trigger2, handler)
