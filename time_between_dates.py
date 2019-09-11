"""
Comment here 
""" 
###Functional code begins here - edit at your own peril!
from datetime import datetime

now = datetime.now()
def days_since_now(a_datetime):
    days_since = now - a_datetime
    return(days_since.days)

def split_order_dates(datestring):
    all_dates = sorted([datetime.strptime(x,"%Y-%m-%d") for x in datestring.split(",")])
    return(all_dates)


def handle_order_history(event, lead, traveler, connected_record, resources):
    order_hist = connected_record.get_entities('Google BigQuery', 'OrderHistory')
    if order_hist == None or len(order_hist) == 0:
        return
   
    all_order_dates = order_hist[0].all_order_dates # Calls list of order dates on OrderHistory entity.
    
    if all_order_dates is None or len(all_order_dates)==0: #Checks that we have data to work with. 
        return

    all_days_since = [days_since_now(d) for d in split_order_dates(all_order_dates)] # Calculates number of days between now and each datetime in this list
    
    traveler.VarH = str(len([x for x in all_days_since if x > 365 and x <= 730]) >=1)
    
        
        
def handle_user_event(event, lead, traveler, connected_record, resources):
    order_hist = connected_record.get_entities('Google BigQuery', 'OrderHistory')
    if order_hist == None or len(order_hist) == 0:
        return
   
    all_order_dates = order_hist[0].all_order_dates # Calls list of order dates on OrderHistory entity.
    
    if all_order_dates is None or len(all_order_dates)==0: #Checks that we have data to work with. 
        return

    all_days_since = [days_since_now(d) for d in split_order_dates(all_order_dates)] # Calculates number of days between now and each datetime in this list
    
    traveler.VarH = str(len([x for x in all_days_since if x > 365 and x < 730]) >=1)

    
def handle_lead(event, lead, traveler, connected_record, resources):
    order_hist = connected_record.get_entities('Google BigQuery', 'OrderHistory')
    if order_hist == None or len(order_hist) == 0:
        return
   
    all_order_dates = order_hist[0].all_order_dates
    
    if all_order_dates is None or len(all_order_dates)==0:
        return

    all_days_since = [days_since_now(d) for d in split_order_dates(all_order_dates)]
    
    traveler.VarH = str(len([x for x in all_days_since if x > 365 and x < 730]) >=1)    
    
    
###Event handlers        
handler1 = NamedHandler(handle_order_history, 'handle_order_history')
trigger1 = OnChangeToFieldTrigger('Google BigQuery', 'OrderHistory', 'Id')
advanced_rule_registry.register(trigger1, handler1)

handler3 = NamedHandler(handle_user_event, 'handle_user_event')
trigger3 = OnChangeToFieldTrigger('Google BigQuery', 'UserEvents', 'Id')
advanced_rule_registry.register(trigger3, handler3)


handler2 = NamedHandler(handle_lead, 'handle_lead')
trigger2 = OnChangeToFieldTrigger('Google BigQuery', 'dim_user', 'Id')
advanced_rule_registry.register(trigger2, handler2)



