"""
Set the campaign_brands value by entering a list of brand names between the square brackets. 
Each brand name should be wrapped in quotes and separated from the next brand name by a comma. 
""" 

campaign_brands = ["JOHN RICHMOND","DSQUARED2","BALENCIAGA"]  

### Functional code starts here. 

campaign_brands = [x.upper() for x in campaign_brands]


def any_matches(a,b):
    if len([i for i in a if i in b]) >= 1:
        return("TRUE")
    return("FALSE")
    
def handle_user_events(event, lead, traveler, connected_record, resources):
    traveler.VarB = str(event.brands_list)
    
    
def handle_lead(event, lead, traveler, connected_record, resources):
    user_events = connected_record.get_entities('Google BigQuery', 'UserEvents')
    if user_events == None or len(user_events) == 0:
        return 
    myBrands = user_events[0].brands_list
    if myBrands is None:
        return
    myBrands = myBrands.upper()
    traveler.VarB = any_matches(campaign_brands,myBrands)
        

handler1 = NamedHandler(handle_user_events, 'handle_user_events')
trigger1 = OnChangeToFieldTrigger('Google BigQuery', 'UserEvents', 'Id')
advanced_rule_registry.register(trigger1, handler1)

  
handler2 = NamedHandler(handle_lead, 'handle_lead')
trigger2 = OnChangeToFieldTrigger('Google BigQuery', 'dim_user', 'Id')
advanced_rule_registry.register(trigger2, handler2)


