"""
Set the campaign_cats value by entering a list of apparel categories between the square brackets. 
Each category should be wrapped in quotes and separated from the next one by a comma. 
""" 

campaign_cats = ["Clutch Bags","Wallets", "Accessories"]


###Functional code starts here. 

campaign_cats = [i.upper() for i in campaign_cats]

def any_matches(a,b):
    if len([i for i in a if i in b]) >= 1:
        return(True)
    return(False)
    
def handle_user_events(event, lead, traveler, connected_record, resources):
    traveler.VarB = str(event.categories_list)
    traveler.VarG = str(event.subcategories_list)
    
    
def handle_lead(event, lead, traveler, connected_record, resources):
    myCats = connected_record.get_entities('Google BigQuery', 'UserEvents')[0].categories_list
    mySubCats = connected_record.get_entities('Google BigQuery', 'UserEvents')[0].subcategories_list
    allCats = myCats+", "+mySubCats
    allCats = allCats.upper()
    
    if allCats is None:
        return
    traveler.VarB = any_matches(campaign_cats,allCats)
        
handler1 = NamedHandler(handle_user_events, 'handle_user_events')
trigger1 = OnChangeToFieldTrigger('Google BigQuery', 'UserEvents', 'Id')
advanced_rule_registry.register(trigger1, handler1)

handler2 = NamedHandler(handle_lead, 'handle_lead')
trigger2 = OnChangeToFieldTrigger('Google BigQuery', 'dim_user', 'Id')
advanced_rule_registry.register(trigger2, handler2)
