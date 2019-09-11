now = datetime.datetime.now()
def days_since_now(a_datetime):
  days_since = now - a_datetime
  return(days_since.days)

def split_order_dates(datestring):
  all_dates = sorted([datetime.datetime.strptime(x,"%Y-%m-%d") for x in datestring.split(",")])
  return(all_dates)

all_days_since = [days_since_now(d) for d in split_order_dates(orders_string)]

if len ([x for x in all_days_since if x > 365 and x < 730]) > =1:
  true
