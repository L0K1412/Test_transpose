import requests
import json
from datetime import datetime
import pytz
import time

chain_list = ['ethereum', 'polygon', 'scroll', 'arbitrum', 'canto','nova']

def timestamp_to_readable(ts, timezone="Etc/GMT", format="%Y-%m-%dT%H:%M:%SZ"):
    # https://en.wikipedia.org/wiki/List_of_tz_database_time_zones Asia/Ho_Chi_Minh, Asia/Singapore, America/Los_Angeles
    tz = pytz.timezone(timezone)
    return datetime.fromtimestamp(ts, tz=tz).strftime(format)

def readable_to_timestamp(str, format="%Y-%m-%dT%H:%M:%SZ"):
    tz = pytz.timezone('Etc/GMT')
    dt = datetime.strptime(str, format)
    dt = tz.localize(dt)
    return dt.timestamp()

def get_dex_volumn_by_time(chain_id, contract_address, time_from, time_to):
    # print(time_from)
    # print(time_to)
    url = "https://api.transpose.io/sql"
    sql_query = """
    select date_trunc('hour', timestamp) as time,
        from_token_address, quantity_in, to_token_address, quantity_out,
        sender_address as dex_address, origin_address, transaction_hash
    from {{chain_id}}.dex_swaps
    where (lower(from_token_address) = lower('{{contract_address}}') or lower(to_token_address) = lower('{{contract_address}}'))
        and timestamp >= to_timestamp({{time_from}}) and timestamp < to_timestamp({{time_to}})
    order by timestamp asc
    """
    headers = {
        'Content-Type': 'application/json',
        'X-API-KEY': '8KfPOUqpGxQnrFCLLsAt7dA8ljkzNsSl',
    }
    
    response = requests.post(url,
            headers=headers,
            json={
                'sql': sql_query,
                'parameters': {
                    'chain_id' : chain_id,
                    'contract_address': contract_address,
                    'time_from' : time_from,
                    'time_to' : time_to
                }
            },
        )
    
    response_json = json.loads(response.text)
    
    #print(response_json)
    # response_json is the result returned from the API as a dict
    data = response_json["results"]
    
    #sum volumn based on time
    time_dict = {}
    for transaction in data:
        key = transaction['time']
        if key in time_dict:
            if transaction['from_token_address'].lower() == contract_address.lower():
                time_dict[key]['value'] += transaction['quantity_in']
            else:
                time_dict[key]['value'] += transaction['quantity_out']
        else:
            if transaction['from_token_address'].lower() == contract_address.lower():
                time_dict[key] = {'value': transaction['quantity_in']}
            else:
                time_dict[key] = {'value': transaction['quantity_out']}
    
    dex_volumn = []
    
    #Create a new array to save result from dict
    for key, value in time_dict.items():
        new = {'time': key, 'volumn': value}
        dex_volumn.append(new)
    
    # Sort the list by 'time' field in ascending order
    dex_volumn.sort(key=lambda x: x['time'])

    return dex_volumn

def get_dex_volumn(chain_id, contract_address, time_from):
    if chain_id not in chain_list:
        print(f"Error: {chain_id} not in supported chain list")
        print("Supported chain list:", end = "")
        print(chain_list)
        return
    now = int(time.time())
    if (now - time_from < 3*60*60):
        data = get_dex_volumn_by_time(chain_id, contract_address, time_from, now)
    else:
        time_from = (time_from // 86400) * 86400
        end_time = time_from
        data = []
        while end_time <= now:
            time_to = end_time + 3*60*60
            data = data + get_dex_volumn_by_time(chain_id, contract_address, end_time, time_to)
            end_time = end_time + 3*60*60
    
    return data


#support chains: ethereum, polygon, scroll, arbitrum, canto, nova

#test
# 2 day - 21/4
#data_dex = get_dex_volumn('ethereum','0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48', 1681862400) 
# 7 day - 21/4
data_dex = get_dex_volumn('arbitrum','0x912CE59144191C1204E64559FE8253a0e49E6548', 1683601204)
# 2 day - 4/5
#data_dex = get_dex_volumn('arbitrum','0x912CE59144191C1204E64559FE8253a0e49E6548', 1683158400)
#print(data_cex[0])

for item in data_dex:
   print(item)
