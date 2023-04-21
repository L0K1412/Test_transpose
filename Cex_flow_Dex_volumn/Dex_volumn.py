import requests
import json
from datetime import datetime
import pytz
import time

#create list of dex address
dex_address = ['0xe66b31678d6c16e9ebf358268a790b763c133750',
            '0xdef1c0ded9bec7f1a1670819833240f027b25eff',
            '0x22f9dcf4647084d6c31b2765f6910cd85c178c18',
            '0xd47140f6ab73f6d6b6675fb1610bb5e9b5d96fe5',
            '0x1111111254fb6c44bac0bed2854e76f90643097d',
            '0x11111112542d85b3ef69ae05771c2dccff4faa26',
            '0x220bda5c8994804ac96ebe4df184d25e5c2196d4',
            '0xf2f400c138f9fb900576263af0bc7fcde2b1b8a8',
            '0x288931fa76d7b0482f0fd0bca9a50bf0d22b9fef',
            '0xdb38ae75c5f44276803345f7f02e95a0aeef5944',
            '0x11111254369792b2ca5d084ab5eea397ca8fa48b',
            '0xfade503916c1d1253646c36c9961aa47bf14bd2d',
            '0x9021c84f3900b610ab8625d26d739e3b7bff86ab',
            '0xe069cb01d06ba617bcdf789bf2ff0d5e5ca20c71',
            '0x8df6084e3b84a65ab9dd2325b5422e5debd8944a',
            '0x63f0797015489d407fc2ac7e3891467e1ed0166c',
            '0x9008d19f58aabd9ed0d60971565aa8510560ab41',
            '0xa2b47e3d5c44877cca798226b7b8118f9bfb7a56',
            '0x52ea46506b9cc5ef470c5bf89f17dc28bb35d85c',
            '0x45f783cce6b7ff23b2ab2d70e416cdb7d6055f51',
            '0xf7ca8f55c54cbb6d0965bc6d65c43adc500bc591',
            '0xa356867fdcea8e71aeaf87805808803806231fdc',
            '0x31e085afd48a1d6e51cc193153d625e8f0514c7f',
            '0xdf1a1b60f2d438842916c0adc43748768353ec25',
            '0x00555513acf282b42882420e5e5ba87b44d8fa6e',
            '0x54a4a1167b004b004520c605e3f01906f683413d',
            '0x881d40237659c251811cec9c364ef91dc08d300c',
            '0xf92c1ad75005e6436b4ee84e88cb23ed8a290988',
            '0x1bd435f3c054b6e901b7b108a0ab7617c808677b',
            '0x9509665d015bfe3c77aa5ad6ca20c8afa1d98989',
            '0xf90e98f3d8dce44632e5020abf2e122e0f99dfab',
            '0xdef171fe48cf0115b1d80b88dc8eab59176fee57',
            '0x7a250d5630b4cf539739df2c5dacb4c659f2488d',
            '0xe592427a0aece92de3edee1f18e0157c05861564',
            '0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45',
            '0x619b188b3f02605e289771e0001f9c313b8436a0',
            '0x81c46feca27b31f3adc2b91ee4be9717d1cd3dd7',
            '0x31efc4aeaa7c39e54a33fdc3c46ee2bd70ae0a09',
            '0x775ee938186fddc13bd7c89d24820e1b0758f91d'
]

dex_list = ','.join(["('"+address.lower()+"')" for address in dex_address])

#print(dex_list)

chain_list = ['ethereum', 'polygon', 'goerli', 'scroll', 'arbitrum', 'canto']

def timestamp_to_readable(ts, timezone="Etc/GMT", format="%Y-%m-%dT%H:%M:%SZ"):
    # https://en.wikipedia.org/wiki/List_of_tz_database_time_zones Asia/Ho_Chi_Minh, Asia/Singapore, America/Los_Angeles
    tz = pytz.timezone(timezone)
    return datetime.fromtimestamp(ts, tz=tz).strftime(format)

def readable_to_timestamp(str, format="%Y-%m-%dT%H:%M:%SZ"):
    tz = pytz.timezone('Etc/GMT')
    dt = datetime.strptime(str, format)
    dt = tz.localize(dt)
    return dt.timestamp()

def get_dex_volumn_by_time(chain_id, contract_address, time_from, time_to, dex_list):
    # print(time_from)
    # print(time_to)
    url = "https://api.transpose.io/sql"
    sql_query = """
    with
    dex_address(wallet_address) as (
        values{{list_dex}}
    )
    select
        date_trunc('hour', timestamp) as time,
        transaction_hash, 
        from_address as "from",
        to_address as "to", 
        quantity as value
        from {{chain_id}}.token_transfers
        where lower(contract_address) = lower('{{contract_address}}')
        AND timestamp >= to_timestamp({{time_from}}) and timestamp < to_timestamp({{time_to}})
        and (lower(from_address) in (select lower(wallet_address) from dex_address) 
        or lower(to_address) in (select lower(wallet_address) from dex_address))
    order by timestamp ASC
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
                'list_dex' : dex_list,
                'chain_id' : chain_id,
                'contract_address': contract_address,
                'time_from' : time_from,
                'time_to' : time_to
            }
        },
    )
    response_json = json.loads(response.text)
    
    # response_json is the result returned from the API as a dict
    data = response_json["results"]
    
    #print(data[0])
                
    #sum volumn based on time
    sum_dict = {}
    for transaction in data:
        key = (transaction['time'])
        if key in sum_dict:
            sum_dict[key] += transaction['value']
        else:
            sum_dict[key] = transaction['value']
            
    dex_volumn = []
    
    #Create a new array to save result from dict
    for key, value in sum_dict.items():
        new = {'time': key, 'volumn': value}
        dex_volumn.append(new)
    
    return dex_volumn

def get_dex_volumn(chain_id, contract_address, time_from, cex_list):
    if chain_id not in chain_list:
        print(f"Error: {chain_id} not in supported chain list")
        print("Supported chain list:", end = "")
        print(chain_list)
        return
    now = int(time.time())
    if (now - time_from < 24*60*60):
        data = get_dex_volumn_by_time(chain_id, contract_address, time_from, now, cex_list)
    else:
        time_from = (time_from // 86400) * 86400
        end_time = time_from
        data = []
        while end_time <= now:
            time_to = end_time + 24*60*60
            data = data + get_dex_volumn_by_time(chain_id, contract_address, end_time, time_to, cex_list)
            end_time = end_time + 24*60*60
    
    return data


#support chains: ethereum, polygon, goerli, scroll, arbitrum, canto

#test
# 2 day - 21/4
#data_dex = get_dex_volumn('ethereum','0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48', 1681862400, dex_list) 
# 7 day - 14/4
#data_dex = get_dex_volumn('ethereum','0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48', 1681430420, dex_list)
# 1 day - 20/4
data_dex = get_dex_volumn('ethereum','0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48', 1681948900, dex_list)
#print(data_cex[0])

for item in data_dex:
   print(item)
