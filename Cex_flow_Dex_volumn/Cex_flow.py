import requests
import json
from datetime import datetime
import pytz
import time


# Do chưa có code api lấy cex_address nên e để mặc định là đọc file json của cex_address
# Đọc file JSON
with open('D:\Ambros Demo\Blockchain\Cex_address.json') as f:
    data = json.load(f)

# Tạo list các địa chỉ của chain Ethereum ở dạng lowercase
ethereum_addresses = []
for item in data:
    if item['blockchain'] == 'ethereum':
        ethereum_addresses.append(item['address'].lower())

# Format lại list thành dạng parameter cho query: ('address_1'),('address2'),....,('address_last)        
cex_list = ','.join(["('"+address+"')" for address in ethereum_addresses])

chain_list = ['ethereum', 'polygon', 'goerli', 'scroll', 'arbitrum', 'canto']

def format_number(num):
    if num >= 10**9:
        return f"{num / 10**9:.2f}b"
    elif num >= 10**6:
        return f"{num / 10**6:.2f}m"
    elif num >= 10**3:
        return f"{num / 10**3:.2f}k"
    else:
        return str(num)
    
def timestamp_to_readable(ts, timezone="Etc/GMT", format="%Y-%m-%dT%H:%M:%SZ"):
    # https://en.wikipedia.org/wiki/List_of_tz_database_time_zones Asia/Ho_Chi_Minh, Asia/Singapore, America/Los_Angeles
    tz = pytz.timezone(timezone)
    return datetime.fromtimestamp(ts, tz=tz).strftime(format)

def readable_to_timestamp(str, format="%Y-%m-%dT%H:%M:%SZ"):
    tz = pytz.timezone('Etc/GMT')
    dt = datetime.strptime(str, format)
    dt = tz.localize(dt)
    return dt.timestamp()
    
def get_cex_flow_by_time(chain_id, contract_address, time_from, time_to, cex_list):
    # print(time_from)
    # print(time_to)
    url = "https://api.transpose.io/sql"
    sql_query = """
    with
    cex_address(wallet_address) as (
        values{{list_cex}}
    )
    select
        date_trunc('hour', timestamp) as time,
        transaction_hash, 
        from_address as "from",
        to_address as "to", 
        quantity as value,
        case 
            when lower(from_address) in (select lower(wallet_address) from cex_address) then 'OUT'
            else 'IN'
        end as situation
        from {{chain_id}}.token_transfers
        where lower(contract_address) = lower('{{contract_address}}')
        AND timestamp >= to_timestamp({{time_from}}) and timestamp < to_timestamp({{time_to}})
        and (lower(from_address) in (select lower(wallet_address) from cex_address) 
        or lower(to_address) in (select lower(wallet_address) from cex_address))
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
                'list_cex' : cex_list,
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
    
    # for item in data:
    #     if item['time'] == '2023-04-19T23:00:00Z':
    #         print(item)
            
    #sum in/out amount based on time and situation
    time_dict = {}
    for transaction in data:
        key = transaction['time']
        if key in time_dict:
            if transaction['situation'] == 'IN':
                time_dict[key]['in_amount'] += transaction['value']
            else:
                time_dict[key]['out_amount'] += transaction['value']
        else:
            if transaction['situation'] == 'OUT':
                time_dict[key] = {'in_amount': transaction['value'], 'out_amount': 0}
            else:
                time_dict[key] = {'in_amount': 0, 'out_amount': transaction['value']}

    #Create a new array with the combined elements and new columns 'in_amount' and 'out_amount'
    cex_flow = []
    for key, value in time_dict.items():
        new = {'time': key, 'in_amount': value['in_amount'], 'out_amount': value['out_amount']}
        cex_flow.append(new)
    
    return cex_flow

def get_cex_flow(chain_id, contract_address, time_from, cex_list):
    # check chain_id có thuộc chain được hỗ trợ
    if chain_id not in chain_list:
        print(f"Error: {chain_id} not in supported chain list")
        print("Supported chain list:", end = "")
        print(chain_list)
        return
    
    # lấy thời gian hiện tại
    now = int(time.time())
    
    # so sánh thời gian tính từ time_from đến hiện tại
    # nếu bé hơn 1 ngày thì chạy 1 lần query
    
    if (now - time_from < 24*60*60):
        data = get_cex_flow_by_time(chain_id, contract_address, time_from, now, cex_list)
    # nếu lớn hơn 1 ngày thì sẽ tách ra các query theo từng ngày
    else:
        # set lại time_from về đầu ngày, ví dụ nếu mình nhập timestamp tương ứng 21/04/2023 11:00:00 thì nó sẽ set về 00:00:00
        time_from = (time_from // 86400) * 86400
        end_time = time_from
        data = []
        while end_time <= now:
            time_to = end_time + 24*60*60
            # có thể check data đã tồn tại trong db dựa theo end_time và time_to
            data = data + get_cex_flow_by_time(chain_id, contract_address, end_time, time_to, cex_list)
            end_time = end_time + 24*60*60
    
    return data
    
#support chains: ethereum, polygon, goerli, scroll, arbitrum, canto

#test
# 2 day - 21/4
#data_cex = get_cex_flow('ethereum','0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48', 1681862400, cex_list) 
# 7 day - 14/4
#data_cex = get_cex_flow('ethereum','0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48', 1681430420, cex_list)
# 1 day - 20/4
data_cex = get_cex_flow('ethereum','0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48', 1681948900 + 86400, cex_list)
#print(data_cex[0])

for item in data_cex:
  print(item)
  
