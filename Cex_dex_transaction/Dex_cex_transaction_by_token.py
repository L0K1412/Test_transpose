import requests
import json
from datetime import datetime
import pytz


#create list of dex address
dex_list = ['0xe66b31678d6c16e9ebf358268a790b763c133750',
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

dex_list = [addr.lower() for addr in dex_list]

#create list of cex address
cex_list = ['0x3f5CE5FBFe3E9af3971dD833D26bA9b5C936f0bE', 
        '0xd551234ae421e3bcba99a0da6d736074f22192ff', 
        '0x564286362092d8e7936f0549571a803b203aaced', 
        '0x0681d8db095565fe8a346fa0277bffde9c0edbbf', 
        '0xfe9e8709d3215310075d67e3ed32a380ccf451c8', 
        '0x4e9ce36e442e55ecd9025b9a6e0d88485d628a67', 
        '0xbe0eb53f46cd790cd13851d5eff43d12404d33e8', 
        '0xf977814e90da44bfa03b6295a0616a897441acec', 
        '0x001866ae5b3de6caa5a51543fd9fb64f524f5478', 
        '0x85b931a32a0725be14285b66f1a22178c672d69b', 
        '0x708396f17127c42383E3b9014072679b2F60B82f', 
        '0xe0f0cfde7ee664943906f17f7f14342e76a5cec7', 
        '0x8f22f2063d253846b53609231ed80fa571bc0c8f', 
        '0x28c6c06298d514db089934071355e5743bf21d60', 
        '0x21a31ee1afc51d94c2efccaa2092ad1028285549', 
        '0xdfd5293d8e347dfe59e90efd55b2956a1343963d', 
        '0x56eddb7aa87536c09ccc2793473599fd21a8b17f', 
        '0x9696f59e4d72e237be84ffd425dcad154bf96976', 
        '0x4d9ff50ef4da947364bb9650892b2554e7be5e2b', 
        '0x4976a4a02f38326660d17bf34b431dc6e2eb2327', 
        '0xd88b55467f58af508dbfdc597e8ebd2ad2de49b3', 
        '0x7dfe9a368b6cf0c0309b763bb8d16da326e8f46e', 
        '0x345d8e3a1f62ee6b1d483890976fd66168e390f2', 
        '0xc3c8e0a39769e2308869f7461364ca48155d1d9e', 
        '0x2e581a5ae722207aa59acd3939771e7c7052dd3d', 
        '0x44592b81c05b4c35efb8424eb9d62538b949ebbf', 
        '0xd5fd1bc99d5801278345e9d29be2225d06c26e93', 
        '0x06a0048079ec6571cd1b537418869cde6191d42d', 
        '0x892e9e24aea3f27f4c6e9360e312cce93cc98ebe', 
        '0x00799bbc833d5b168f0410312d2a8fd9e0e3079c', 
        '0x141fef8cd8397a390afe94846c8bd6f4ab981c48', 
        '0x50d669f43b484166680ecc3670e4766cdb0945ce', 
        '0x2f7e209e0f5f645c7612d7610193fe268f118b28', 
        '0x61189da79177950a7272c88c6058b96d4bcd6be2', 
        '0x34ea4138580435b5a521e460035edb19df1938c1', 
        '0xf60c2ea62edbfe808163751dd0d8693dcb30019c', 
        '0xeea81c4416d71cef071224611359f6f99a4c4294', 
        '0xfb8131c260749c7835a08ccbdb64728de432858e', 
        '0x9c67e141c0472115aa1b98bd0088418be68fd249', 
        '0x59a5208b32e627891c389ebafc644145224006e8', 
        '0xa12431d0b9db640034b0cdfceef9cce161e62be4', 
        '0x2b5634c42055806a59e9107ed44d43c426e58258', 
        '0x689c56aef474df92d44a1b70850f808488f9769c', 
        '0xa1d8d972560c2f8144af871db508f0b0b10a3fbf', 
        '0x4ad64983349c49defe8d7a4686202d24b25d0ce8', 
        '0x1692e170361cefd1eb7240ec13d048fd9af6d667', 
        '0xd6216fc19db775df9774a6e33526131da7d19a2c', 
        '0xe59cd29be3be4461d79c0881d238cbe87d64595a', 
        '0x899b5d52671830f567bf43a14684eb14e1f945fe', 
        '0xf16e9b0d03470827a95cdfd0cb8a8a3b46969b91', 
        '0xcaD621da75a66c7A8f4FF86D30A2bF981Bfc8FdD', 
        '0x2faf487a4414fe77e2327f0bf4ae2a264a776ad2', 
        '0xc098b2a3aa256d2140208c3de6543aaef5cd3a94', 
        '0x6cc5f688a315f3dc28a7781717a9a798a59fda7b', 
        '0x236f9f97e0e62388479bf9e5ba4889e46b0273c3', 
        '0xa7efae728d2936e78bda97dc267687568dd593f3', 
        '0x2c8fbb630289363ac80705a1a61273f76fd5a161', 
        '0x59fae149a8f8ec74d5bc038f8b76d25b136b9573', 
        '0x98ec059dc3adfbdd63429454aeb0c990fba4a128', 
        '0x5041ed759dd4afc3a72b8192c143f72f4724081a', 
        '0x71660c4005ba85c37ccec55d0c4493e66fe775d3', 
        '0x503828976d22510aad0201ac7ec88293211d23da', 
        '0xddfabcdc4d8ffc6d5beaf154f18b778f892a0740', 
        '0x3cd751e6b0078be393132286c442345e5dc49699', 
        '0xb5d85cbf7cb3ee0d56b3bb207d5fc4b82f43f511', 
        '0xeb2629a2734e272bcc07bda959863f316f4bd4cf', 
        '0xab5c66752a9e8167967685f1450532fb96d5d24f', 
        '0x6748f50f686bfbca6fe8ad62b22228b87f31ff2b', 
        '0xfdb16996831753d5331ff813c29a93c76834a0ad', 
        '0xeee28d484628d41a82d01e21d12e2e78d69920da', 
        '0x5c985e89dde482efe97ea9f1950ad149eb73829b', 
        '0xdc76cd25977e0a5ae17155770273ad58648900d3', 
        '0xadb2b42f6bd96f5c65920b9ac88619dce4166f94', 
        '0xa8660c8ffd6d578f657b72c0c811284aef0b735e', 
        '0x1062a747393198f70f71ec65a582423dba7e5ab3', 
        '0xe93381fb4c4f14bda253907b18fad305d799241a', 
        '0xfa4b5be3f2f84f56703c42eb22142744e95a2c58', 
        '0x46705dfff24256421a05d056c29e81bdc09723b8', 
        '0x32598293906b5b17c27d657db3ad2c9b3f3e4265', 
        '0x5861b8446a2f6e19a067874c133f04c578928727', 
        '0x926fc576b7facf6ae2d08ee2d4734c134a743988', 
        '0xeec606a66edb6f497662ea31b5eb1610da87ab5f', 
        '0x7ef35bb398e0416b81b019fea395219b65c52164', 
        '0x229b5c097f9b35009ca1321ad2034d4b3d5070f6', 
        '0xd8a83b72377476d0a66683cde20a8aad0b628713', 
        '0x90e9ddd9d8d5ae4e3763d0cf856c97594dea7325', 
        '0x18916e1a2933cb349145a280473a5de8eb6630cb', 
        '0x6f48a3e70f0251d1e83a989e62aaa2281a6d5380', 
        '0xf056f435ba0cc4fcd2f1b17e3766549ffc404b94', 
        '0x137ad9c4777e1d36e4b605e745e8f37b2b62e9c5', 
        '0x5401dbf7da53e1c9dbf484e3d69505815f2f5e6e', 
        '0x034f854b44d28e26386c1bc37ff9b20c6380b00d', 
        '0x0577a79cfc63bbc0df38833ff4c4a3bf2095b404', 
        '0x0c6c34cdd915845376fb5407e0895196c9dd4eec', 
        '0x794d28ac31bcb136294761a556b68d2634094153', 
        '0xfd54078badd5653571726c3370afb127351a6f26', 
        '0xb4cd0386d2db86f30c1a11c2b8c4f4185c1dade9', 
        '0x4d77a1144dc74f26838b69391a6d3b1e403d0990', 
        '0x28ffe35688ffffd0659aee2e34778b0ae4e193ad', 
        '0xcac725bef4f114f728cbcfd744a731c2a463c3fc', 
        '0x73f8fc2e74302eb2efda125a326655acf0dc2d1b', 
        '0x0a98fb70939162725ae66e626fe4b52cff62c2e5', 
        '0xf66852bc122fd40bfecc63cd48217e88bda12109', 
        '0x49517ca7b7a50f592886d4c74175f4c07d460a70', 
        '0x58c2cb4a6bee98c309215d0d2a38d7f8aa71211c', 
        '0x2910543af39aba0cd09dbb2d50200b3e800a63d2', 
        '0x0a869d79a7052c7f1b55a8ebabbea3420f0d1e13', 
        '0xe853c56864a2ebe4576a807d26fdc4a0ada51919', 
        '0x267be1c1d684f78cb4f6a176c4911b741e4ffdc0', 
        '0xfa52274dd61e1643d2205169732f29114bc240b3', 
        '0x53d284357ec70ce289d6d64134dfac8e511c8a3d', 
        '0x89e51fa8ca5d66cd220baed62ed01e8951aa7c40', 
        '0xc6bed363b30df7f35b601a5547fe56cd31ec63da', 
        '0x29728d0efd284d85187362faa2d4d76c2cfc2612', 
        '0xae2d4617c862309a3d75a0ffb358c7a5009c673f', 
        '0x43984d578803891dfa9706bdeee6078d80cfc79e', 
        '0x66c57bf505a85a74609d2c83e94aabb26d691e1f', 
        '0xda9dfa130df4de4673b89022ee50ff26f6ea73cf', 
        '0x1151314c646ce4e0efd76d1af4760ae66a9fe30f', 
        '0x742d35cc6634c0532925a3b844bc454e4438f44e', 
        '0x876eabf441b2ee5b5b0554fd502a8e0600950cfa', 
        '0xdcd0272462140d0a3ced6c4bf970c7641f08cd2c', 
        '0x4fdd5eb2fb260149a3903859043e962ab89d8ed4', 
        '0x1b29dd8ff0eb3240238bf97cafd6edea05d5ba82', 
        '0x30a2ebf10f34c6c4874b0bdd5740690fd2f3b70c', 
        '0x3f7e77b627676763997344a1ad71acb765fc8ac5', 
        '0x59448fe20378357f206880c58068f095ae63d5a5', 
        '0x36a85757645e8e8aec062a1dee289c7d615901ca', 
        '0xc56fefd1028b0534bfadcdb580d3519b5586246e', 
        '0x0b73f67a49273fc4b9a65dbd25d7d0918e734e63', 
        '0x482f02e8bc15b5eabc52c6497b425b3ca3c821e8', 
        '0xf73C3c65bde10BF26c2E1763104e609A41702EFE', 
        '0x00bdb5699745f5b860228c8f939abf1b9ae374ed', 
        '0x1522900b6dafac587d499a862861c0869be6e428', 
        '0x9a9bed3eb03e386d66f8a29dc67dc29bbb1ccb72', 
        '0x059799f2261d37b829c2850cee67b5b975432271', 
        '0x4c766def136f59f6494f0969b1355882080cf8e0', 
        '0xc5b611f502a0dcf6c3188fd494061ae29b2baa4f', 
        '0x111cFf45948819988857BBF1966A0399e0D1141e', 
        '0xd24400ae8bfebb18ca49be86258a3c749cf46853', 
        '0x6fc82a5fe25a5cdb58bc74600a40a69c065263f8', 
        '0x61edcdf5bb737adffe5043706e7c5bb1f1a56eea', 
        '0x5f65f7b609678448494de4c87521cdf6cef1e932', 
        '0xb302bfe9c246c6e150af70b1caaa5e3df60dac05', 
        '0x8d6f396d210d385033b348bcae9e4f9ea4e045bd', 
        '0xd69b0089d9ca950640f5dc9931a41a5965f00303', 
        '0xDB044B8298E04D442FdBE5ce01B8cc8F77130e33', 
        '0x3d1D8A1d418220fd53C18744d44c182C46f47468', 
        '0x59E0cDA5922eFbA00a57794faF09BF6252d64126', 
        '0x1579B5f6582C7a04f5fFEec683C13008C4b0A520', 
        '0xb9ee1e551f538A464E8F8C41E9904498505B49b0', 
        '0x33Ddd548FE3a082d753E5fE721a26E1Ab43e3598', 
        '0x32be343b94f860124dc4fee278fdcbd38c102d88', 
        '0x209c4784ab1e8183cf58ca33cb740efbf3fc18ef', 
        '0xb794f5ea0ba39494ce839613fffba74279579268', 
        '0xa910f92acdaf488fa6ef02174fb86208ad7722ba', 
        '0xfbb1b73c4f0bda4f67dca266ce6ef42f520fbb98', 
        '0xe94b04a0fed112f3664e45adb2b8915693dd5ff3', 
        '0x66f820a414680b5bcda5eeca5dea238543f42054', 
        '0xd1669ac6044269b59fa12c5822439f609ca54f41', 
        '0x8d1f2ebfaccf1136db76fdd1b86f1dede2d23852', 
        '0xd2c82f2e5fa236e114a81173e375a73664610998', 
        '0xaf1931c20ee0c11bea17a41bfbbad299b2763bc0', 
        '0x416299aade6443e6f6e8ab67126e65a7f606eef5', 
        '0x6262998ced04146fa42253a5c0af90ca02dfd2a3', 
        '0x46340b20830761efd32832a74d7169b29feb9758', 
        '0x0d0707963952f2fba59dd06f2b425ace40b492fe', 
        '0x7793cd85c11a924478d358d49b05b37e91b5810f', 
        '0x1c4b70a3968436b9a0a9cf5205c787eb81bb558c', 
        '0x75e89d5979e4f6fba9f97c104c2f0afb3f1dcb88', 
        '0x3cc936b795a188f0e246cbb2d74c5bd190aecf18', 
        '0x39f6a6c85d39d5abad8a398310c52e7c374f2ba3'
]

cex_list = [addr.lower() for addr in cex_list]

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
    return datetime.strptime(str, format).timestamp()
    
def get_cex_transaction(chain_id, contract_id, time_from):
    if chain_id not in chain_list:
        print(f"Error: {chain_id} not in supported chain list")
        print("Supported chain list:", end = "")
        print(chain_list)
        return
    
    time_from = timestamp_to_readable(time_from)
    #print(time_from)
    
    url = "https://api.transpose.io/sql"
    sql_query = """
    with token_info as (
        SELECT contract_address, symbol, pow(10,decimals) as deci from ethereum.tokens 
            where contract_address = '{{contract_address}}'
            limit 1)
    SELECT
        tt.timestamp as time, 
        tt.transaction_hash, 
        tt.from_address as "from",
        tt.to_address as "to", 
        tt.quantity/t.deci as value
    FROM
        {{chain_id}}.token_transfers tt join token_info t
        on tt.contract_address = t.contract_address
    where timestamp >= '{{time_from}}'
    ORDER BY
        tt.timestamp DESC;"""
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
                'contract_address': contract_id,
                'time_from' : time_from
            }
        },
    )
    response_json = json.loads(response.text)
    
    # response_json is the result returned from the API as a dict
    data = response_json["results"]
    
    #print(data[0])
    
    cex_transaction = []
    
    #filter transactions of cex and set situation
    for transaction in data:
        if transaction['from'] is not None and transaction['to'] is not None:
            if transaction['from'].lower() in cex_list or transaction['to'].lower() in cex_list:
                cex_transaction.append(transaction)
    
    for transaction in cex_transaction:
        if transaction['from'].lower() in cex_list:
            transaction['situation'] = 'out'
        if transaction['to'].lower() in cex_list:
            transaction['situation'] = 'in'
            
    #print(cex_transaction[0])
    
    return cex_transaction

def get_dex_transaction(chain_id, contract_id, time_from):
    if chain_id not in chain_list:
        print(f"Error: {chain_id} not in supported chain list")
        print("Supported chain list:", end = "")
        print(chain_list)
        return
    
    time_from = timestamp_to_readable(time_from)
    
    url = "https://api.transpose.io/sql"
    sql_query = """
    with token_info as (
        SELECT contract_address, symbol, pow(10,decimals) as deci from ethereum.tokens 
            where contract_address = '{{contract_address}}'
            limit 1)
    SELECT
        tt.timestamp as time, 
        tt.transaction_hash, 
        tt.from_address as "from",
        tt.to_address as "to", 
        tt.quantity/t.deci as value
    FROM
        {{chain_id}}.token_transfers tt join token_info t
        on tt.contract_address = t.contract_address
    where timestamp >= '{{time_from}}'
    ORDER BY
        tt.timestamp DESC;"""
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
                'contract_address': contract_id,
                'time_from' : time_from
            }
        },
    )
    response_json = json.loads(response.text)
    
    # response_json is the result returned from the API as a dict
    data = response_json["results"]
    
    #print(data[0])
    
    dex_transaction = []
    
    #filter transactions of dex
    for transaction in data:
        if transaction['from'] is not None and transaction['to'] is not None:
            if transaction['from'].lower() in dex_list or transaction['to'].lower() in dex_list:
                dex_transaction.append(transaction)
                
    #print(dex_transaction[0])
                
    for transaction in dex_transaction:
        if transaction['from'].lower() in dex_list:
            transaction['situation'] = 'out'
        if transaction['to'].lower() in dex_list:
            transaction['situation'] = 'in'
    
    return dex_transaction
    
#support chains: ethereum, polygon, goerli, scroll, arbitrum, canto

#SHIB on ethereum from 2023-04-01
data_cex = get_cex_transaction('ethereum','0x95aD61b0a150d79219dCF64E1E6Cc01f0B64C4cE', 1680307200)

data_dex = get_dex_transaction('ethereum','0x95aD61b0a150d79219dCF64E1E6Cc01f0B64C4cE', 1680307200)

#print(data_cex[0])

print('Cex transaction of token SHIB on etherum from 2023-03-31:')

for item in data_cex:
   print(item)
   
print('Dex transaction of token SHIB on etherum from 2023-03-31:')

for item in data_dex:
   print(item)

