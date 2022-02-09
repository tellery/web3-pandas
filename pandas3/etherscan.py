import requests
import logging

logger = logging.getLogger()


def get_contract_abi(contract_address: str):
    r = requests.get('https://api.etherscan.io/api', params={
        'module': 'contract',
        'action': 'getabi',
        'address': contract_address
    })
    r.raise_for_status()
    data = r.json()
    if data['status'] != '1':
        logger.error("Failed to get contract abi: %s:%s", data['status'], data['message'])
        raise Exception("Get ABI from Etherscan failed")
    return data['result']
