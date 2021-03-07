from data_apis import data_apis

def get_data(name, **kwargs):
    import requests
    
    url = data_apis.get(name, {}).get('url', '')
    headers = data_apis.get(name, {}).get('headers', {})
    if not url: return None
    response = requests.request("GET", url.format(**kwargs), headers=headers)
    return response.text
