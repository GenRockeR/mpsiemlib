import requests


def get_eps(siem_address='localhost'):
    raw_addr = f'http://{siem_address}:8013/events/counter/simple?name=storage.events_raw.in&granularity=300' \
               f'&aggregation=avg'
    norm_addr = f'http://{siem_address}:8013/events/counter/simple?name=storage.events_norm.in&granularity=300' \
                f'&aggregation=avg'
    corr_in_addr = f'http://{siem_address}:8013/events/counter/simple?name=correlator.events.in&granularity=300' \
                   f'&aggregation=avg'
    corr_out_addr = f'http://{siem_address}:8013/events/counter/simple?name=correlator.events.out&granularity=300' \
                    f'&aggregation=avg'
    parsed_string_r = round(requests.get(url=raw_addr).json().get('count')[0])
    parsed_string_n = round(requests.get(url=norm_addr).json().get('count')[0])
    parsed_string_ci = round(requests.get(url=corr_in_addr).json().get('count')[0])
    parsed_string_co = round(requests.get(url=corr_out_addr).json().get('count')[0])

    keys_list = ['raw_eps', 'norm_eps', 'corr_in', 'corr_out']
    metric_list = [parsed_string_r, parsed_string_n, parsed_string_ci, parsed_string_co]
    new_list = dict(zip(keys_list, metric_list))

    return new_list


def get_siem_tables(siem_address='localhost'):
    tables_request = f'http://{siem_address}:8013/v2/control/tables'
    req = requests.get(tables_request).json()
    return req
