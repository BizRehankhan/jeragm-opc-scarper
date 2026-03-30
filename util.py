
import pandas as pd
from constant import ENERGY_TYPES


def to_tabular_format(data, energy_type):
    wdnodes = data['returndata']['wdnodes']
    zb_node = next((w['nodes'][0] for w in wdnodes if w['wdcode'] == 'zb'), {})
    reg_nodes = {r['code']: r for r in next(
        (w['nodes'] for w in wdnodes if w['wdcode'] == 'reg'), [])}

    rows = []
    for node in data['returndata']['datanodes']:
        wds_dict = {w['wdcode']: w['valuecode'] for w in node['wds']}
        reg_info = reg_nodes.get(wds_dict.get('reg'), {})
        sj = wds_dict.get('sj', '')

        rows.append({
            'energy_type': energy_type,
            'indicator_code': wds_dict.get('zb'),
            'indicator_name': zb_node.get('name', ''),
            'indicator_unit': zb_node.get('unit', ''),
            'region_code': wds_dict.get('reg'),
            'region_name': reg_info.get('name', ''),
            'region_cname': reg_info.get('cname', ''),
            'year': int(sj[:4]) if sj else None,
            'month': int(sj[4:6]) if sj else None,
            'period': sj,
            'data': node['data']['data'],
            'hasdata': node['data']['hasdata'],
            'strdata': node['data']['strdata'],
            'dotcount': node['data']['dotcount']
        })

    return pd.DataFrame(rows)


def get_all_energy_type(name=None):
    if name:
        return next(
            (item for item in ENERGY_TYPES if item["name"] == name.lower(
            ) or item["_name"] == name),
            None
        )
    return ENERGY_TYPES


