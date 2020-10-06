import pandas as pd
import numpy as np
import requests, re
from db_helper import db_helper
from tqdm import tqdm

class Preprocess():
    def nospace(self, addr):
        try:
            if re.match(' ', addr):
                addr = re.sub(re.match(' ', addr).group(), '', addr, 1)
            else:
                addr = addr
            if 'nan' in addr:
                addr = re.sub('nan', '', addr)
            else:
                addr = addr

            if '  ' in addr:
                addr = re.sub('  ', ' ', addr)
            else:
                addr = addr
        except TypeError:
            pass # nan 데이터

        return addr

    def seoul_to_teuk(self, addr):
        try:
            addr = re.sub('서울 ', '서울특별시 ', addr)
        except:
            pass
        try:
            addr = re.sub('인천 ', '인천광역시 ', addr)
        except:
            pass
        # try:
        #     addr = re.sub('부산 ', '부산광역시 ', addr)
        # except:
        #     pass
        return addr

    def cleaning_for_pnu(self, addr):
        try:
            addr = re.sub(re.search('[0-9][0-9]?[0-9]?[의][0-9][0-9]?', addr).group(), re.sub('의', '-', re.search('[0-9][0-9]?[0-9]?[의][0-9][0-9]?', addr).group()), addr)
        except:
            pass
        try:
            addr = addr.replace(re.split(re.search(' [0-9][0-9]?[0-9]?[-]?[0-9]?[0-9]? ?', addr).group(), addr)[1], '')
        except:
            pass
        return str(addr).strip()

    def mk_pnu_table(self, place):
        road_pnu = db_helper.read_tables(dbname = 'spwkdw_{}'.format(place), table_name = 'building_pyojebu')[['pnu', 'na_adr_bsc']]
        jibun_pnu = db_helper.read_tables(dbname= 'landbook_{}'.format(place), table_name= 'lot_polygon')[['pnu','address']]

        road_pnu['na_adr_bsc'] = road_pnu['na_adr_bsc'].apply(nospace)
        road_pnu.columns = ['matched_pnu', 'matching_addr']
        jibun_pnu.columns = ['matched_pnu', 'matching_addr']
        jibun_pnu['matching_addr'] = jibun_pnu['matching_addr'].apply(seoul_to_teuk)
        pnu_table = pd.concat([road_pnu, jibun_pnu])
        pnu_table = pnu_table.sort_values(by='matched_pnu').drop_duplicates(subset='matching_addr')

        return pnu_table

    def match_pnu_with_addr(self, tmp, place):
        pnu_table = mk_pnu_table(place)
        tmp['matching_addr'] = tmp['final_addr'].apply(seoul_to_teuk)
        tmp['matching_addr'] = tmp['final_addr'].apply(cleaning_for_pnu)

        tmp = pd.merge(tmp, pnu_table, on = 'matching_addr', how ='left')

        return tmp

