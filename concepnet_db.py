import pandas as pd
import requests
import datetime
import time
import json
import os
from tqdm import tqdm

from kbutil.dbutil import db_connector



if __name__ == '__main__':

    print("=== Concepnet DB Insert (START) =======================================")

    # DB 연결
    conn = db_connector("real")
    cur = conn.cursor()

    dict_data = dict()

    relation_list = ['RelatedTo', 'AtLocation', 'HasProperty', 'IsA', 'CapableOf', 'UsedFor',
                     'DerivedFrom', 'Desires', 'HasA', 'ReceivesAction', 'PartOf', 'CreatedBy']
                     
    top_path = './data/conceptnet_re'
    big_class_list = os.listdir(top_path)

    for big_one in tqdm(big_class_list):

        big_path = top_path + '/' + big_one
        big_class = big_one

        small_class_list = os.listdir(big_path)

        for small_one in tqdm(small_class_list):
            
            small_path = big_path + '/' + small_one
            small_class = small_one

            json_file_list = os.listdir(small_path)

            for one_file in json_file_list:
                
                file_path = small_path + '/' + one_file
                with open(file_path, 'r', encoding='UTF-8') as f:
                    json_data = json.load(f)
                    dict_data = json.dumps(json_data)

                full_file_name = file_path.rsplit('/', 1)[1]
                file_name = full_file_name.rsplit('.', 1)[0]

                split_list = file_name.split('!!!')

                ko_keyword = ''
                en_keyword = ''

                if len(split_list) == 2:
                    ko_keyword = file_name.split('!!!', 1)[0]
                    en_keyword = file_name.split('!!!', 1)[1]
                elif len(split_list) == 3:
                    ko_keyword = file_name.split('!!!', 2)[1]
                    en_keyword = file_name.split('!!!', 1)[2]
                else:
                    print('----------------------------------------')
                    print(one_file)
                    print('----------------------------------------')
                    continue

                en_compare = en_keyword.replace('_', ' ')
                dict_data = json.loads(dict_data)

                select_concepnet_sql = """ SELECT id, collect_target, big_class, small_class, search_word, e1_label, e2_label, r, visual_concept
                                             FROM knowlegebase_db
                                            WHERE search_word = ?
                                              AND collect_target = ?
                                       """
                select_concepnet_values = (ko_keyword, 'concepnet')
                cur.execute(select_concepnet_sql, select_concepnet_values)
                select_result = cur.fetchall()

                if len(select_result) == 0:
                    for one_dict in dict_data:

                        rel = one_dict['rel']
                        end = one_dict['end']
                        start = one_dict['start']

                        rel_id = ''
                        rel_label = ''

                        end_id = ''
                        end_label = ''
                        end_language = ''
                        
                        start_id = ''
                        start_label = ''
                        start_language = ''

                        # 데이터 예외 처리
                        try:
                            rel_id = rel['@id']
                        except:
                            rel_id = ''        

                        try:
                            rel_label = rel['label']
                        except:
                            rel_label = ''

                        try:
                            end_id = end['@id']
                        except:
                            end_id = ''

                        try:
                            end_label = end['label'].lower()
                            end_label = end_label.replace('-', ' ')
                            end_label = end_label.replace('_', ' ')            
                        except:
                            end_label = ''
                        
                        try:
                            end_language = end['language']
                        except:
                            end_language = ''

                        try:
                            start_id = start['@id']
                        except:
                            start_id = ''

                        try:
                            start_label = start['label'].lower()
                            start_label = start_label.replace('-', ' ')
                            start_label = start_label.replace('_', ' ')
                        except:
                            start_label = ''

                        try:
                            start_language = start['language']
                        except:
                            start_language = ''

                        # 2개 모두 en (영어) 일 경우에만
                        if end_language == 'en' and start_language == 'en':
                            for one_relation in relation_list:
                                if rel_label == one_relation:
                                    e1_label = start_label
                                    e2_label = end_label

                                    e1 = start_id
                                    e2 = end_id
                                    
                                    surface_ko = []

                                    # print('------------------------------------------------------')
                                    # print('%s : %s > %s' % (rel_label, start_label, end_label))
                                    # print('%s : %s > %s' % (rel_label, e1_label, e2_label))
                                    # print('------------------------------------------------------')

                                    # 데이터 INSERT
                                    surface_ko.append(e1_label)
                                    surface_ko.append(rel_label)
                                    surface_ko.append(e2_label)

                                    ts_time = time.time()
                                    timestamp_date = datetime.datetime.fromtimestamp(ts_time).strftime('%Y-%m-%d %H:%M:%S')
                                    insert_concepnet_sql =  """
                                                        INSERT INTO knowlegebase_db
                                                        (collect_type, collect_target, big_class, small_class, search_word, word_en, e1_label, e2_label, surface_ko, r, e1, e2, use_yn, reg_date, visual_concept)
                                                        VALUES('간접', 'concepnet', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'N', ?, 'Object')
                                                        """
                                    insert_concepnet_values = (big_class, small_class, ko_keyword, en_keyword, e1_label, e2_label, surface_ko, rel_id, e1, e2, timestamp_date)
                                    cur.execute(insert_concepnet_sql, insert_concepnet_values)
                                    conn.commit()

                                    time.sleep(0.001)

    conn.close()
    print("=== Concepnet DB Insert (SUCCESS) =======================================")
    print("=== ============================= =======================================")
