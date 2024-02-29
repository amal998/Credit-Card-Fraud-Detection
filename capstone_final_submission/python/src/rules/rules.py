#Importing all the libraries
from db.dao import HBaseDao
from db.geo_map import GEO_Map
from datetime import datetime
import uuid

lookup_table = 'lookup_data_hive'
card_trans_table = 'card_transactions_hive'
speed_threshold = 0.25



class Rules():

    def verify_ucl_data(card_id, amount):
        try:
            hbasedao = HBaseDao.get_instance()
            card_row = hbasedao.get_data(key=str(card_id), table=lookup_table)
            card_ucl = (card_row[b'lookup_card_family:ucl']).decode("utf-8")

            if amount < float(card_ucl):
                return True
            else:
                return False
        except Exception as e:
            raise Exception(e)


    def verify_credit_score_data(card_id):

        try:
            hbasedao = HBaseDao.get_instance()

            card_row = hbasedao.get_data(key=str(card_id), table=lookup_table)
            card_score = (card_row[b'lookup_card_family:score']).decode("utf-8")

            if int(card_score) > 200:
                return True
            else:
                return False
        except Exception as e:
            raise Exception(e)


    def calculate_speed(dist, transaction_dt1, transaction_dt2):
        transaction_dt1 = datetime.strptime(transaction_dt1, '%d-%m-%Y %H:%M:%S')
        transaction_dt2 = datetime.strptime(transaction_dt2, '%d-%m-%Y %H:%M:%S')

        elapsed_time = transaction_dt1 - transaction_dt2
        elapsed_time = elapsed_time.total_seconds()

        try:
            return dist / elapsed_time
        except ZeroDivisionError:
            return 299792.458
        

    def verify_postcode_data(card_id, postcode, transaction_dt):
        try:
            hbasedao = HBaseDao.get_instance()
            geo_map = GEO_Map.get_instance()

            card_row = hbasedao.get_data(key=str(card_id), table=lookup_table)
            last_postcode = (card_row[b'lookup_transaction_family:postcode']).decode("utf-8")
            last_transaction_dt = (card_row[b'lookup_transaction_family:transaction_dt']).decode("utf-8")

            current_lat = geo_map.get_lat(str(postcode))
            current_lon = geo_map.get_long(str(postcode))
            previous_lat = geo_map.get_lat(last_postcode)
            previous_lon = geo_map.get_long(last_postcode)

            dist = geo_map.distance(lat1=current_lat, long1=current_lon, lat2=previous_lat, long2=previous_lon)

            speed = calculate_speed(dist, transaction_dt, last_transaction_dt)

            if speed < speed_threshold:
                return True
            else:
                return False

        except Exception as e:
            raise Exception(e)

    
    def verify_rules_status(card_id, member_id, amount, pos_id, postcode, transaction_dt):

        hbasedao = HBaseDao.get_instance()

        rule1 = verify_ucl_data(card_id, amount)
        rule2 = verify_credit_score_data(card_id)
        rule3 = verify_postcode_data(card_id, postcode, transaction_dt)

        if all([rule1, rule2, rule3]):
            status = 'GENUINE'
            hbasedao.write_data(key=str(card_id),
                            row={'lookup_transaction_family:postcode': str(postcode), 'lookup_transaction_family:transaction_dt': str(transaction_dt)},
                            table=lookup_table)
        else:
            status = 'FRAUD'

        new_id = str(uuid.uuid4()).replace('-', '')
        hbasedao.write_data(key=new_id,
                        row={'card_transactions_family:card_id': str(card_id), 'card_transactions_family:member_id': str(member_id),
                             'card_transactions_family:amount': str(amount), 'card_transactions_family:pos_id': str(pos_id),
                             'card_transactions_family:postcode': str(postcode), 'card_transactions_family:status': str(status),
                             'card_transactions_family:transaction_dt': str(transaction_dt)},
                        table=card_trans_table)
        return status






