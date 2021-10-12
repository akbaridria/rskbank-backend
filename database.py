from posixpath import split
import pymysql
from dotenv import load_dotenv
from pymysql.constants import CLIENT
import os 


load_dotenv()


class database :

    def __init__(self) :
        self.user = os.getenv('DATABASE_USER')
        self.password = os.getenv('DATABASE_PASSWORD')
        self.host = os.getenv('DATABASE_HOST') 
        self.db_name = os.getenv('DATABASE_NAME')

    def mysqlconnect(self):
        conn = pymysql.connect(
            host=self.host,
            user=self.user, 
            password=self.password,
            db=self.db_name,
            port=3306,
            cursorclass=pymysql.cursors.DictCursor,
            client_flag=CLIENT.MULTI_STATEMENTS
            )
        return conn

    def get_biggest_trade(self, database_name) :
        conn = self.mysqlconnect()
        cur = conn.cursor()
        query_sql = 'SELECT amount_usd as total, tx_hash FROM {database_name} WHERE amount_usd = (SELECT MAX(amount_usd) as total from {database_name}) LIMIT 1'
        cur.execute(query_sql.format(database_name= database_name))
        _d = cur.fetchone()
        return _d
    
    def get_total_trade(self, database_name):
        conn = self.mysqlconnect()
        cur = conn.cursor()
        query_sql = 'SELECT SUM(amount_usd) as total from {database_name}'
        cur.execute(query_sql.format(database_name = database_name))
        _d = cur.fetchone()
        return _d

    def get_total_unique_user(self, database_name):
        conn = self.mysqlconnect()
        cur = conn.cursor()
        query_sql = 'SELECT COUNT(DISTINCT(trader)) as total from {database_name}'
        cur.execute(query_sql.format(database_name = database_name))
        _d = cur.fetchone()
        return _d
    
    def get_total_unique_user_by_address(self, database_name):
        conn = self.mysqlconnect()
        cur = conn.cursor()
        query_sql = 'SELECT SUM(DISTINCT(address)) from {database_name}'
        cur.execute(query_sql.format(database_name = database_name))
        _d = cur.fetchone()
        return _d

    def get_total_transactions(self, database_name):
        conn = self.mysqlconnect()
        cur = conn.cursor()
        query_sql = 'SELECT COUNT(*) as total from {database_name}'
        cur.execute(query_sql.format(database_name = database_name))
        _d = cur.fetchone()
        return _d

    def get_top_10_trader_sov_bridge(self):
        conn = self.mysqlconnect()
        cur = conn.cursor()
        query_sql = 'SELECT SUM(from_value) as total, address as trader FROM sov_bridge GROUP BY address ORDER BY total DESC LIMIT 10'
        cur.execute(query_sql)
        _d = cur.fetchall()
        return _d
    
    def get_top_10_trader_rsk_bridge(self):
        conn = self.mysqlconnect()
        cur = conn.cursor()
        query_sql = "SELECT SUM(amount) as total, trader FROM rsk_bridge WHERE from_token IN ('rUSDT', 'rDAI', 'rUSDC', 'DOC') GROUP BY trader ORDER BY total DESC LIMIT 10"
        cur.execute(query_sql)
        _d = cur.fetchall()
        return _d
    def get_top_10_trader_moc(self):
        conn =  self.mysqlconnect()
        cur = conn.cursor()
        query_sql = "SELECT SUM(amount_usd) as total, trader from moc_protocol_mint_redeem GROUP BY trader ORDER BY total DESC LIMIT 10"
        cur.execute(query_sql)
        _d = cur.fetchall()
        return _d

    def get_top_10_trader_rif(self):
        conn =  self.mysqlconnect()
        cur = conn.cursor()
        query_sql = "SELECT SUM(amount_usd) as total, trader from rif_on_chain_mint_redeem GROUP BY trader ORDER BY total DESC LIMIT 10"
        cur.execute(query_sql)
        _d = cur.fetchall()
        return _d

    def get_volume_trade_by_date_moc(self):
        conn =  self.mysqlconnect()
        cur = conn.cursor()
        query_sql = "SELECT DATE(date) as time, SUM(amount_usd) as value from moc_protocol_mint_redeem GROUP BY time ORDER BY time"
        cur.execute(query_sql)
        _d = cur.fetchall()
        return _d

    def get_total_unique_user_by_date_moc(self):
        conn = self.mysqlconnect()
        cur = conn.cursor()
        query_sql = "SELECT DATE(date) as time, COUNT(DISTINCT(trader)) as value FROM `moc_protocol_mint_redeem` GROUP BY time"
        cur.execute(query_sql)
        _d = cur.fetchall()
        return _d
    
    def get_new_user_by_date_moc(self):
        conn = self.mysqlconnect()
        cur = conn.cursor()
        query_sql = "SELECT time, COUNT(*) as value from (select trader, min(date) as time from moc_protocol_mint_redeem group by trader) oc group by time"
        cur.execute(query_sql)
        _d = cur.fetchall()
        return _d

    def get_volume_trade_by_date_rif(self):
        conn =  self.mysqlconnect()
        cur = conn.cursor()
        query_sql = "SELECT DATE(date) as time, SUM(amount_usd) as value from rif_on_chain_mint_redeem GROUP BY time ORDER BY time"
        cur.execute(query_sql)
        _d = cur.fetchall()
        return _d

    def get_total_unique_user_by_date_rif(self):
        conn = self.mysqlconnect()
        cur = conn.cursor()
        query_sql = "SELECT DATE(date) as time, COUNT(DISTINCT(trader)) as value FROM rif_on_chain_mint_redeem GROUP BY time"
        cur.execute(query_sql)
        _d = cur.fetchall()
        return _d
    
    def get_new_user_by_date_rif(self):
        conn = self.mysqlconnect()
        cur = conn.cursor()
        query_sql = "SELECT time, COUNT(*) as value from (select trader, min(date) as time from rif_on_chain_mint_redeem group by trader) oc group by time"
        cur.execute(query_sql)
        _d = cur.fetchall()
        return _d
    
    def get_top_10_trader_by_address(self, database_name):
        conn = self.mysqlconnect()
        cur = conn.cursor()
        query_sql = "SELECT COUNT(*) as total, trader from {database_name} GROUP BY trader ORDER BY total DESC LIMIT 10"
        cur.execute(query_sql.format(database_name = database_name))
        _d = cur.fetchall()
        return _d

    def get_total_trade_volume_monthly(self, database_name):
        conn = self.mysqlconnect()
        cur = conn.cursor()
        query_sql = "SELECT DATE(date) as time, SUM(amount_usd) as value from {database_name} GROUP BY MONTH(date)"
        cur.execute(query_sql.format(database_name = database_name))
        _d = cur.fetchall()
        return _d

    def get_total_unique_user_monthly(self, database_name):
        conn = self.mysqlconnect()
        cur = conn.cursor()
        query_sql = "SELECT DATE(date) as time, COUNT(DISTINCT(trader)) as value from {database_name} GROUP BY MONTH(date)"
        cur.execute(query_sql.format(database_name = database_name))
        _d = cur.fetchall()
        return _d

    def get_total_new_user_monthly(self, database_name):
        conn = self.mysqlconnect()
        cur = conn.cursor()
        query_sql = "SELECT time, COUNT(*) as value from (select trader, min(date) as time from {database_name} group by trader) oc group by MONTH(time)"
        cur.execute(query_sql.format(database_name = database_name))
        _d = cur.fetchall()
        return _d

    def get_best_trader_rsk(self):
        conn = self.mysqlconnect()
        cur = conn.cursor()
        query_sql = "SELECT amount as total, tx_hash FROM rsk_bridge WHERE amount = (SELECT MAX(amount) as total from rsk_bridge  WHERE from_token IN ('rUSDT', 'DOC', 'rDAI', 'rUSDC')) LIMIT 1"
        cur.execute(query_sql)
        _d = cur.fetchone()
        return _d
    
    def get_total_trade_volume_rsk(self):
        conn = self.mysqlconnect()
        cur = conn.cursor()
        query_sql = "SELECT SUM(amount) as total FROM rsk_bridge WHERE from_token IN ('rUSDT', 'DOC', 'rDAI', 'rUSDC')"
        cur.execute(query_sql)
        _d = cur.fetchone()
        return _d

    def get_total_unique_user_rsk(self):
        conn = self.mysqlconnect()
        cur = conn.cursor()
        query_sql = "SELECT COUNT(DISTINCT(trader)) as total FROM rsk_bridge WHERE from_token IN ('rUSDT', 'DOC', 'rDAI', 'rUSDC')"
        cur.execute(query_sql)
        _d = cur.fetchone()
        return _d

    def get_collected_fee_rsk(self):
        conn = self.mysqlconnect()
        cur = conn.cursor()
        query_sql = "SELECT SUM(collected_fee) as total FROM rsk_bridge WHERE from_token IN ('rUSDT', 'DOC', 'rDAI', 'rUSDC')"
        cur.execute(query_sql)
        _d = cur.fetchone()
        return _d

    def get_top_10_address_rsk(self):
        conn = self.mysqlconnect()
        cur = conn.cursor()
        query_sql = "SELECT SUM(amount) as total, trader FROM rsk_bridge WHERE from_token IN ('rUSDT', 'DOC', 'rDAI', 'rUSDC') GROUP BY trader ORDER BY total DESC LIMIT 10"
        cur.execute(query_sql)
        _d = cur.fetchall()
        return _d

    def get_total_trade_by_date_rsk(self):
        conn = self.mysqlconnect()
        cur = conn.cursor()
        query_sql = "SELECT DATE(date) as time, SUM(amount) as value FROM rsk_bridge WHERE from_token IN ('rUSDT', 'DOC', 'rDAI', 'rUSDC') GROUP BY time"
        cur.execute(query_sql)
        _d = cur.fetchall()
        return _d

    def get_total_trade_by_month_rsk(self):
        conn = self.mysqlconnect()
        cur = conn.cursor()
        query_sql = "SELECT DATE(date) as time, SUM(amount) as value FROM rsk_bridge WHERE from_token IN ('rUSDT', 'DOC', 'rDAI', 'rUSDC') GROUP BY MONTH(date)"
        cur.execute(query_sql)
        _d = cur.fetchall()
        return _d
    
    def get_total_collected_fee_by_date_rsk(self):
        conn = self.mysqlconnect()
        cur = conn.cursor()
        query_sql = "SELECT DATE(date) as time, SUM(collected_fee) as value FROM rsk_bridge WHERE from_token IN ('rUSDT', 'DOC', 'rDAI', 'rUSDC') GROUP BY time"
        cur.execute(query_sql)
        _d = cur.fetchall()
        return _d

    def get_total_collected_fee_by_month_rsk(self):
        conn = self.mysqlconnect()
        cur = conn.cursor()
        query_sql = "SELECT DATE(date) as time, SUM(collected_fee) as value FROM rsk_bridge WHERE from_token IN ('rUSDT', 'DOC', 'rDAI', 'rUSDC') GROUP BY MONTH(date)"
        cur.execute(query_sql)
        _d = cur.fetchall()
        return _d

    def get_total_unique_user_by_date_rsk(self):
        conn = self.mysqlconnect()
        cur = conn.cursor()
        query_sql = "SELECT DATE(date) as time, COUNT(DISTINCT(trader)) as value FROM rsk_bridge WHERE from_token IN ('rUSDT', 'DOC', 'rDAI', 'rUSDC') GROUP BY time"
        cur.execute(query_sql)
        _d = cur.fetchall()
        return _d

    def get_total_unique_user_by_month_rsk(self):
        conn = self.mysqlconnect()
        cur = conn.cursor()
        query_sql = "SELECT DATE(date) as time, COUNT(DISTINCT(trader)) as value FROM rsk_bridge WHERE from_token IN ('rUSDT', 'DOC', 'rDAI', 'rUSDC') GROUP BY MONTH(date)"
        cur.execute(query_sql)
        _d = cur.fetchall()
        return _d

    def get_best_trade_sov(self):
        conn = self.mysqlconnect()
        cur = conn.cursor()
        query_sql = "SELECT from_value as total, tx_hash FROM sov_bridge WHERE from_value = (SELECT MAX(from_value) as total from sov_bridge) LIMIT 1"
        cur.execute(query_sql)
        _d = cur.fetchone()
        return _d

    def get_total_trade_sov(self):
        conn = self.mysqlconnect()
        cur = conn.cursor()
        query_sql = 'SELECT SUM(amount) as total from sov_bridge'
        cur.execute(query_sql)
        _d = cur.fetchone()
        return _d

    def get_total_unique_user_sovryn(self):
        conn = self.mysqlconnect()
        cur = conn.cursor()
        query_sql = "SELECT COUNT(DISTINCT(address)) as total FROM `sov_bridge`"
        cur.execute(query_sql)
        _d = cur.fetchone()
        return _d
    
    def get_total_fee_sovryn(self):
        conn = self.mysqlconnect()
        cur = conn.cursor()
        query_sql = "SELECT SUM(fee) as total FROM `sov_bridge`"
        cur.execute(query_sql)
        _d = cur.fetchone()
        return _d

    def get_top_trader_sov(self):
        conn = self.mysqlconnect()
        cur = conn.cursor()
        query_sql = "SELECT SUM(amount) as total, address as trader FROM `sov_bridge` GROUP BY address order by total DESC LIMIT 10"
        cur.execute(query_sql)
        _d = cur.fetchall()
        return _d

    def get_total_trade_by_date_sov(self):
        conn = self.mysqlconnect()
        cur = conn.cursor()
        query_sql = "SELECT DATE(sign_at) as time, SUM(amount) as value FROM `sov_bridge` GROUP BY time"
        cur.execute(query_sql)
        _d = cur.fetchall()
        return _d
    
    def get_total_trade_by_month_sov(self):
        conn = self.mysqlconnect()
        cur = conn.cursor()
        query_sql = "SELECT DATE(sign_at) as time, SUM(amount) as value FROM `sov_bridge` GROUP BY MONTH(sign_at)"
        cur.execute(query_sql)
        _d = cur.fetchall()
        return _d

    def get_collected_fee_sov_date(self):
        conn = self.mysqlconnect()
        cur = conn.cursor()
        query_sql = "SELECT DATE(sign_at) as time, SUM(fee) as value FROM sov_bridge GROUP BY time"
        cur.execute(query_sql)
        _d = cur.fetchall()
        return _d
    
    def get_collected_fee_sov_month(self):
        conn = self.mysqlconnect()
        cur = conn.cursor()
        query_sql = "SELECT DATE(sign_at) as time, SUM(fee) as value FROM sov_bridge GROUP BY MONTH(sign_at)"
        cur.execute(query_sql)
        _d = cur.fetchall()
        return _d

    def get_total_unique_address_by_date_sov(self):
        conn = self.mysqlconnect()
        cur = conn.cursor()
        query_sql = "SELECT DATE(sign_at) as time, COUNT(DISTINCT(address)) as value FROM sov_bridge GROUP BY time"
        cur.execute(query_sql)
        _d = cur.fetchall()
        return _d
    
    def get_total_unique_address_by_month_sov(self):
        conn = self.mysqlconnect()
        cur = conn.cursor()
        query_sql = "SELECT DATE(sign_at) as time, COUNT(DISTINCT(address)) as value FROM sov_bridge GROUP BY MONTH(time)"
        cur.execute(query_sql)
        _d = cur.fetchall()
        return _d

    def search_address_moc(self, address): 
        conn = self.mysqlconnect()
        cur = conn.cursor()
        query_sql = "SELECT * FROM `moc_protocol_mint_redeem` WHERE trader = '{address}' ORDER BY date DESC"
        cur.execute(query_sql.format(address=address))
        _d = cur.fetchall()
        return _d
    
    def search_address_rif(self, address): 
        conn = self.mysqlconnect()
        cur = conn.cursor()
        query_sql = "SELECT * FROM `rif_on_chain_mint_redeem` WHERE trader = '{address}' ORDER BY date DESC"
        cur.execute(query_sql.format(address=address))
        _d = cur.fetchall()
        return _d

    def search_address_rsk(self, address): 
        conn = self.mysqlconnect()
        cur = conn.cursor()
        query_sql = "SELECT * FROM rsk_bridge WHERE trader = '{address}' AND from_token IN ('rUSDT', 'DOC', 'rDAI', 'rUSDC') ORDER BY date DESC"
        cur.execute(query_sql.format(address=address))
        _d = cur.fetchall()
        return _d

    def search_address_sov(self, address): 
        conn = self.mysqlconnect()
        cur = conn.cursor()
        query_sql = "SELECT * FROM sov_bridge WHERE address = '{address}' ORDER BY sign_at DESC"
        cur.execute(query_sql.format(address=address))
        _d = cur.fetchall()
        return _d
