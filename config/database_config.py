import clickhouse_connect
import pandas as pd
from typing import Optional

class DatabaseManager:
    """Менеджер підключень до баз даних"""
    
    def __init__(self):
        self.statistic_client = None
        self.keitaro_client = None
    
    def connect_statistic(self) -> clickhouse_connect.driver.Client:
        """Підключення до бази statistic"""
        if not self.statistic_client:
            self.statistic_client = clickhouse_connect.get_client(
                host='91.99.82.194',
                port=8123,
                username='readonly',
                password='cohsh3Yahr6ohlie5Ei'
            )
            print("✅ Підключено до statistic")
        return self.statistic_client
    
    def connect_keitaro(self) -> clickhouse_connect.driver.Client:
        """Підключення до бази keitaro"""
        if not self.keitaro_client:
            self.keitaro_client = clickhouse_connect.get_client(
                host='65.108.255.109',
                port=18123,
                username='keitaro',
                password='5df78e427ac04c3b867f878a39056813',
                database='keitaro'
            )
            print("✅ Підключено до keitaro")
        return self.keitaro_client
    
    def test_connections(self):
        """Тестування підключень"""
        print("🔍 Тестування підключень...")
        
        try:
            # Тест statistic
            stat_client = self.connect_statistic()
            result = stat_client.query("SELECT 1 as test").result_rows
            print(f"✅ Statistic: {result}")
            
            # Тест keitaro
            keitaro_client = self.connect_keitaro()
            result = keitaro_client.query("SELECT 1 as test").result_rows
            print(f"✅ Keitaro: {result}")
            
        except Exception as e:
            print(f"❌ Помилка підключення: {e}")
    
    def query_to_df(self, client, query: str) -> pd.DataFrame:
        """Виконати запит та повернути DataFrame"""
        try:
            result = client.query(query)
            return pd.DataFrame(result.result_rows, columns=result.column_names)
        except Exception as e:
            print(f"❌ Помилка запиту: {e}")
            return pd.DataFrame()