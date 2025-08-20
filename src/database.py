import os
import sys
# Додаємо кореневу директорію проекту до sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

import pandas as pd
import numpy as np
from typing import Optional, List, Dict, Any
import logging
from datetime import datetime, timedelta
import time
from config.database_config import DatabaseManager
from config.constants import *

# Налаштування логування
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PushDatabase:
    """
    Високорівневий клас для роботи з базами даних push-аналізу
    Надає зручні методи для отримання даних з можливістю кешування
    """
    
    def __init__(self, cache_enabled: bool = True):
        """
        Ініціалізація з опціональним кешуванням
        
        Args:
            cache_enabled: Чи використовувати кешування запитів
        """
        self.db_manager = DatabaseManager()
        self.cache_enabled = cache_enabled
        self._cache = {}
        
        # Створюємо директорії для кешу
        os.makedirs('data/cache', exist_ok=True)
    
    def _get_cache_key(self, query: str, params: dict = None) -> str:
        """Генерує ключ для кешування"""
        import hashlib
        cache_string = f"{query}_{str(params)}"
        return hashlib.md5(cache_string.encode()).hexdigest()
    
    def _load_from_cache(self, cache_key: str) -> Optional[pd.DataFrame]:
        """Завантажує дані з кешу"""
        if not self.cache_enabled:
            return None
            
        cache_file = f"data/cache/{cache_key}.parquet"
        if os.path.exists(cache_file):
            # Перевіряємо час створення файлу (кеш дійсний 1 годину)
            file_time = os.path.getmtime(cache_file)
            if time.time() - file_time < 3600:  # 1 година
                logger.info(f"📦 Завантаження з кешу: {cache_key[:8]}...")
                return pd.read_parquet(cache_file)
        return None
    
    def _save_to_cache(self, data: pd.DataFrame, cache_key: str) -> None:
        """Зберігає дані в кеш"""
        if not self.cache_enabled:
            return
            
        cache_file = f"data/cache/{cache_key}.parquet"
        data.to_parquet(cache_file)
        logger.info(f"💾 Збережено в кеш: {cache_key[:8]}...")
    
    def execute_query(self, database: str, query: str, params: dict = None, use_cache: bool = True) -> pd.DataFrame:
        """
        Виконує запит до бази даних з опціональним кешуванням
        
        Args:
            database: 'statistic' або 'keitaro'
            query: SQL запит
            params: Параметри запиту для кешування
            use_cache: Використовувати кеш чи ні
        
        Returns:
            DataFrame з результатами
        """
        # Перевіряємо кеш
        cache_key = self._get_cache_key(query, params) if use_cache else None
        if cache_key:
            cached_data = self._load_from_cache(cache_key)
            if cached_data is not None:
                return cached_data
        
        # Виконуємо запит
        try:
            if database == 'statistic':
                client = self.db_manager.connect_statistic()
            elif database == 'keitaro':
                client = self.db_manager.connect_keitaro()
            else:
                raise ValueError(f"Невідома база даних: {database}")
            
            logger.info(f"🔍 Виконання запиту до {database}...")
            start_time = time.time()
            
            df = self.db_manager.query_to_df(client, query)
            
            execution_time = time.time() - start_time
            logger.info(f"✅ Запит виконано за {execution_time:.2f}с, отримано {len(df)} записів")
            
            # Зберігаємо в кеш
            if cache_key and not df.empty:
                self._save_to_cache(df, cache_key)
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Помилка виконання запиту: {e}")
            raise
    
    def get_push_data(self, 
                     start_date: str = PUSH_START_DATE,
                     end_date: str = PUSH_END_DATE,
                     ab_groups: List[str] = None,
                     countries: List[str] = None) -> pd.DataFrame:
        """
        Отримує дані про push-сповіщення з фільтрацією
        
        Args:
            start_date: Початкова дата (YYYY-MM-DD)
            end_date: Кінцева дата (YYYY-MM-DD)
            ab_groups: Список A/B груп для фільтрації
            countries: Список країн для фільтрації
        
        Returns:
            DataFrame з push-даними
        """
        # Базовий запит
        where_conditions = [
            f"e.event_type = {PUSH_EVENT_TYPE}",
            f"e.type = {ANDROID_TYPE}",
            "d.gadid IS NOT NULL",
            "d.tag IS NOT NULL",
            f"toDate(e.created_at) >= '{start_date}'",
            f"toDate(e.created_at) <= '{end_date}'"
        ]
        
        # Додаткові фільтри
        if ab_groups:
            ab_filter = "', '".join(ab_groups)
            where_conditions.append(f"d.tag IN ('{ab_filter}')")
        
        if countries:
            country_filter = "', '".join(countries)
            where_conditions.append(f"d.country_name IN ('{country_filter}')")
        
        query = f"""
        SELECT 
            toString(d.gadid) as gadid,
            d.tag as ab_group,
            d.country_name as country,
            COUNT(*) as push_count,
            MIN(e.created_at) as first_push,
            MAX(e.created_at) as last_push,
            COUNT(DISTINCT toDate(e.created_at)) as push_days,
            AVG(e.sub_1) as avg_success_rate
        FROM event e
        JOIN device d ON e.device_id = d.id
        WHERE {' AND '.join(where_conditions)}
        GROUP BY gadid, ab_group, country
        ORDER BY push_count DESC
        """
        
        params = {
            'start_date': start_date,
            'end_date': end_date,
            'ab_groups': ab_groups,
            'countries': countries
        }
        
        return self.execute_query('statistic', query, params)
    
    def get_control_group_data(self,
                              start_date: str = PUSH_START_DATE,
                              end_date: str = PUSH_END_DATE) -> pd.DataFrame:
        """
        Отримує дані контрольної групи (група 6 - без push-сповіщень)
        
        Args:
            start_date: Початкова дата
            end_date: Кінцева дата
            
        Returns:
            DataFrame з користувачами групи 6
        """
        query = f"""
        SELECT 
            toString(gadid) as gadid,
            tag as ab_group,
            country_name as country,
            0 as push_count,
            NULL as first_push,
            NULL as last_push,
            0 as push_days,
            NULL as avg_success_rate
        FROM device
        WHERE tag = '6'
          AND type = {ANDROID_TYPE}
          AND gadid IS NOT NULL
        GROUP BY gadid, tag, country_name
        """
        
        params = {
            'start_date': start_date,
            'end_date': end_date
        }
        
        return self.execute_query('statistic', query, params)
    
    def get_conversion_data(self,
                           start_date: str = CONVERSION_START_DATE,
                           end_date: str = CONVERSION_END_DATE,
                           campaign_ids: List[int] = None,
                           conversion_types: List[str] = ['deposit', 'registration']) -> pd.DataFrame:
        """
        Отримує дані про конверсії з фільтрацією
        
        Args:
            start_date: Початкова дата
            end_date: Кінцева дата
            campaign_ids: Список ID кампаній
            conversion_types: Типи конверсій ('deposit', 'registration')
        
        Returns:
            DataFrame з конверсіями
        """
        where_conditions = [
            "sub_id_14 IS NOT NULL",
            "sub_id_14 != ''",
            f"date_key >= '{start_date}'",
            f"date_key <= '{end_date}'"
        ]
        
        # Фільтр по кампаніях
        if campaign_ids:
            ids_filter = ','.join(map(str, campaign_ids))
            where_conditions.append(f"campaign_id IN ({ids_filter})")
        
        # Фільтр по типах конверсій
        conversion_filter = []
        if 'deposit' in conversion_types:
            conversion_filter.append("is_sale > 0")
        if 'registration' in conversion_types:
            conversion_filter.append("is_lead > 0")
        
        if conversion_filter:
            where_conditions.append(f"({' OR '.join(conversion_filter)})")
        
        query = f"""
        SELECT 
            sub_id_14 as gadid,
            SUM(is_sale) as total_deposits,
            SUM(is_lead) as total_registrations,
            MIN(datetime) as first_conversion,
            MAX(datetime) as last_conversion,
            COUNT(*) as conversion_events,
            SUM(sale_revenue) as total_revenue,
            country,
            campaign_id
        FROM keitaro_clicks
        WHERE {' AND '.join(where_conditions)}
        GROUP BY gadid, country, campaign_id
        """
        
        params = {
            'start_date': start_date,
            'end_date': end_date,
            'campaign_ids': campaign_ids,
            'conversion_types': conversion_types
        }
        
        return self.execute_query('keitaro', query, params)
    
    def get_campaign_info(self, app_names: List[str] = TARGET_APPS) -> pd.DataFrame:
        """
        Отримує інформацію про кампанії цільових застосунків
        
        Args:
            app_names: Список назв застосунків
        
        Returns:
            DataFrame з інформацією про кампанії
        """
        # Створюємо умови пошуку для кожного застосунку
        search_conditions = []
        for app in app_names:
            search_conditions.extend([
                f"name ILIKE '%{app}%'",
                f"alias ILIKE '%{app}%'"
            ])
        
        query = f"""
        SELECT DISTINCT 
            id,
            name,
            alias,
            state,
            created_at,
            updated_at
        FROM keitaro_groups
        WHERE ({' OR '.join(search_conditions)})
        ORDER BY name
        """
        
        params = {'app_names': app_names}
        
        return self.execute_query('keitaro', query, params)
    
    def get_device_info(self, gadids: List[str]) -> pd.DataFrame:
        """
        Отримує додаткову інформацію про пристрої
        
        Args:
            gadids: Список GADID для пошуку
        
        Returns:
            DataFrame з інформацією про пристрої
        """
        if not gadids:
            return pd.DataFrame()
        
        # Обмежуємо кількість для уникнення занадто великих запитів
        gadids_sample = gadids[:10000] if len(gadids) > 10000 else gadids
        gadids_filter = "', '".join(gadids_sample)
        
        query = f"""
        SELECT 
            toString(gadid) as gadid,
            country_name,
            language_name,
            tag,
            type,
            active,
            timezone,
            created_at as device_created_at
        FROM device
        WHERE toString(gadid) IN ('{gadids_filter}')
        """
        
        params = {'gadids': len(gadids_sample)}
        
        return self.execute_query('statistic', query, params)
    
    def get_push_performance_summary(self) -> Dict[str, Any]:
        """
        Отримує загальну статистику по push-кампаніям
        
        Returns:
            Словник з ключовими метриками
        """
        logger.info("📊 Збір загальної статистики...")
        
        # Push статистика
        push_query = f"""
        SELECT 
            COUNT(DISTINCT d.gadid) as unique_users,
            COUNT(*) as total_pushes,
            COUNT(DISTINCT d.tag) as ab_groups_count,
            COUNT(DISTINCT d.country_name) as countries_count,
            AVG(e.sub_1) as avg_delivery_rate
        FROM event e
        JOIN device d ON e.device_id = d.id
        WHERE e.event_type = {PUSH_EVENT_TYPE}
          AND e.type = {ANDROID_TYPE}
          AND toDate(e.created_at) >= '{PUSH_START_DATE}'
          AND toDate(e.created_at) <= '{PUSH_END_DATE}'
        """
        
        push_stats = self.execute_query('statistic', push_query)
        
        # Конверсії статистика
        conversion_query = f"""
        SELECT 
            COUNT(DISTINCT sub_id_14) as unique_converters,
            SUM(is_sale) as total_deposits,
            SUM(is_lead) as total_registrations,
            SUM(sale_revenue) as total_revenue
        FROM keitaro_clicks
        WHERE sub_id_14 IS NOT NULL
          AND date_key >= '{CONVERSION_START_DATE}'
          AND date_key <= '{CONVERSION_END_DATE}'
          AND (is_sale > 0 OR is_lead > 0)
        """
        
        conversion_stats = self.execute_query('keitaro', conversion_query)
        
        # Об'єднуємо статистику
        summary = {
            'period': f"{PUSH_START_DATE} - {PUSH_END_DATE}",
            'push_stats': push_stats.iloc[0].to_dict() if not push_stats.empty else {},
            'conversion_stats': conversion_stats.iloc[0].to_dict() if not conversion_stats.empty else {},
            'generated_at': datetime.now().isoformat()
        }
        
        return summary
    
    def clear_cache(self):
        """Очищає кеш"""
        cache_dir = 'data/cache'
        if os.path.exists(cache_dir):
            for file in os.listdir(cache_dir):
                if file.endswith('.parquet'):
                    os.remove(os.path.join(cache_dir, file))
            logger.info("🗑️ Кеш очищено")
    
    def get_cache_info(self) -> Dict[str, Any]:
        """Інформація про кеш"""
        cache_dir = 'data/cache'
        if not os.path.exists(cache_dir):
            return {'files': 0, 'total_size': 0}
        
        files = [f for f in os.listdir(cache_dir) if f.endswith('.parquet')]
        total_size = sum(os.path.getsize(os.path.join(cache_dir, f)) for f in files)
        
        return {
            'files': len(files),
            'total_size_mb': round(total_size / 1024 / 1024, 2),
            'cache_enabled': self.cache_enabled
        }