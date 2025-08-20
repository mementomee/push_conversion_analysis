import os
import sys
# Додаємо кореневу директорію проекту до sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

import pandas as pd
from typing import List, Optional, Dict, Any
import logging
from src.database import PushDatabase
from config.constants import *

logger = logging.getLogger(__name__)

class DataLoader:
    """
    Клас для завантаження та підготовки даних для аналізу push-сповіщень
    Використовує PushDatabase для доступу до даних
    """
    
    def __init__(self, cache_enabled: bool = True):
        """
        Ініціалізація з опціональним кешуванням
        
        Args:
            cache_enabled: Використовувати кеш для запитів
        """
        self.db = PushDatabase(cache_enabled=cache_enabled)
        self.target_campaign_ids = None
        
    def load_push_data(self, 
                      start_date: str = PUSH_START_DATE,
                      end_date: str = PUSH_END_DATE,
                      ab_groups: List[str] = None,
                      countries: List[str] = None) -> pd.DataFrame:
        """
        Завантажити дані про push-сповіщення
        
        Args:
            start_date: Початкова дата (YYYY-MM-DD)
            end_date: Кінцева дата (YYYY-MM-DD)
            ab_groups: Фільтр по A/B групах
            countries: Фільтр по країнах
            
        Returns:
            DataFrame з push-даними
        """
        logger.info(f"📱 Завантаження push-даних: {start_date} - {end_date}")
        
        df = self.db.get_push_data(start_date, end_date, ab_groups, countries)
        
        if df.empty:
            logger.warning("⚠️ Push-дані не знайдено!")
            return df
        
        # Додаткова обробка
        df = self._process_push_data(df)
        
        logger.info(f"✅ Завантажено {len(df)} push-записів для {df['gadid'].nunique()} користувачів")
        return df
    
    def load_complete_dataset(self,
                             start_date: str = PUSH_START_DATE,
                             end_date: str = PUSH_END_DATE,
                             include_control_group: bool = True) -> pd.DataFrame:
        """
        Завантажити повний набір даних включаючи контрольну групу
        
        Args:
            start_date: Початкова дата
            end_date: Кінцева дата
            include_control_group: Включити групу 6 (контрольну)
            
        Returns:
            DataFrame з усіма групами включаючи контрольну
        """
        logger.info("📊 Завантаження повного набору даних з контрольною групою...")
        
        # Завантажуємо push-дані (групи 1-5)
        push_df = self.load_push_data(start_date, end_date)
        
        if include_control_group:
            # Завантажуємо контрольну групу (група 6)
            control_df = self.db.get_control_group_data(start_date, end_date)
            
            if not control_df.empty:
                # Обробляємо контрольну групу
                control_df = self._process_control_group_data(control_df)
                
                # Приводимо колонки до консистентного формату
                control_df = self._align_dataframes(push_df, control_df)
                
                # Об'єднуємо з push-даними
                complete_df = pd.concat([push_df, control_df], ignore_index=True)
                logger.info(f"✅ Додано {len(control_df)} користувачів з контрольної групи 6")
            else:
                logger.warning("⚠️ Контрольна група не знайдена")
                complete_df = push_df
        else:
            complete_df = push_df
        
        logger.info(f"📋 Загалом {len(complete_df)} записів для {complete_df['gadid'].nunique()} користувачів")
        return complete_df
    
    def load_conversion_data(self, 
                           start_date: str = CONVERSION_START_DATE,
                           end_date: str = CONVERSION_END_DATE,
                           campaign_ids: List[int] = None,
                           conversion_types: List[str] = ['deposit', 'registration']) -> pd.DataFrame:
        """
        Завантажити дані про конверсії
        
        Args:
            start_date: Початкова дата
            end_date: Кінцева дата
            campaign_ids: ID кампаній (автоматично знаходяться якщо None)
            conversion_types: Типи конверсій
            
        Returns:
            DataFrame з конверсіями
        """
        logger.info(f"💰 Завантаження конверсій: {start_date} - {end_date}")
        
        # Автоматично знаходимо кампанії якщо не вказані
        if campaign_ids is None:
            if self.target_campaign_ids is None:
                self.target_campaign_ids = self.find_campaign_groups()
            campaign_ids = self.target_campaign_ids
        
        df = self.db.get_conversion_data(start_date, end_date, campaign_ids, conversion_types)
        
        if df.empty:
            logger.warning("⚠️ Конверсії не знайдено!")
            return df
        
        # Додаткова обробка
        df = self._process_conversion_data(df)
        
        logger.info(f"✅ Завантажено {len(df)} конверсій для {df['gadid'].nunique()} користувачів")
        return df
    
    def find_campaign_groups(self, app_names: List[str] = TARGET_APPS) -> List[int]:
        """
        Знайти ID кампаній цільових застосунків
        
        Args:
            app_names: Список назв застосунків
            
        Returns:
            Список ID кампаній
        """
        logger.info(f"🔍 Пошук кампаній для застосунків: {app_names}")
        
        campaigns_df = self.db.get_campaign_info(app_names)
        
        if campaigns_df.empty:
            logger.warning("⚠️ Кампанії не знайдено!")
            return []
        
        # Фільтруємо активні кампанії
        active_campaigns = campaigns_df[campaigns_df['state'] == 'active']
        campaign_ids = active_campaigns['id'].tolist()
        
        logger.info(f"✅ Знайдено {len(campaign_ids)} активних кампаній")
        logger.info(f"📋 Кампанії: {campaigns_df[['name', 'id', 'state']].to_dict('records')}")
        
        return campaign_ids
    
    def load_full_dataset(self, 
                         push_filters: Dict[str, Any] = None,
                         conversion_filters: Dict[str, Any] = None) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Завантажити повний набір даних для аналізу
        
        Args:
            push_filters: Фільтри для push-даних
            conversion_filters: Фільтри для конверсій
            
        Returns:
            Tuple (push_df, conversions_df)
        """
        logger.info("📊 Завантаження повного набору даних...")
        
        # Параметри за замовчуванням
        push_params = {
            'start_date': PUSH_START_DATE,
            'end_date': PUSH_END_DATE,
            'ab_groups': None,
            'countries': None
        }
        if push_filters:
            push_params.update(push_filters)
        
        conversion_params = {
            'start_date': CONVERSION_START_DATE,
            'end_date': CONVERSION_END_DATE,
            'campaign_ids': None,
            'conversion_types': ['deposit', 'registration']
        }
        if conversion_filters:
            conversion_params.update(conversion_filters)
        
        # Завантажуємо дані
        push_df = self.load_push_data(**push_params)
        conversions_df = self.load_conversion_data(**conversion_params)
        
        return push_df, conversions_df
    
    def get_data_summary(self) -> Dict[str, Any]:
        """
        Отримати загальну статистику по даних
        
        Returns:
            Словник з метриками
        """
        return self.db.get_push_performance_summary()
    
    def load_device_enrichment(self, gadids: List[str]) -> pd.DataFrame:
        """
        Завантажити додаткову інформацію про пристрої
        
        Args:
            gadids: Список GADID
            
        Returns:
            DataFrame з інформацією про пристрої
        """
        logger.info(f"📱 Завантаження інформації про {len(gadids)} пристроїв...")
        
        df = self.db.get_device_info(gadids)
        
        if not df.empty:
            logger.info(f"✅ Знайдено інформацію про {len(df)} пристроїв")
        
        return df
    
    def _process_push_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Додаткова обробка push-даних
        
        Args:
            df: Сирі push-дані
            
        Returns:
            Оброблені дані
        """
        # Конвертуємо дати
        df['first_push'] = pd.to_datetime(df['first_push'])
        df['last_push'] = pd.to_datetime(df['last_push'])
        
        # Розраховуємо тривалість кампанії
        df['campaign_duration_hours'] = (
            df['last_push'] - df['first_push']
        ).dt.total_seconds() / 3600
        
        # Додаємо категорії по кількості push-ів
        df['push_category'] = pd.cut(
            df['push_count'], 
            bins=[0, 1, 3, 5, 10, float('inf')],
            labels=['1', '2-3', '4-5', '6-10', '10+'],
            include_lowest=True
        )
        
        # Додаємо tier країни
        df['tier'] = df['country'].apply(get_country_tier)
        
        # Валідація даних
        df = df[df['gadid'].notna() & (df['gadid'] != '')]
        df = df[df['ab_group'].notna()]
        df = df[df['push_count'] > 0]
        
        return df
    
    def _process_conversion_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Додаткова обробка конверсій
        
        Args:
            df: Сирі дані конверсій
            
        Returns:
            Оброблені дані
        """
        # Конвертуємо дати
        df['first_conversion'] = pd.to_datetime(df['first_conversion'])
        df['last_conversion'] = pd.to_datetime(df['last_conversion'])
        
        # Розраховуємо тривалість конверсій
        df['conversion_window_hours'] = (
            df['last_conversion'] - df['first_conversion']
        ).dt.total_seconds() / 3600
        
        # Категорії користувачів
        df['user_type'] = 'No Conversion'
        df.loc[df['total_registrations'] > 0, 'user_type'] = 'Registration Only'
        df.loc[df['total_deposits'] > 0, 'user_type'] = 'Deposit'
        
        # ARPU розрахунки
        df['arpu'] = df['total_revenue'] / df['conversion_events'].clip(lower=1)
        
        # Додаємо tier країни
        df['tier'] = df['country'].apply(get_country_tier)
        
        # Валідація
        df = df[df['gadid'].notna() & (df['gadid'] != '')]
        
        return df
    
    def _process_control_group_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Додаткова обробка контрольної групи (група 6)
        
        Args:
            df: Сирі дані контрольної групи
            
        Returns:
            Оброблені дані
        """
        if df.empty:
            return df
            
        # Заповнюємо порожні значення для консистентності з push-даними
        df['campaign_duration_hours'] = 0
        df['push_category'] = '0'  # Нуль push-ів
        df['push_days'] = 0
        
        # Конвертуємо дати (навіть якщо NULL)
        df['first_push'] = pd.NaT
        df['last_push'] = pd.NaT
        
        # Додаємо tier країни якщо є country
        if 'country' in df.columns:
            df['tier'] = df['country'].apply(get_country_tier)
        
        # Валідація даних
        df = df[df['gadid'].notna() & (df['gadid'] != '')]
        df = df[df['ab_group'].notna()]
        
        return df
    
    def _align_dataframes(self, push_df: pd.DataFrame, control_df: pd.DataFrame) -> pd.DataFrame:
        """
        Приводить структуру control_df до відповідності з push_df
        
        Args:
            push_df: DataFrame з push-даними
            control_df: DataFrame з контрольною групою
            
        Returns:
            Узгоджений control_df
        """
        if push_df.empty or control_df.empty:
            return control_df
        
        # Отримуємо всі колонки з push_df
        push_columns = set(push_df.columns)
        control_columns = set(control_df.columns)
        
        # Додаємо відсутні колонки в control_df
        missing_columns = push_columns - control_columns
        for col in missing_columns:
            if col in ['first_push', 'last_push']:
                control_df[col] = pd.NaT
            elif col in ['push_count', 'push_days', 'campaign_duration_hours']:
                control_df[col] = 0
            elif col in ['avg_success_rate']:
                control_df[col] = None
            elif col == 'push_category':
                control_df[col] = '0'
            elif col == 'tier':
                if 'country' in control_df.columns:
                    control_df[col] = control_df['country'].apply(get_country_tier)
                else:
                    control_df[col] = 'Unknown'
            else:
                # Для інших колонок ставимо None/NaN
                control_df[col] = None
        
        # Впорядковуємо колонки як у push_df
        control_df = control_df.reindex(columns=push_df.columns, fill_value=None)
        
        return control_df
    
    def save_processed_data(self, 
                          push_df: pd.DataFrame, 
                          conversions_df: pd.DataFrame,
                          merged_df: pd.DataFrame = None) -> None:
        """
        Зберегти оброблені дані
        
        Args:
            push_df: Push-дані
            conversions_df: Конверсії
            merged_df: Об'єднані дані (опціонально)
        """
        logger.info("💾 Збереження оброблених даних...")
        
        # Створюємо папки
        os.makedirs('data/processed', exist_ok=True)
        
        # Зберігаємо в ефективному форматі
        push_df.to_parquet('data/processed/push_data.parquet')
        conversions_df.to_parquet('data/processed/conversion_data.parquet')
        
        if merged_df is not None:
            merged_df.to_parquet('data/processed/merged_data.parquet')
        
        logger.info("✅ Дані збережено в data/processed/")
    
    def load_processed_data(self) -> tuple[pd.DataFrame, pd.DataFrame, Optional[pd.DataFrame]]:
        """
        Завантажити збережені оброблені дані
        
        Returns:
            Tuple (push_df, conversions_df, merged_df)
        """
        logger.info("📂 Завантаження збережених даних...")
        
        try:
            push_df = pd.read_parquet('data/processed/push_data.parquet')
            conversions_df = pd.read_parquet('data/processed/conversion_data.parquet')
            
            merged_df = None
            if os.path.exists('data/processed/merged_data.parquet'):
                merged_df = pd.read_parquet('data/processed/merged_data.parquet')
            
            logger.info("✅ Дані успішно завантажено")
            return push_df, conversions_df, merged_df
            
        except FileNotFoundError:
            logger.warning("⚠️ Оброблені дані не знайдено")
            return pd.DataFrame(), pd.DataFrame(), None
    
    def clear_cache(self):
        """Очищує кеш бази даних"""
        self.db.clear_cache()
    
    def get_cache_info(self) -> Dict[str, Any]:
        """Інформація про кеш"""
        return self.db.get_cache_info()