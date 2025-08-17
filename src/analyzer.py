import os
import sys
# Додаємо кореневу директорію проекту до sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

import pandas as pd
from config.constants import get_country_tier

class PushAnalyzer:
    """Основний клас для аналізу"""
    
    def __init__(self):
        pass
    
    def merge_data(self, push_df: pd.DataFrame, conversions_df: pd.DataFrame) -> pd.DataFrame:
        """Об'єднати push та conversion дані"""
        # LEFT JOIN - всі push користувачі + їх конверсії
        merged = push_df.merge(conversions_df, on='gadid', how='left')
        
        # Заповнити пропуски
        merged['total_deposits'] = merged['total_deposits'].fillna(0)
        merged['total_registrations'] = merged['total_registrations'].fillna(0)
        merged['has_deposit'] = (merged['total_deposits'] > 0).astype(int)
        merged['has_registration'] = (merged['total_registrations'] > 0).astype(int)
        
        # Додати tier
        merged['tier'] = merged['country'].apply(get_country_tier)
        
        return merged
    
    def ab_analysis(self, df: pd.DataFrame) -> pd.DataFrame:
        """A/B аналіз груп"""
        ab_stats = df.groupby('ab_group').agg({
            'gadid': 'count',
            'push_count': 'mean',
            'has_deposit': 'sum',
            'has_registration': 'sum',
            'total_deposits': 'sum',
            'total_registrations': 'sum'
        }).round(2)
        
        ab_stats.columns = ['total_users', 'avg_pushes', 'users_with_deposits', 
                           'users_with_regs', 'total_deposits', 'total_registrations']
        
        # Конверсії
        ab_stats['deposit_conversion'] = (
            ab_stats['users_with_deposits'] / ab_stats['total_users'] * 100
        ).round(3)
        
        ab_stats['reg_conversion'] = (
            ab_stats['users_with_regs'] / ab_stats['total_users'] * 100
        ).round(3)
        
        return ab_stats
    
    def geo_analysis(self, df: pd.DataFrame) -> pd.DataFrame:
        """Аналіз по географії"""
        geo_stats = df.groupby(['tier', 'ab_group']).agg({
            'gadid': 'count',
            'push_count': 'mean',
            'has_deposit': 'sum',
            'total_deposits': 'sum'
        }).round(2)
        
        geo_stats['conversion_rate'] = (
            geo_stats['has_deposit'] / geo_stats['gadid'] * 100
        ).round(3)
        
        return geo_stats