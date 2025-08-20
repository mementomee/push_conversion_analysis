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
        merged['total_revenue'] = merged['total_revenue'].fillna(0)
        merged['has_deposit'] = (merged['total_deposits'] > 0).astype(int)
        merged['has_registration'] = (merged['total_registrations'] > 0).astype(int)
        
        # Обробка колонки country - використовуємо з push_df якщо є конфлікт
        if 'country_x' in merged.columns and 'country_y' in merged.columns:
            merged['country'] = merged['country_x'].fillna(merged['country_y'])
            merged = merged.drop(['country_x', 'country_y'], axis=1)
        elif 'country' not in merged.columns:
            # Якщо country взагалі немає, використовуємо з оригінального push_df
            if 'country' in push_df.columns:
                country_mapping = push_df[['gadid', 'country']].drop_duplicates()
                merged = merged.merge(country_mapping, on='gadid', how='left')
        
        # Додати tier якщо є country
        if 'country' in merged.columns:
            merged['tier'] = merged['country'].apply(get_country_tier)
        else:
            merged['tier'] = 'Unknown'
        
        return merged
    
    def ab_analysis(self, df: pd.DataFrame) -> pd.DataFrame:
        """A/B аналіз груп включаючи контрольну групу"""
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
        
        # Додаємо ARPU
        ab_stats['arpu'] = (
            ab_stats['total_deposits'] / ab_stats['total_users']
        ).round(4)
        
        # Додаємо тип групи
        ab_stats['group_type'] = 'Push Group'
        if '6' in ab_stats.index:
            ab_stats.loc['6', 'group_type'] = 'Control Group'
        
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
    
    def calculate_push_effectiveness(self, ab_stats: pd.DataFrame) -> dict:
        """
        Розраховує ефективність push-сповіщень порівняно з контрольною групою
        
        Args:
            ab_stats: Результати A/B аналізу
            
        Returns:
            Словник з метриками ефективності
        """
        if '6' not in ab_stats.index:
            return {"error": "Контрольна група не знайдена"}
        
        control_stats = ab_stats.loc['6']
        push_groups = ab_stats[ab_stats['group_type'] == 'Push Group']
        
        # Базові метрики
        control_conversion = control_stats['deposit_conversion']
        avg_push_conversion = push_groups['deposit_conversion'].mean()
        best_push_conversion = push_groups['deposit_conversion'].max()
        
        # Розрахунки ефективності
        avg_improvement = avg_push_conversion - control_conversion
        best_improvement = best_push_conversion - control_conversion
        relative_improvement = (avg_push_conversion / control_conversion - 1) * 100 if control_conversion > 0 else 0
        
        # Найкраща push-група
        best_group = push_groups['deposit_conversion'].idxmax()
        
        return {
            "control_group": {
                "users": int(control_stats['total_users']),
                "deposits": int(control_stats['users_with_deposits']),
                "conversion_rate": float(control_stats['deposit_conversion']),
                "arpu": float(control_stats['arpu'])
            },
            "push_groups_average": {
                "conversion_rate": float(avg_push_conversion),
                "improvement_vs_control": float(avg_improvement),
                "relative_improvement_pct": float(relative_improvement)
            },
            "best_push_group": {
                "group_id": best_group,
                "conversion_rate": float(best_push_conversion),
                "improvement_vs_control": float(best_improvement)
            },
            "summary": {
                "push_effective": avg_push_conversion > control_conversion,
                "avg_lift_percentage_points": float(avg_improvement),
                "best_lift_percentage_points": float(best_improvement)
            }
        }