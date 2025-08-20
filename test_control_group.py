#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Тестування роботи з контрольною групою (група 6)
"""

import sys
import os
sys.path.insert(0, os.path.abspath('.'))

from src.data_loader import DataLoader
from src.analyzer import PushAnalyzer
import pandas as pd

def test_control_group_analysis():
    """Тестує аналіз з включенням контрольної групи"""
    
    print("Тестування аналізу з контрольною групою...")
    
    # Ініціалізуємо компоненти
    data_loader = DataLoader(cache_enabled=True)
    analyzer = PushAnalyzer()
    
    try:
        # Завантажуємо повний набір даних включаючи контрольну групу
        print("\nЗавантаження даних з контрольною групою...")
        complete_df = data_loader.load_complete_dataset(include_control_group=True)
        
        if complete_df.empty:
            print("ПОМИЛКА: Дані не завантажилися")
            return False
        
        # Завантажуємо конверсії
        print("\nЗавантаження конверсій...")
        conversions_df = data_loader.load_conversion_data()
        
        # Об'єднуємо дані
        print("\nОб'єднання даних...")
        merged_df = analyzer.merge_data(complete_df, conversions_df)
        
        # Проводимо A/B аналіз з контрольною групою
        print("\nA/B аналіз з контрольною групою...")
        ab_stats = analyzer.ab_analysis(merged_df)
        
        print("\nРезультати A/B аналізу:")
        print("=" * 80)
        print(ab_stats.to_string())
        
        # Перевіряємо наявність групи 6
        if '6' in ab_stats.index:
            print(f"\nУСПІХ: Контрольна група 6 знайдена!")
            control_stats = ab_stats.loc['6']
            print(f"Користувачів у групі 6: {control_stats['total_users']:,}")
            print(f"Депозитів у групі 6: {control_stats['users_with_deposits']:,}")
            print(f"Конверсія групи 6: {control_stats['deposit_conversion']:.3f}%")
            print(f"ARPU групи 6: ${control_stats['arpu']:.4f}")
            
            # Порівняння з push-групами
            push_groups = ab_stats[ab_stats['group_type'] == 'Push Group']
            avg_push_conversion = push_groups['deposit_conversion'].mean()
            control_conversion = control_stats['deposit_conversion']
            
            print(f"\nПорівняння:")
            print(f"Середня конверсія push-груп: {avg_push_conversion:.3f}%")
            print(f"Конверсія контрольної групи: {control_conversion:.3f}%")
            print(f"Різниця: {avg_push_conversion - control_conversion:+.3f} п.п.")
            
            if avg_push_conversion > control_conversion:
                print("УСПІХ: Push-сповіщення показують позитивний ефект!")
            else:
                print("УВАГА: Push-сповіщення не показують позитивного ефекту")
        else:
            print("ПОМИЛКА: Контрольна група 6 не знайдена в даних")
            return False
        
        return True
        
    except Exception as e:
        print(f"ПОМИЛКА під час тестування: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Головна функція"""
    print("Запуск тестування контрольної групи...")
    
    success = test_control_group_analysis()
    
    if success:
        print("\nУСПІХ: Тестування завершено успішно!")
        print("Тепер можна запускати повний аналіз з включенням контрольної групи")
    else:
        print("\nПОМИЛКА: Тестування не вдалося")
        
if __name__ == "__main__":
    main()