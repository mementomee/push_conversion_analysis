#!/usr/bin/env python3
"""
Приклад аналізу з включенням контрольної групи
"""

import sys
import os
sys.path.insert(0, os.path.abspath('.'))

from src.data_loader import DataLoader
from src.analyzer import PushAnalyzer
import json

def run_complete_ab_analysis():
    """Запускає повний A/B аналіз з контрольною групою"""
    
    print("🚀 Запуск повного A/B аналізу з контрольною групою...")
    
    # Ініціалізуємо компоненти
    data_loader = DataLoader(cache_enabled=True)
    analyzer = PushAnalyzer()
    
    # 1. Завантажуємо дані з контрольною групою
    print("\n📊 Завантаження повного набору даних...")
    complete_df = data_loader.load_complete_dataset(include_control_group=True)
    
    # 2. Завантажуємо конверсії
    print("💰 Завантаження конверсій...")
    conversions_df = data_loader.load_conversion_data()
    
    # 3. Об'єднуємо дані
    print("🔗 Об'єднання даних...")
    merged_df = analyzer.merge_data(complete_df, conversions_df)
    
    # 4. Проводимо A/B аналіз
    print("📈 A/B аналіз з контрольною групою...")
    ab_stats = analyzer.ab_analysis(merged_df)
    
    # 5. Розраховуємо ефективність push-сповіщень
    print("🎯 Розрахунок ефективності push-сповіщень...")
    effectiveness = analyzer.calculate_push_effectiveness(ab_stats)
    
    # 6. Виводимо результати
    print("\n" + "="*80)
    print("📋 РЕЗУЛЬТАТИ A/B АНАЛІЗУ З КОНТРОЛЬНОЮ ГРУПОЮ")
    print("="*80)
    
    print("\n📊 Статистика по групах:")
    print(ab_stats.to_string())
    
    if "error" not in effectiveness:
        print(f"\n🎯 ЕФЕКТИВНІСТЬ PUSH-СПОВІЩЕНЬ:")
        print("-" * 50)
        
        control = effectiveness["control_group"]
        push_avg = effectiveness["push_groups_average"] 
        best = effectiveness["best_push_group"]
        summary = effectiveness["summary"]
        
        print(f"👥 Контрольна група (без push-ів):")
        print(f"   Користувачів: {control['users']:,}")
        print(f"   Депозитів: {control['deposits']:,}")
        print(f"   Конверсія: {control['conversion_rate']:.3f}%")
        print(f"   ARPU: ${control['arpu']:.4f}")
        
        print(f"\n📈 Середня ефективність push-груп:")
        print(f"   Конверсія: {push_avg['conversion_rate']:.3f}%")
        print(f"   Покращення: +{push_avg['improvement_vs_control']:.3f} п.п.")
        print(f"   Відносне покращення: +{push_avg['relative_improvement_pct']:.1f}%")
        
        print(f"\n🏆 Найкраща push-група ({best['group_id']}):")
        print(f"   Конверсія: {best['conversion_rate']:.3f}%")
        print(f"   Покращення vs контроль: +{best['improvement_vs_control']:.3f} п.п.")
        
        print(f"\n✅ ВИСНОВОК:")
        if summary['push_effective']:
            print(f"   Push-сповіщення ЕФЕКТИВНІ!")
            print(f"   Середнє покращення: +{summary['avg_lift_percentage_points']:.3f} п.п.")
            print(f"   Максимальне покращення: +{summary['best_lift_percentage_points']:.3f} п.п.")
        else:
            print(f"   Push-сповіщення НЕ показують позитивного ефекту")
    
    # 7. Зберігаємо результати
    print(f"\n💾 Збереження результатів...")
    
    # Зберігаємо CSV для аналізу
    ab_stats.to_csv('outputs/final_results/ab_analysis_with_control.csv')
    
    # Зберігаємо JSON з ефективністю
    with open('outputs/final_results/push_effectiveness_analysis.json', 'w', encoding='utf-8') as f:
        json.dump(effectiveness, f, ensure_ascii=False, indent=2)
    
    print("✅ Результати збережено в outputs/final_results/")
    
    return ab_stats, effectiveness

if __name__ == "__main__":
    ab_stats, effectiveness = run_complete_ab_analysis()
    print("\n🎉 Аналіз завершено!")