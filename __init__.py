"""
Push Analysis Project - Аналіз впливу push-сповіщень на депозити Android-користувачів

Це пакет для аналізу ефективності push-сповіщень в мобільних застосунках.
Включає інструменти для завантаження даних, A/B тестування, 
географічного аналізу та візуалізації результатів.

Основні компоненти:
- config: Налаштування баз даних та константи
- src: Основна логіка аналізу
- notebooks: Jupyter notebooks для інтерактивного аналізу
"""

__version__ = "1.0.0"
__title__ = "Push Analysis"
__description__ = "Аналіз впливу push-сповіщень на поведінку користувачів"
__author__ = "Nazar Petrashchuk"

def test_setup():
    """
    Швидка перевірка налаштувань проекту
    
    Returns:
        bool: Чи готова система до роботи
    """
    try:
        from config.database_config import DatabaseManager
        db = DatabaseManager()
        db.test_connections()
        print("✅ Система готова до роботи!")
        return True
    except Exception as e:
        print(f"❌ Помилка налаштування: {e}")
        print("💡 Запустіть: python test_system.py для детальної діагностики")
        return False

# Не робимо складних імпортів на рівні пакету щоб уникнути проблем
__all__ = ['test_setup']