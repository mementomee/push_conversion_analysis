"""
Основні модулі для аналізу push-сповіщень
"""

# Імпортуємо тільки після того, як всі файли створені
try:
    from .database import PushDatabase
    from .data_loader import DataLoader
    from .analyzer import PushAnalyzer
    from .visualizer import PushVisualizer
    
    __all__ = [
        'PushDatabase',
        'DataLoader', 
        'PushAnalyzer',
        'PushVisualizer'
    ]
    
    def get_analysis_suite(cache_enabled=True):
        """
        Повертає готовий набір інструментів для аналізу
        
        Returns:
            tuple: (loader, analyzer, visualizer)
        """
        loader = DataLoader(cache_enabled=cache_enabled)
        analyzer = PushAnalyzer()
        visualizer = PushVisualizer()
        
        return loader, analyzer, visualizer

except ImportError as e:
    print(f"Увага: Деякі модулі не завантажені: {e}")
    __all__ = []

__version__ = "1.0.0"
__author__ = "Nazar Petrashchuk"