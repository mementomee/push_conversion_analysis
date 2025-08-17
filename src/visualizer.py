import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np

# Українська локалізація для matplotlib
plt.rcParams['font.family'] = ['DejaVu Sans']
sns.set_style("whitegrid")
sns.set_palette("husl")

class PushVisualizer:
    """Клас для створення візуалізацій аналізу push-сповіщень"""
    
    def __init__(self, figsize=(12, 8)):
        self.figsize = figsize
        
    def plot_ab_conversion_comparison(self, ab_stats: pd.DataFrame, save_path: str = None):
        """Порівняння конверсій між A/B групами"""
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        fig.suptitle('Порівняння A/B груп push-сповіщень', fontsize=16, fontweight='bold')
        
        # 1. Кількість користувачів по групах
        ab_stats['total_users'].plot(kind='bar', ax=axes[0,0], color='skyblue')
        axes[0,0].set_title('Кількість користувачів по A/B групах')
        axes[0,0].set_ylabel('Кількість користувачів')
        axes[0,0].tick_params(axis='x', rotation=45)
        
        # 2. Середня кількість push-ів
        ab_stats['avg_pushes'].plot(kind='bar', ax=axes[0,1], color='lightgreen')
        axes[0,1].set_title('Середня кількість push-ів')
        axes[0,1].set_ylabel('Середня к-ть push-ів')
        axes[0,1].tick_params(axis='x', rotation=45)
        
        # 3. Конверсія в депозити
        ab_stats['deposit_conversion'].plot(kind='bar', ax=axes[1,0], color='coral')
        axes[1,0].set_title('Конверсія в депозити (%)')
        axes[1,0].set_ylabel('Конверсія (%)')
        axes[1,0].tick_params(axis='x', rotation=45)
        
        # 4. Конверсія в реєстрації
        ab_stats['reg_conversion'].plot(kind='bar', ax=axes[1,1], color='gold')
        axes[1,1].set_title('Конверсія в реєстрації (%)')
        axes[1,1].set_ylabel('Конверсія (%)')
        axes[1,1].tick_params(axis='x', rotation=45)
        
        plt.tight_layout()
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.show()
    
    def plot_geo_tier_analysis(self, geo_stats: pd.DataFrame, save_path: str = None):
        """Аналіз по географічних tier-ах"""
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=['Користувачі по Tier', 'Середні push-и по Tier', 
                          'Конверсія по Tier', 'Депозити по Tier'],
            specs=[[{"secondary_y": False}, {"secondary_y": False}],
                   [{"secondary_y": False}, {"secondary_y": False}]]
        )
        
        # Агрегація по tier
        tier_summary = geo_stats.groupby('tier').agg({
            'gadid': 'sum',
            'push_count': 'mean', 
            'conversion_rate': 'mean',
            'total_deposits': 'sum'
        }).reset_index()
        
        # Додаємо графіки
        fig.add_trace(go.Bar(x=tier_summary['tier'], y=tier_summary['gadid'],
                            name='Користувачі', marker_color='lightblue'),
                     row=1, col=1)
        
        fig.add_trace(go.Bar(x=tier_summary['tier'], y=tier_summary['push_count'],
                            name='Середні push-и', marker_color='lightgreen'),
                     row=1, col=2)
        
        fig.add_trace(go.Bar(x=tier_summary['tier'], y=tier_summary['conversion_rate'],
                            name='Конверсія (%)', marker_color='coral'),
                     row=2, col=1)
        
        fig.add_trace(go.Bar(x=tier_summary['tier'], y=tier_summary['total_deposits'],
                            name='Депозити', marker_color='gold'),
                     row=2, col=2)
        
        fig.update_layout(height=800, showlegend=False, 
                         title_text="Аналіз по географічних Tier-ах")
        
        if save_path:
            fig.write_html(save_path)
        fig.show()
    
    def plot_optimal_pushes_by_tier(self, df: pd.DataFrame, save_path: str = None):
        """Графік оптимальної кількості push-ів по tier"""
        # Аналіз конверсії залежно від кількості push-ів
        push_bins = [1, 2, 3, 5, 10, 20, float('inf')]
        push_labels = ['1', '2', '3', '4-5', '6-10', '11-20', '20+']
        
        df['push_bucket'] = pd.cut(df['push_count'], bins=push_bins, labels=push_labels, include_lowest=True)
        
        conversion_by_pushes = df.groupby(['tier', 'push_bucket']).agg({
            'gadid': 'count',
            'has_deposit': 'sum'
        }).reset_index()
        
        conversion_by_pushes['conversion_rate'] = (
            conversion_by_pushes['has_deposit'] / conversion_by_pushes['gadid'] * 100
        )
        
        # Створюємо heatmap
        pivot_data = conversion_by_pushes.pivot(index='push_bucket', 
                                               columns='tier', 
                                               values='conversion_rate')
        
        plt.figure(figsize=(12, 8))
        sns.heatmap(pivot_data, annot=True, fmt='.2f', cmap='RdYlBu_r', 
                   cbar_kws={'label': 'Конверсія в депозити (%)'})
        plt.title('Конверсія залежно від кількості push-ів по Tier-ах', fontweight='bold')
        plt.xlabel('Географічний Tier')
        plt.ylabel('Кількість push-ів')
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.show()
        
        return conversion_by_pushes
    
    def plot_push_timeline(self, df: pd.DataFrame, save_path: str = None):
        """Часова лінія відправки push-ів та конверсій"""
        # Конвертуємо дати
        df['first_push_date'] = pd.to_datetime(df['first_push'])
        
        daily_stats = df.groupby([df['first_push_date'].dt.date, 'ab_group']).agg({
            'gadid': 'count',
            'has_deposit': 'sum'
        }).reset_index()
        
        fig = px.line(daily_stats, x='first_push_date', y='gadid', 
                     color='ab_group', title='Динаміка відправки push-ів по днях')
        fig.update_layout(xaxis_title='Дата', yaxis_title='Кількість користувачів')
        
        if save_path:
            fig.write_html(save_path)
        fig.show()
    
    def create_summary_dashboard(self, ab_stats: pd.DataFrame, geo_stats: pd.DataFrame):
        """Створює підсумковий dashboard"""
        fig = make_subplots(
            rows=3, cols=2,
            subplot_titles=['A/B групи: Конверсія', 'A/B групи: Користувачі',
                          'Tier: Конверсія', 'Tier: Середні push-и',
                          'Загальна статистика', 'ROI по групах'],
            specs=[[{"type": "bar"}, {"type": "bar"}],
                   [{"type": "bar"}, {"type": "bar"}], 
                   [{"type": "table"}, {"type": "bar"}]]
        )
        
        # A/B конверсія
        fig.add_trace(go.Bar(x=ab_stats.index, y=ab_stats['deposit_conversion'],
                            name='Депозити', marker_color='coral'),
                     row=1, col=1)
        
        # A/B користувачі
        fig.add_trace(go.Bar(x=ab_stats.index, y=ab_stats['total_users'],
                            name='Користувачі', marker_color='lightblue'),
                     row=1, col=2)
        
        # Tier конверсія
        tier_summary = geo_stats.groupby('tier')['conversion_rate'].mean()
        fig.add_trace(go.Bar(x=tier_summary.index, y=tier_summary.values,
                            name='Конверсія', marker_color='lightgreen'),
                     row=2, col=1)
        
        # Tier push-и
        tier_pushes = geo_stats.groupby('tier')['push_count'].mean()
        fig.add_trace(go.Bar(x=tier_pushes.index, y=tier_pushes.values,
                            name='Push-и', marker_color='gold'),
                     row=2, col=2)
        
        fig.update_layout(height=1200, showlegend=False,
                         title_text="Підсумковий Dashboard Аналізу Push-сповіщень")
        fig.show()
        
        return fig