from datetime import datetime, timedelta

# ЧАСОВІ ПЕРІОДИ
PUSH_START_DATE = '2025-05-22'
PUSH_END_DATE = '2025-05-29' 
CONVERSION_START_DATE = '2025-05-22'
CONVERSION_END_DATE = '2025-06-07'  # +9 днів для конверсій

# ФІЛЬТРИ
ANDROID_TYPE = 1
PUSH_EVENT_TYPE = 7  # SendPush
TARGET_APPS = ['Michelangelo', 'Leonardo', 'Raphael', 'Splinter']

# GEO CLASSIFICATION (коди країн)
TIER_1_COUNTRIES = [
    'US', 'UK', 'CA', 'AU', 'DE', 'FR', 'NL', 
    'SE', 'NO', 'DK', 'CH', 'AT', 'BE', 'FI'
]

TIER_2_COUNTRIES = [
    'ES', 'IT', 'PL', 'BR', 'MX', 'AR', 'CL', 
    'CZ', 'HU', 'SK', 'HR', 'SI', 'EE', 'LV', 'LT'
]

TIER_3_COUNTRIES = [
    'IN', 'ID', 'TH', 'VN', 'PH', 'MY', 'BD', 
    'PK', 'UA', 'RU', 'TR', 'EG', 'ZA', 'NG'
]

# МАППІНГ ПОВНИХ НАЗВ КРАЇН НА КОДИ (для БД statistic)
COUNTRY_NAME_TO_CODE = {
    # Tier 1
    'United States': 'US',
    'United Kingdom': 'UK',
    'Canada': 'CA',
    'Australia': 'AU',
    'Germany': 'DE',
    'France': 'FR',
    'Netherlands': 'NL',
    'Sweden': 'SE',
    'Norway': 'NO',
    'Denmark': 'DK',
    'Switzerland': 'CH',
    'Austria': 'AT',
    'Belgium': 'BE',
    'Finland': 'FI',
    
    # Tier 2
    'Spain': 'ES',
    'Italy': 'IT',
    'Poland': 'PL',
    'Brazil': 'BR',
    'Mexico': 'MX',
    'Argentina': 'AR',
    'Chile': 'CL',
    'Czechia': 'CZ',
    'Czech Republic': 'CZ',  # альтернативна назва
    'Hungary': 'HU',
    'Slovakia': 'SK',
    'Croatia': 'HR',
    'Slovenia': 'SI',
    'Estonia': 'EE',
    'Latvia': 'LV',
    'Lithuania': 'LT',
    
    # Tier 3
    'India': 'IN',
    'Indonesia': 'ID',
    'Thailand': 'TH',
    'Vietnam': 'VN',
    'Philippines': 'PH',
    'Malaysia': 'MY',
    'Bangladesh': 'BD',
    'Pakistan': 'PK',
    'Ukraine': 'UA',
    'Russia': 'RU',
    'Russian Federation': 'RU',  # альтернативна назва
    'Turkey': 'TR',
    'Egypt': 'EG',
    'South Africa': 'ZA',
    'Nigeria': 'NG',
    
    # Додаткові країни які можуть зустрітися в БД
    'Kazakhstan': 'KZ',
    'Venezuela': 'VE',
    'Venezuela, Bolivarian Republic of': 'VE',
    'Côte d\'Ivoire': 'CI',
    'Serbia': 'RS',
    'Bosnia and Herzegovina': 'BA',
    'Albania': 'AL',
    'Bulgaria': 'BG',
    'Romania': 'RO',
    'Moldova': 'MD',
    'Belarus': 'BY',
    'Georgia': 'GE',
    'Armenia': 'AM',
    'Azerbaijan': 'AZ',
    'Morocco': 'MA',
    'Tunisia': 'TN',
    'Algeria': 'DZ',
    'Jordan': 'JO',
    'Lebanon': 'LB',
    'Iran': 'IR',
    'Iraq': 'IQ',
    'Saudi Arabia': 'SA',
    'United Arab Emirates': 'AE',
    'Israel': 'IL',
    'Kenya': 'KE',
    'Ghana': 'GH',
    'Senegal': 'SN',
    'Tanzania': 'TZ',
    'Uganda': 'UG',
    'Ethiopia': 'ET',
    'Colombia': 'CO',
    'Peru': 'PE',
    'Ecuador': 'EC',
    'Uruguay': 'UY',
    'Paraguay': 'PY',
    'Bolivia': 'BO',
    'Costa Rica': 'CR',
    'Panama': 'PA',
    'Guatemala': 'GT',
    'Dominican Republic': 'DO',
    'Cuba': 'CU',
    'Jamaica': 'JM',
    'China': 'CN',
    'Japan': 'JP',
    'South Korea': 'KR',
    'Taiwan': 'TW',
    'Hong Kong': 'HK',
    'Singapore': 'SG',
    'New Zealand': 'NZ'
}

def get_country_tier(country: str) -> str:
    """Визначити tier країни з підтримкою повних назв та кодів"""
    
    # Перевірка на порожні/невідомі значення
    if not country or country in ['Unknown', '', 'NULL', None]:
        return 'Unknown'
    
    # Спробуємо отримати код країни з повної назви
    country_code = COUNTRY_NAME_TO_CODE.get(country, country)
    
    # Визначаємо tier по коду
    if country_code in TIER_1_COUNTRIES:
        return 'Tier 1'
    elif country_code in TIER_2_COUNTRIES:
        return 'Tier 2'
    elif country_code in TIER_3_COUNTRIES:
        return 'Tier 3'
    else:
        return 'Other'