
import requests
from requests.exceptions import RequestException
from bs4 import BeautifulSoup
from typing import Optional, Dict, Any, List
import json
from cian_parser import CianFlatRentParser

def fetch_html_content(url: str) -> Optional[str]:
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=15) # Увеличен таймаут
        response.raise_for_status() 
        return response.text
    except RequestException as e:
        print(f"Ошибка при получении HTML с {url}: {e}")
        return None


if __name__ == "__main__":
    cian_url = "https://spb.cian.ru/rent/flat/305548024/" 
    
    print(f"Попытка получить HTML-код страницы: {cian_url}")
    html_code = fetch_html_content(cian_url)
    
    if html_code:
        print("HTML-код успешно получен. Запускаем парсер...")
        
        soup = BeautifulSoup(html_code, 'html.parser')
        parser = CianFlatRentParser()
        parsed_data = parser.parse(soup)

        print("\n--- Результаты парсинга ---")
        print(json.dumps(parsed_data, indent=4, ensure_ascii=False))

        print("\n--- Проверка заполненных полей PosterData ---")
        expected_fields = [
            'price', 'address', 'area_total', 'rooms', 'floor', 'building_total_floors',
            'description', 'balcony', 'kitchen_area', 'living_area', 'year_built',
            'repair_type', 'building_type', 'parking', 'elevator', 'image_urls'
        ]
        
        for field_name in expected_fields:
            value = parsed_data.get(field_name)
            if value is not None and value != '' and (not isinstance(value, (list, dict)) or value):
                print(f"✅ {field_name}: {value}")
            else:
                print(f"❌ {field_name}: Не заполнен или пуст (Значение: {value})")
    else:
        print(f"Не удалось получить HTML-код страницы с {cian_url}. Парсинг не выполнен.")