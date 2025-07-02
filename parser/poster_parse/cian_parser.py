from base_parser import BaseParser
from bs4 import BeautifulSoup
from typing import Dict, Any, List, Optional
from posterData import *
import re
import json 

class CianFlatRentParser(BaseParser):
    """
    Парсер для детальных страниц объявлений об аренде квартир на Циане.
    Обновлен для использования нового блока OfferSummaryInfoGroup
    и для извлечения информации о Жилом Комплексе.
    """
    _SELECTORS: Dict[str, str] = {
        "price": "div[data-testid='price-amount']", 
        "address": "div[data-name='AddressContainer']", 
        "description": "div[data-name='Description'] div",
        
        "summary_info_value_template": "div[data-name='OfferSummaryInfoItem']:has(p.a10a3f92e9--color_gray60_100--r_axa:contains('{}')) p.a10a3f92e9--color_text-primary-default--vSRPB",
        "factoids_info_value_template": "div[data-name='ObjectFactoidsItem']:has(span.a10a3f92e9--color_gray60_100--r_axa:contains('{}')) span[style*='letter-spacing']",

        "rooms": "h1[class='a10a3f92e9--title--vlZwT']",
        "image_urls": "img.a10a3f92e9--image--d_x2i",

        # Селекторы для метро
        "metro_stations": "div[data-name='MetroInfo'] div.a10a3f92e9--content--_fN_7 span.a10a3f92e9--name--P_y5b", # Название станции
        "metro_time": "div[data-name='MetroInfo'] div.a10a3f92e9--content--_fN_7 span.a10a3f92e9--time--_pW7k", # Время до станции
        "metro_transport": "div[data-name='MetroInfo'] div.a10a3f92e9--content--_fN_7 span.a10a3f92e9--type--o4kL4", # Тип транспорта (пешком/транспортом)

        # Общий список характеристик для поиска ключевых слов (для тех, что не извлекаются напрямую)
        "features_list_items": "div.a10a3f92e9--container--P010w div.a10a3f92e9--item--_NP3B",
        "additional_features_block": "div.a10a3f92e9--container--P010w", 

        # Новые селекторы для информации о ЖК
        "complex_name_selector": "div[data-name='ComplexHeader'] h2 a", 
        "complex_features_block": "div[data-name='ComplexFeatures']",
    }

    _KEYWORDS: Dict[str, List[str]] = {
        "parking": ["парковка", "машиноместо"],
        "building_type_keywords": ["панельный", "кирпичный", "монолитный", "блочный", "деревянный"], # Переименовал для ясности
        
        # Ключевые слова для характеристик ЖК
        "complex_enclosed_area_keywords": ["закрытая территория", "огороженная территория"],
        "complex_security_keywords": ["охрана", "видеонаблюдение", "консьерж"],
        "complex_parking_types_keywords": ["подземный паркинг", "многоуровневая парковка", "гостевая парковка"],
        "complex_infrastructure_keywords": ["школа", "детский сад", "детская площадка", "спортивная площадка", "магазины", "супермаркет", "кафе", "ресторан", "фитнес-центр", "поликлиника", "аптека"],
    }

    def _get_info_from_summary_or_factoids(self, soup: BeautifulSoup, label: str) -> Optional[str]:
        """
        Вспомогательный метод для получения текста информации,
        приоритетно из блока OfferSummaryInfoItem, затем из ObjectFactoidsItem.
        """
        selector_summary = self._SELECTORS["summary_info_value_template"].format(label)
        text = self._get_text(soup, selector_summary)
        if text:
            return text
        
        selector_factoids = self._SELECTORS["factoids_info_value_template"].format(label)
        text = self._get_text(soup, selector_factoids)
        return text

    def _extract_and_clean_area(self, raw_text: Optional[str]) -> Optional[float]:
        """
        Вспомогательный метод для извлечения, очистки и преобразования площади в float.
        """
        if raw_text:
            cleaned_text = re.sub(r'[^\d,.]', '', raw_text).replace(',', '.').strip()
            cleaned_text = cleaned_text.replace(' ', '')
            try:
                return float(cleaned_text)
            except ValueError:
                return None
        return None

    def _extract_and_clean_price(self, soup: BeautifulSoup, selector: str) -> Optional[int]:
        """
        Вспомогательный метод для извлечения, очистки и преобразования цены в int.
        """
        text = self._get_text(soup, selector)
        if text:
            cleaned_text = re.sub(r'[^\d]', '', text).strip()
            try:
                return int(cleaned_text)
            except ValueError:
                return None
        return None

    def parse(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Парсит HTML-содержимое страницы объявления об аренде квартиры на Циане
        и возвращает словарь с извлеченными данными.
        """
        data: Dict[str, Any] = {}

        # 1. Основные поля, извлекаемые напрямую по селекторам
        data['price'] = self._extract_and_clean_price(soup, self._SELECTORS["price"])
        data['address'] = self._get_text(soup, self._SELECTORS["address"])
        data['description'] = self._get_text(soup, self._SELECTORS["description"])
        
        # 2. Площади и их очистка (используем новый вспомогательный метод _get_info_from_summary_or_factoids)
        data['area_total'] = self._extract_and_clean_area(self._get_info_from_summary_or_factoids(soup, 'Общая площадь'))
        data['kitchen_area'] = self._extract_and_clean_area(self._get_info_from_summary_or_factoids(soup, 'Площадь кухни'))
        data['living_area'] = self._extract_and_clean_area(self._get_info_from_summary_or_factoids(soup, 'Жилая площадь'))
        
        # 3. Обработка комнат (Улучшенная логика)
        rooms_raw = self._get_text(soup, self._SELECTORS["rooms"])
        if rooms_raw:
            rooms_raw_lower = rooms_raw.lower()
            
            russian_num_words = {
                'одно': 1, 'двух': 2, 'трех': 3, 'четырех': 4, 'пяти': 5,
                'шести': 6, 'семи': 7, 'восьми': 8, 'девяти': 9, 'десяти': 10
            }

            patterns_to_match = [
                (r'\bстудия\b', 'fixed_0'),                                  
                (r'(\d+)-комн\.|\b(\d+)\s*комн(?:\.|ат)?\b', 'num_group'),
                (r'\bевро(двух|трех|четырех|пяти|шести|семи|восьми|девяти|десяти)шка\b', 'euro_num'),
                (r'\b(одно|двух|трех|четырех|пяти|шести|семи|восьми|девяти|десяти)комнатная\b', 'word_num'),
                (r'\bкомната\b', 'fixed_1')
            ]

            data['rooms'] = None 

            for pattern, handler_type in patterns_to_match:
                match = re.search(pattern, rooms_raw_lower)
                if match:
                    if handler_type == 'fixed_0':
                        data['rooms'] = 0
                    elif handler_type == 'fixed_1':
                        data['rooms'] = 1
                    elif handler_type == 'num_group':
                        data['rooms'] = int(match.group(1)) if match.group(1) else int(match.group(2))
                    elif handler_type == 'euro_num':
                        num_word = match.group(1)
                        data['rooms'] = russian_num_words.get(num_word, None) + 1 if russian_num_words.get(num_word) else None
                    elif handler_type == 'word_num':
                        num_word = match.group(1)
                        data['rooms'] = russian_num_words.get(num_word, None)
                    break 

        # 4. Обработка этажа и общей этажности здания (используем новый вспомогательный метод)
        floor_info_raw = self._get_info_from_summary_or_factoids(soup, 'Этаж')
        if floor_info_raw:
            match = re.search(r'(\d+)\s+из\s+(\d+)', floor_info_raw)
            if match:
                data['floor'] = int(match.group(1))
                data['building_total_floors'] = int(match.group(2))
            else: 
                floor_match = re.search(r'(\d+)\s+этаж', floor_info_raw)
                if floor_match:
                    data['floor'] = int(floor_match.group(1))

        # 5. Год постройки (используем новый вспомогательный метод)
        year_built_raw = self._get_info_from_summary_or_factoids(soup, 'Год постройки')
        if year_built_raw:
            match = re.search(r'\d{4}', year_built_raw)
            if match:
                data['year_built'] = int(match.group(0))

        # 6. Новые поля из OfferSummaryInfoItem (извлекаем напрямую)
        data['sanuzel'] = self._get_info_from_summary_or_factoids(soup, 'Санузел')
        data['view_from_windows'] = self._get_info_from_summary_or_factoids(soup, 'Вид из окон')

        # 7. Балкон/лоджия (прямое извлечение из OfferSummaryInfoItem, а не по ключевым словам)
        balcony_raw = self._get_info_from_summary_or_factoids(soup, 'Балкон/лоджия')
        data['balcony'] = bool(balcony_raw and 'нет' not in balcony_raw.lower()) 

        # 8. Тип ремонта (прямое извлечение из OfferSummaryInfoItem, а не по ключевым словам)
        repair_type_raw = self._get_info_from_summary_or_factoids(soup, 'Ремонт')
        data['repair_type'] = repair_type_raw if repair_type_raw and repair_type_raw.lower() != 'нет' else None

        # 9. Информация о метро
        metro_data = []
        metro_blocks = soup.select("div[data-name='MetroInfo']")
        for block in metro_blocks:
            station_name = self._get_text(block, self._SELECTORS["metro_stations"])
            time_to_metro = self._get_text(block, self._SELECTORS["metro_time"])
            transport_type = self._get_text(block, self._SELECTORS["metro_transport"])

            if station_name:
                metro_info = {"station": station_name}
                if time_to_metro:
                    time_match = re.search(r'(\d+)\s*мин', time_to_metro)
                    if time_match:
                        metro_info["time_min"] = int(time_match.group(1))
                    metro_info["transport_type"] = transport_type.strip() if transport_type else None
                metro_data.append(metro_info)
        data['metro_info'] = metro_data

        # 10. Булевы и категориальные поля (лифт, тип здания, парковка) - ОБНОВЛЕННАЯ ЛОГИКА
        
        # Для лифта:
        elevator_raw = self._get_info_from_summary_or_factoids(soup, 'Количество лифтов')
        if elevator_raw and 'нет информации' not in elevator_raw.lower():
            data['elevator'] = True
        else:
            data['elevator'] = False 
        
        # Для типа здания:
        building_type_raw = self._get_info_from_summary_or_factoids(soup, 'Строительная серия')
        if building_type_raw and 'нет информации' not in building_type_raw.lower():
            data['building_type'] = building_type_raw.strip()
        else:
            # Запасной вариант: поиск по ключевым словам, если прямое извлечение не дало результата
            features_text_for_keywords = ""
            feature_elements = soup.select(self._SELECTORS["features_list_items"])
            for el in feature_elements:
                features_text_for_keywords += el.get_text(strip=True).lower() + " "
            
            additional_features_block = soup.select_one(self._SELECTORS["additional_features_block"])
            if additional_features_block:
                features_text_for_keywords += additional_features_block.get_text(strip=True).lower() + " "
            
            description_text_lower = str(data.get('description', '')).lower()
            full_text_for_keywords = features_text_for_keywords + description_text_lower
            data['building_type'] = next((k for k in self._KEYWORDS["building_type_keywords"] if k in full_text_for_keywords), None)

        # Для парковки:
        parking_raw = self._get_info_from_summary_or_factoids(soup, 'Парковка') or \
                      self._get_info_from_summary_or_factoids(soup, 'Паркинг')
        
        if parking_raw and 'нет информации' not in parking_raw.lower() and 'нет' not in parking_raw.lower():
            data['parking'] = True
        else:
            # Запасной вариант: поиск по ключевым словам
            features_text_for_keywords = "" # Переинициализируем для ясности
            feature_elements = soup.select(self._SELECTORS["features_list_items"])
            for el in feature_elements:
                features_text_for_keywords += el.get_text(strip=True).lower() + " "
            
            additional_features_block = soup.select_one(self._SELECTORS["additional_features_block"])
            if additional_features_block:
                features_text_for_keywords += additional_features_block.get_text(strip=True).lower() + " "
            
            description_text_lower = str(data.get('description', '')).lower()
            full_text_for_keywords = features_text_for_keywords + description_text_lower
            data['parking'] = self._check_keyword_presence(full_text_for_keywords, self._KEYWORDS["parking"])


        # 11. URL изображений
        image_elements = soup.select(self._SELECTORS["image_urls"])
        image_urls = []
        for img in image_elements:
            src = img.get('src') or img.get('data-src')
            if src and src.startswith('http'):
                image_urls.append(src)
        data['image_urls'] = image_urls

        # 12. Координаты (извлечение из JSON-Ld скрипта)
        script_tags = soup.find_all('script', type='application/ld+json')
        for script in script_tags:
            try:
                json_data = json.loads(script.string)
                if '@type' in json_data and json_data['@type'] == 'Place':
                    if 'geo' in json_data:
                        data['latitude'] = float(json_data['geo']['latitude'])
                        data['longitude'] = float(json_data['geo']['longitude'])
                        break
                elif '@type' in json_data and (json_data['@type'] == 'Offer' or json_data['@type'] == 'Product'):
                    if 'itemOffered' in json_data and 'geo' in json_data['itemOffered']:
                        data['latitude'] = float(json_data['itemOffered']['geo']['latitude'])
                        data['longitude'] = float(json_data['itemOffered']['geo']['longitude'])
                        break
            except json.JSONDecodeError:
                continue
            except KeyError:
                continue
        
        # 13. Информация о Жилом комплексе (ЖК)
        complex_instance = ResidentialComplex()

        # 13.1 Название ЖК
        complex_name = self._get_text(soup, self._SELECTORS["complex_name_selector"])
        if complex_name:
            complex_instance.name = complex_name

        # 13.2 Застройщик (Developer) - используем общий метод для summary/factoids
        developer_raw = self._get_info_from_summary_or_factoids(soup, 'Застройщик')
        if developer_raw and 'нет информации' not in developer_raw.lower():
            complex_instance.developer = developer_raw.strip()

        # 13.3 Срок сдачи (Completion Year & Quarter) - используем общий метод для summary/factoids
        completion_raw = self._get_info_from_summary_or_factoids(soup, 'Срок сдачи')
        if completion_raw and 'нет информации' not in completion_raw.lower():
            match_quarter_year = re.search(r'(\d+)\s*кв\.\s*(\d{4})', completion_raw, re.IGNORECASE)
            match_year_only = re.search(r'(\d{4})', completion_raw)

            if match_quarter_year:
                try:
                    complex_instance.completion_quarter = int(match_quarter_year.group(1))
                    complex_instance.completion_year = int(match_quarter_year.group(2))
                except ValueError:
                    pass
            elif match_year_only:
                try:
                    complex_instance.completion_year = int(match_year_only.group(1))
                except ValueError:
                    pass

        # 13.4 Особенности ЖК (Закрытая территория, Охрана, Тип парковки, Инфраструктура)
        # Собираем весь текст из потенциальных блоков фичей ЖК и описания
        complex_features_text_combined = ""
        complex_features_block_element = soup.select_one(self._SELECTORS.get("complex_features_block"))
        if complex_features_block_element:
            complex_features_text_combined += complex_features_block_element.get_text(strip=True).lower() + " "
        
        # Также ищем в общем описании, если что-то не найдено в специальном блоке
        complex_features_text_combined += str(data.get('description', '')).lower() + " "

        complex_instance.enclosed_area = self._check_keyword_presence(complex_features_text_combined, self._KEYWORDS["complex_enclosed_area_keywords"])
        complex_instance.security = self._check_keyword_presence(complex_features_text_combined, self._KEYWORDS["complex_security_keywords"])

        parking_complex_type = None
        for keyword in self._KEYWORDS["complex_parking_types_keywords"]:
            if keyword in complex_features_text_combined:
                parking_complex_type = keyword # Берем первое совпадение
                break
        complex_instance.parking_complex = parking_complex_type

        infrastructure_list = []
        for keyword in self._KEYWORDS["complex_infrastructure_keywords"]:
            if keyword in complex_features_text_combined:
                infrastructure_list.append(keyword)
        if infrastructure_list:
            complex_instance.infrastructure_features = infrastructure_list
        
        # Добавляем инстанс ЖК в данные, если хоть какое-то поле ЖК заполнено
        if any(getattr(complex_instance, field.name) is not None for field in complex_instance.__dataclass_fields__.values()):
            data['residential_complex'] = complex_instance

        return data
    """
    Парсер для детальных страниц объявлений об аренде квартир на Циане.
    Обновлен для использования нового блока OfferSummaryInfoGroup.
    """
    _SELECTORS: Dict[str, str] = {
        "price": "div[data-testid='price-amount']", 
        "address": "div[data-name='AddressContainer']", 
        "description": "div[data-name='Description'] div",
        
        "summary_info_value_template": "div[data-name='OfferSummaryInfoItem']:has(p.a10a3f92e9--color_gray60_100--r_axa:contains('{}')) p.a10a3f92e9--color_text-primary-default--vSRPB",
        "factoids_info_value_template": "div[data-name='ObjectFactoidsItem']:has(span.a10a3f92e9--color_gray60_100--r_axa:contains('{}')) span[style*='letter-spacing']",

        "rooms": "h1[class='a10a3f92e9--title--vlZwT']", 
        "image_urls": "img.a10a3f92e9--image--d_x2i", 

        # Селекторы для метро
        "metro_stations": "div[data-name='MetroInfo'] div.a10a3f92e9--content--_fN_7 span.a10a3f92e9--name--P_y5b", # Название станции
        "metro_time": "div[data-name='MetroInfo'] div.a10a3f92e9--content--_fN_7 span.a10a3f92e9--time--_pW7k", # Время до станции
        "metro_transport": "div[data-name='MetroInfo'] div.a10a3f92e9--content--_fN_7 span.a10a3f92e9--type--o4kL4", # Тип транспорта (пешком/транспортом)

        # Общий список характеристик для поиска ключевых слов (для тех, что не извлекаются напрямую)
        "features_list_items": "div.a10a3f92e9--container--P010w div.a10a3f92e9--item--_NP3B",
        "additional_features_block": "div.a10a3f92e9--container--P010w",
        "complex_name_selector": "div[data-name='ComplexHeader'] h2 a", 
        "complex_features_block": "div[data-name='ComplexFeatures']",
    }

    _KEYWORDS: Dict[str, List[str]] = {
        "parking": ["парковка", "машиноместо"],
        "building_type_keywords": ["панельный", "кирпичный", "монолитный", "блочный", "деревянный"],
        "complex_enclosed_area_keywords": ["закрытая территория", "огороженная территория"],
        "complex_security_keywords": ["охрана", "видеонаблюдение", "консьерж"],
        "complex_parking_types_keywords": ["подземный паркинг", "многоуровневая парковка", "гостевая парковка"],
        "complex_infrastructure_keywords": ["школа", "детский сад", "детская площадка", "спортивная площадка", "магазины", "супермаркет", "кафе", "ресторан", "фитнес-центр", "поликлиника", "аптека"]
    }

    def _get_info_from_summary_or_factoids(self, soup: BeautifulSoup, label: str) -> Optional[str]:
        """
        Вспомогательный метод для получения текста информации,
        приоритетно из блока OfferSummaryInfoItem, затем из ObjectFactoidsItem.
        """
        selector_summary = self._SELECTORS["summary_info_value_template"].format(label)
        text = self._get_text(soup, selector_summary)
        if text:
            return text
        
        selector_factoids = self._SELECTORS["factoids_info_value_template"].format(label)
        text = self._get_text(soup, selector_factoids)
        return text

    def _extract_and_clean_area(self, raw_text: Optional[str]) -> Optional[float]:
        """
        Вспомогательный метод для извлечения, очистки и преобразования площади в float.
        """
        if raw_text:
            cleaned_text = re.sub(r'[^\d,.]', '', raw_text).replace(',', '.').strip()
            cleaned_text = cleaned_text.replace(' ', '')
            try:
                return float(cleaned_text)
            except ValueError:
                return None
        return None

    def _extract_and_clean_price(self, soup: BeautifulSoup, selector: str) -> Optional[int]:
        """
        Вспомогательный метод для извлечения, очистки и преобразования цены в int.
        """
        text = self._get_text(soup, selector)
        if text:
            cleaned_text = re.sub(r'[^\d]', '', text).strip()
            try:
                return int(cleaned_text)
            except ValueError:
                return None
        return None

    def parse(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Парсит HTML-содержимое страницы объявления об аренде квартиры на Циане
        и возвращает словарь с извлеченными данными.
        """
        data: Dict[str, Any] = {}

        # 1. Основные поля, извлекаемые напрямую по селекторам
        data['price'] = self._extract_and_clean_price(soup, self._SELECTORS["price"])
        data['address'] = self._get_text(soup, self._SELECTORS["address"])
        data['description'] = self._get_text(soup, self._SELECTORS["description"])
        
        # 2. Площади и их очистка (используем новый вспомогательный метод _get_info_from_summary_or_factoids)
        data['area_total'] = self._extract_and_clean_area(self._get_info_from_summary_or_factoids(soup, 'Общая площадь'))
        data['kitchen_area'] = self._extract_and_clean_area(self._get_info_from_summary_or_factoids(soup, 'Площадь кухни'))
        data['living_area'] = self._extract_and_clean_area(self._get_info_from_summary_or_factoids(soup, 'Жилая площадь'))
        
        # 3. Обработка комнат (Улучшенная логика)
        rooms_raw = self._get_text(soup, self._SELECTORS["rooms"])
        if rooms_raw:
            rooms_raw_lower = rooms_raw.lower()
            
            # Helper to map Russian words for numbers to digits (for "однокомнатная", "евродвушка" etc.)
            russian_num_words = {
                'одно': 1, 'двух': 2, 'трех': 3, 'четырех': 4, 'пяти': 5,
                'шести': 6, 'семи': 7, 'восьми': 8, 'девяти': 9, 'десяти': 10
            }

            # Define patterns in order of priority (more specific/direct first)
            patterns_to_match = [
                (r'\bстудия\b', 'fixed_0'),                                  
                (r'(\d+)-комн\.|\b(\d+)\s*комн(?:\.|ат)?\b', 'num_group'),
                (r'\bевро(двух|трех|четырех|пяти|шести|семи|восьми|девяти|десяти)шка\b', 'euro_num'),
                (r'\b(одно|двух|трех|четырех|пяти|шести|семи|восьми|девяти|десяти)комнатная\b', 'word_num'),
                (r'\bкомната\b', 'fixed_1')
            ]

            data['rooms'] = None 

            for pattern, handler_type in patterns_to_match:
                match = re.search(pattern, rooms_raw_lower)
                if match:
                    if handler_type == 'fixed_0':
                        data['rooms'] = 0
                    elif handler_type == 'fixed_1':
                        data['rooms'] = 1
                    elif handler_type == 'num_group':
                        data['rooms'] = int(match.group(1)) if match.group(1) else int(match.group(2))
                    elif handler_type == 'euro_num':
                        num_word = match.group(1)
                        data['rooms'] = russian_num_words.get(num_word, None) + 1 if russian_num_words.get(num_word) else None
                    elif handler_type == 'word_num':
                        num_word = match.group(1)
                        data['rooms'] = russian_num_words.get(num_word, None)
                    break 

        # 4. Обработка этажа и общей этажности здания (используем новый вспомогательный метод)
        floor_info_raw = self._get_info_from_summary_or_factoids(soup, 'Этаж')
        if floor_info_raw:
            match = re.search(r'(\d+)\s+из\s+(\d+)', floor_info_raw)
            if match:
                data['floor'] = int(match.group(1))
                data['building_total_floors'] = int(match.group(2))
            else: # Если формат "5 этаж", попробуем достать только этаж
                floor_match = re.search(r'(\d+)\s+этаж', floor_info_raw)
                if floor_match:
                    data['floor'] = int(floor_match.group(1))

        # 5. Год постройки (используем новый вспомогательный метод)
        year_built_raw = self._get_info_from_summary_or_factoids(soup, 'Год постройки')
        if year_built_raw:
            match = re.search(r'\d{4}', year_built_raw)
            if match:
                data['year_built'] = int(match.group(0))

        # 6. Новые поля из OfferSummaryInfoItem (извлекаем напрямую)
        data['sanuzel'] = self._get_info_from_summary_or_factoids(soup, 'Санузел')
        data['view_from_windows'] = self._get_info_from_summary_or_factoids(soup, 'Вид из окон')

        # 7. Балкон/лоджия (прямое извлечение из OfferSummaryInfoItem, а не по ключевым словам)
        balcony_raw = self._get_info_from_summary_or_factoids(soup, 'Балкон/лоджия')
        data['balcony'] = bool(balcony_raw and 'нет' not in balcony_raw.lower()) # True, если есть текст и не "нет"

        # 8. Тип ремонта (прямое извлечение из OfferSummaryInfoItem, а не по ключевым словам)
        repair_type_raw = self._get_info_from_summary_or_factoids(soup, 'Ремонт')
        data['repair_type'] = repair_type_raw if repair_type_raw and repair_type_raw.lower() != 'нет' else None

        # 9. Информация о метро
        metro_data = []
        metro_blocks = soup.select("div[data-name='MetroInfo']")
        for block in metro_blocks:
            station_name = self._get_text(block, self._SELECTORS["metro_stations"])
            time_to_metro = self._get_text(block, self._SELECTORS["metro_time"])
            transport_type = self._get_text(block, self._SELECTORS["metro_transport"])

            if station_name:
                metro_info = {"station": station_name}
                if time_to_metro:
                    time_match = re.search(r'(\d+)\s*мин', time_to_metro)
                    if time_match:
                        metro_info["time_min"] = int(time_match.group(1))
                    metro_info["transport_type"] = transport_type.strip() if transport_type else None
                metro_data.append(metro_info)
        data['metro_info'] = metro_data

        # 10. Булевы и категориальные поля (парковка, лифт, тип здания) - ОБНОВЛЕННАЯ ЛОГИКА
        
        # Для лифта:
        elevator_raw = self._get_info_from_summary_or_factoids(soup, 'Количество лифтов')
        if elevator_raw and 'нет информации' not in elevator_raw.lower():
            data['elevator'] = True
        else:
            data['elevator'] = False # По умолчанию False, если не найдено или "Нет информации"
        
        # Для типа здания:
        building_type_raw = self._get_info_from_summary_or_factoids(soup, 'Строительная серия')
        if building_type_raw and 'нет информации' not in building_type_raw.lower():
            data['building_type'] = building_type_raw.strip()
        else:
            # Запасной вариант: поиск по ключевым словам, если прямое извлечение не дало результата
            features_text = ""
            feature_elements = soup.select(self._SELECTORS["features_list_items"])
            for el in feature_elements:
                features_text += el.get_text(strip=True).lower() + " "
            
            additional_features_block = soup.select_one(self._SELECTORS["additional_features_block"])
            if additional_features_block:
                features_text += additional_features_block.get_text(strip=True).lower() + " "
            
            description_text_lower = str(data.get('description', '')).lower()
            full_text_for_keywords = features_text + description_text_lower
            data['building_type'] = next((k for k in self._KEYWORDS["building_type_keywords"] if k in full_text_for_keywords), None)

        # Для парковки:
        parking_raw = self._get_info_from_summary_or_factoids(soup, 'Парковка') or \
                      self._get_info_from_summary_or_factoids(soup, 'Паркинг') # Проверяем оба варианта метки
        
        if parking_raw and 'нет информации' not in parking_raw.lower() and 'нет' not in parking_raw.lower():
            data['parking'] = True
        else:
            # Запасной вариант: поиск по ключевым словам, если прямое извлечение не дало результата
            # (features_text и full_text_for_keywords уже будут заполнены из предыдущего блока)
            features_text = "" # Переинициализируем для ясности, если вдруг не были инициализированы ранее
            feature_elements = soup.select(self._SELECTORS["features_list_items"])
            for el in feature_elements:
                features_text += el.get_text(strip=True).lower() + " "
            
            additional_features_block = soup.select_one(self._SELECTORS["additional_features_block"])
            if additional_features_block:
                features_text += additional_features_block.get_text(strip=True).lower() + " "
            
            description_text_lower = str(data.get('description', '')).lower()
            full_text_for_keywords = features_text + description_text_lower
            data['parking'] = self._check_keyword_presence(full_text_for_keywords, self._KEYWORDS["parking"])

        # 11. URL изображений
        image_elements = soup.select(self._SELECTORS["image_urls"])
        image_urls = []
        for img in image_elements:
            src = img.get('src') or img.get('data-src')
            if src and src.startswith('http'):
                image_urls.append(src)
        data['image_urls'] = image_urls

        # 12. Координаты (извлечение из JSON-Ld скрипта)
        script_tags = soup.find_all('script', type='application/ld+json')
        for script in script_tags:
            try:
                json_data = json.loads(script.string)
                if '@type' in json_data and json_data['@type'] == 'Place':
                    if 'geo' in json_data:
                        data['latitude'] = float(json_data['geo']['latitude'])
                        data['longitude'] = float(json_data['geo']['longitude'])
                        break
                elif '@type' in json_data and (json_data['@type'] == 'Offer' or json_data['@type'] == 'Product'):
                    if 'itemOffered' in json_data and 'geo' in json_data['itemOffered']:
                        data['latitude'] = float(json_data['itemOffered']['geo']['latitude'])
                        data['longitude'] = float(json_data['itemOffered']['geo']['longitude'])
                        break
            except json.JSONDecodeError:
                continue
            except KeyError:
                continue

        return data