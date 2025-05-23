import httpx
from bs4 import BeautifulSoup
import re
import json
from datetime import datetime

# Предполагаем, что base_parser находится в той же директории или в папке parser
from base_parser import BaseParser, UnsupportedSiteError

# trans = {'flat': 'квартира', 'house': 'дом', 'suburban':'коттедж', 'rent':'аренда', 'sale':'продажа'}

class CianParser(BaseParser):
    BASE_URL = "https://www.cian.ru"

    async def parse(self, url: str) -> dict:
        """
        Асинхронно парсит данные объявления с Cian.ru.
        """
        if not url.startswith(self.BASE_URL):
            raise UnsupportedSiteError(url)

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }

        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(url, headers=headers, follow_redirects=True)
                response.raise_for_status()  # Выбросить исключение для ошибок HTTP (4xx или 5xx)
            except httpx.HTTPStatusError as e:
                print(f"Ошибка HTTP при доступе к {url}: {e.response.status_code}")
                return {"error": f"HTTP Error: {e.response.status_code}"}
            except httpx.RequestError as e:
                print(f"Ошибка запроса при доступе к {url}: {e}")
                return {"error": f"Request Error: {e}"}

        soup = BeautifulSoup(response.text, 'html.parser')
        data = self._extract_data(soup, url) # Передаем URL для определения типа сделки
        
        data['city'] = data.get('city') or self.get_city(soup)
        data['type_deal'] = url.split('/')[3] or ''
        data['type_property'] = url.split('/')[4] or ''
        data['price'] = data.get('price') or self.get_price(soup)
        data['area_total'] = data.get('area_total') or self.get_area_total(soup)
        data['num_rooms'] = data.get('num_rooms') or self.get_num_rooms(soup)
        data['total_floors'] = data.get('total_floors') or self.get_total_floors(soup)
        data['description_raw'] = data.get('description_raw') or self.get_flat_desc_for_nlp(soup)


        return data

    def _extract_data(self, soup: BeautifulSoup, url: str) -> dict:
        """
        Извлекает основные данные из HTML-структуры страницы Циан,
        сначала из JSON-скриптов, затем из HTML как запасной вариант.
        """
        parsed_data = {}

        # 1. Попытка извлечь данные из window._cian_data_ скрипта
        # Это самый богатый источник данных
        cian_data_script = soup.find('script', string=re.compile(r'window\._cian_data_'))
        if cian_data_script:
            try:
                match = re.search(r'window\._cian_data_ = ({.*?});', cian_data_script.string, re.DOTALL)
                if match:
                    cian_data_json_str = match.group(1)
                    cian_data = json.loads(cian_data_json_str)

                    # Путь к данным в offer_data может меняться, но это типичная структура
                    offer_data = cian_data.get('offer', {})
                    flat_data = offer_data.get('flat', {})
                    building_data = flat_data.get('building', {})
                    address_data = flat_data.get('address', {})
                    price_data = flat_data.get('price', {})

                    # Основные характеристики
                    parsed_data['price'] = price_data.get('value')
                    parsed_data['currency'] = price_data.get('currency')
                    parsed_data['area_total'] = flat_data.get('totalArea')
                    parsed_data['num_rooms'] = flat_data.get('roomsCount')
                    parsed_data['floor'] = flat_data.get('floorNumber')
                    parsed_data['total_floors'] = building_data.get('totalFloors')
                    parsed_data['description_raw'] = flat_data.get('description')
                    
                    # Адрес и геоданные
                    parsed_data['address_raw'] = address_data.get('fullAddress')
                    parsed_data['city'] = address_data.get('cityName')
                    parsed_data['region'] = address_data.get('regionName')
                    parsed_data['district'] = address_data.get('districtName')
                    parsed_data['latitude'] = address_data.get('coordinates', {}).get('lat')
                    parsed_data['longitude'] = address_data.get('coordinates', {}).get('lng')

                    # Характеристики здания
                    parsed_data['year_built'] = building_data.get('buildYear')
                    parsed_data['building_type'] = building_data.get('type') # 'Панельный', 'Кирпичный' и т.д.
                    parsed_data['has_elevator'] = building_data.get('hasLift')
                    parsed_data['parking_type'] = building_data.get('parkingType') # 'OPEN', 'UNDERGROUND' и т.д.
                    parsed_data['is_gated_community'] = building_data.get('hasSecurity') # или isClosedTerritory

                    # Характеристики квартиры
                    parsed_data['area_living'] = flat_data.get('livingArea')
                    parsed_data['area_kitchen'] = flat_data.get('kitchenArea')
                    # Суммируем раздельные и совмещенные санузлы
                    separate_wcs = flat_data.get('separateWcsCount', 0)
                    combined_wcs = flat_data.get('combinedWcsCount', 0)
                    parsed_data['num_bathrooms'] = separate_wcs + combined_wcs
                    parsed_data['bathroom_type'] = 'раздельный' if separate_wcs > 0 and combined_wcs == 0 else 'совмещенный' if combined_wcs > 0 else None
                    parsed_data['has_balcony'] = flat_data.get('hasBalcony')
                    parsed_data['has_loggia'] = flat_data.get('hasLoggia')
                    parsed_data['ceiling_height'] = flat_data.get('ceilingHeight')
                    parsed_data['renovation_quality'] = flat_data.get('renovation') # 'Дизайнерский', 'Косметический', 'Без ремонта' и т.д.
                    parsed_data['furniture_status'] = flat_data.get('furniture')
                    parsed_data['appliances_status'] = flat_data.get('appliances')
                    parsed_data['layout_type'] = flat_data.get('layout')
                    parsed_data['window_view'] = flat_data.get('windowView')

                    # Характеристики сделки/объявления
                    parsed_data['date_published'] = flat_data.get('creationDate')
                    if parsed_data['date_published']:
                        try:
                            # Преобразуем строку даты в объект datetime
                            parsed_data['date_published'] = datetime.fromisoformat(parsed_data['date_published'].replace('Z', '+00:00'))
                        except ValueError:
                            parsed_data['date_published'] = None # Если формат не соответствует
                    
                    parsed_data['is_agent_listing'] = flat_data.get('agentOffer')
                    parsed_data['is_owner_listing'] = not flat_data.get('agentOffer') # Если не агент, то собственник
                    parsed_data['num_photos'] = len(flat_data.get('photos', []))
                    parsed_data['has_video'] = bool(flat_data.get('videoUrl'))
                    parsed_data['has_3d_tour'] = bool(flat_data.get('3dTourUrl'))
                    parsed_data['description_length'] = len(parsed_data.get('description_raw', ''))
                    
                    # Признаки инфраструктуры (могут быть глубже в JSON)
                    # Cian data может содержать массив 'clusterData' или 'geo', откуда можно получить станции метро
                    metro_station = flat_data.get('undergrounds', [])
                    if metro_station:
                        parsed_data['metro_station_name'] = metro_station[0].get('name')
                        # 'distance_to_metro_on_site' может быть в minutesToMetro
                        parsed_data['distance_to_metro_on_site'] = metro_station[0].get('minutesToMetro')

                    # Определяем тип сделки по URL
                    parsed_data['type_deal'] = 'продажа' if '/sale/' in url else 'аренда'
                    # Определяем тип недвижимости по заголовку или содержимому flat_data
                    parsed_data['type_property'] = 'апартаменты' if flat_data.get('isApartments') else 'квартира'

            except json.JSONDecodeError:
                print("Не удалось декодировать window._cian_data_.")
            except Exception as e:
                print(f"Ошибка при извлечении из window._cian_data_: {e}")

        # 2. Извлечение данных из JSON-LD скрипта (если window._cian_data_ не дал всего)
        script_ld_json = soup.find('script', {'type': 'application/ld+json'})
        if script_ld_json:
            try:
                json_ld = json.loads(script_ld_json.string)
                if isinstance(json_ld, list):
                    json_ld = next((item for item in json_ld if item.get('@type') == 'Apartment'), json_ld[0] if json_ld else {})
                
                # Дополняем уже спарсенные данные, если они не были найдены
                parsed_data['price'] = parsed_data.get('price') or json_ld.get('offers', {}).get('price')
                parsed_data['area_total'] = parsed_data.get('area_total') or json_ld.get('floorSize', {}).get('value')
                parsed_data['address_raw'] = parsed_data.get('address_raw') or json_ld.get('address', {}).get('streetAddress')
                parsed_data['num_rooms'] = parsed_data.get('num_rooms') or json_ld.get('numberOfRooms')
                parsed_data['description_raw'] = parsed_data.get('description_raw') or json_ld.get('description')
                if 'address' in json_ld and 'addressLocality' in json_ld['address']:
                    parsed_data['city'] = parsed_data.get('city') or json_ld['address']['addressLocality']

            except json.JSONDecodeError:
                print("Не удалось декодировать JSON-LD.")

        # 3. Дополнительный парсинг из HTML, если JSON-скрипты не дали все данные
        # Используем методы BaseParser
        # Эти методы будут вызваны позже в методе parse, но для полноты _extract_data можно и здесь.
        # Однако, лучше их вызывать после _extract_data, чтобы они перекрывали только отсутствующие данные.
        
        return parsed_data

    def _get_info_item_value(self, soup: BeautifulSoup, label: str) -> str | None:
        """
        Извлекает значение характеристики из блока 'OfferSummaryInfoItem'
        по заданному названию (label).
        """
        # Ищем div с data-name="OfferSummaryInfoItem"
        # Внутри него ищем <p> с текстом, соответствующим label
        info_item = soup.find('div', {'data-name': 'OfferSummaryInfoItem'})
        while info_item:
            label_p = info_item.find('p', class_=re.compile(r'a10a3f92e9--color_gray60_100--'), string=label)
            if label_p:
                # Следующий p tag после label_p должен содержать значение
                value_p = label_p.find_next_sibling('p', class_=re.compile(r'a10a3f92e9--color_text-primary-default--'))
                if value_p:
                    return value_p.get_text(strip=True).replace('\xa0', ' ') # Заменяем неразрывный пробел
            info_item = info_item.find_next_sibling('div', {'data-name': 'OfferSummaryInfoItem'})
        return None

    def get_city(self, soup: BeautifulSoup) -> str | None:
        city_element = soup.find('a', class_=re.compile(r'_93444458b6--link--'), string=re.compile(r'Недвижимость в (.*)'))
        if city_element:
            match = re.search(r'Недвижимость в (.*)', city_element.get_text())
            if match:
                return match.group(1).split(',')[0].strip()
        
        meta_locality = soup.find('meta', {'property': 'og:locality'})
        if meta_locality:
            return meta_locality.get('content')
        
        title_tag = soup.find('title')
        if title_tag:
            title_text = title_tag.get_text()
            match = re.search(r'([А-Яа-яЁё\s-]+),\s+р-н', title_text)
            if match:
                return match.group(1).strip()
        
        return None

    def get_type_deal(self, soup: BeautifulSoup) -> str | None:
        title_h1 = soup.find('h1', class_=re.compile(r'.*title--'))
        if title_h1:
            if 'Продажа' in title_h1.get_text():
                return 'продажа'
            elif 'Аренда' in title_h1.get_text():
                return 'аренда'
        return None

    def get_type_property(self, soup: BeautifulSoup) -> str | None:
        title_h1 = soup.find('h1', class_=re.compile(r'.*title--'))
        if title_h1:
            if 'Апартаменты' in title_h1.get_text():
                return 'апартаменты'
            elif 'Квартира' in title_h1.get_text() or 'студии' in title_h1.get_text():
                return 'квартира'
        
        # Используем _get_info_item_value для "Тип жилья"
        housing_type = self._get_info_item_value(soup, "Тип жилья")
        if housing_type:
            if "Новостройка" in housing_type or "Вторичка" in housing_type: # Или другие варианты
                return "квартира" # Или что-то более точное, если нужно различать
        return None

    def get_price(self, soup: BeautifulSoup) -> int | None:
        price_element = soup.find('span', {'itemprop': 'price'})
        if price_element:
            try:
                price_text = price_element.get_text(strip=True).replace(' ', '').replace('₽', '').replace('~', '')
                return int(float(price_text))
            except ValueError:
                pass
        return None
    
    def get_area_total(self, soup: BeautifulSoup) -> float | None:
        # Используем _get_info_item_value для "Общая площадь"
        area_text = self._get_info_item_value(soup, "Общая площадь")
        if area_text:
            try:
                match = re.search(r'\d+(\.\d+)?', area_text.replace(',', '.'))
                if match:
                    return float(match.group(0))
            except (AttributeError, ValueError):
                pass
        return None

    def get_num_rooms(self, soup: BeautifulSoup) -> int | None:
        title = soup.find('h1', class_=re.compile(r'.*title--'))
        if title:
            title_text = title.get_text()
            match = re.search(r'(\d+)-комнатная', title_text)
            if match:
                return int(match.group(1))
            elif 'Студия' in title_text:
                return 0 
        return None

    def get_total_floors(self, soup: BeautifulSoup) -> int | None:
        floor_item = soup.find('li', class_=re.compile(r'.*item--'), string=re.compile(r'Этаж'))
        if floor_item:
            value_span = floor_item.find('span', class_=re.compile(r'.*value--'))
            if value_span:
                try:
                    floor_text = value_span.get_text(strip=True)
                    match = re.search(r'из (\d+)', floor_text)
                    if match:
                        return int(match.group(1))
                except (AttributeError, ValueError):
                    pass
        return None

    def get_flat_desc_for_nlp(self, soup: BeautifulSoup) -> str | None:
        description_element = soup.find('p', itemprop='description')
        if description_element:
            return description_element.get_text(strip=True)
        return None

    def get_living_area(self, soup: BeautifulSoup) -> float | None:
        area_text = self._get_info_item_value(soup, "Жилая площадь")
        if area_text:
            try:
                match = re.search(r'\d+(\.\d+)?', area_text.replace(',', '.'))
                if match:
                    return float(match.group(0))
            except (AttributeError, ValueError):
                pass
        return None
    
    def get_kitchen_area(self, soup: BeautifulSoup) -> float | None:
        area_text = self._get_info_item_value(soup, "Площадь кухни")
        if area_text:
            try:
                match = re.search(r'\d+(\.\d+)?', area_text.replace(',', '.'))
                if match:
                    return float(match.group(0))
            except (AttributeError, ValueError):
                pass
        return None

    def get_ceiling_height(self, soup: BeautifulSoup) -> float | None:
        height_text = self._get_info_item_value(soup, "Высота потолков")
        if height_text:
            try:
                match = re.search(r'\d+(\.\d+)?', height_text.replace(',', '.'))
                if match:
                    return float(match.group(0))
            except (AttributeError, ValueError):
                pass
        return None

    def get_renovation_quality(self, soup: BeautifulSoup) -> str | None:
        return self._get_info_item_value(soup, "Отделка")

    def get_num_elevators(self, soup: BeautifulSoup) -> int | None:
        elevators_text = self._get_info_item_value(soup, "Количество лифтов")
        if elevators_text:
            try:
                match = re.search(r'(\d+)\s+(пассажирских|грузовых|лифтов)', elevators_text)
                if match:
                    return int(match.group(1))
            except (AttributeError, ValueError):
                pass
        return None

    def get_building_type(self, soup: BeautifulSoup) -> str | None:
        return self._get_info_item_value(soup, "Тип дома")
# Пример использования (можно добавить в отдельный скрипт для тестирования)
async def main():
    cian_parser = CianParser()
    # Используйте URL из предоставленного HTML-кода
    url = "https://www.cian.ru/sale/flat/317404201/" # Пример объявления студии в СПб
    
    print(f"Парсинг: {url}")
    data = await cian_parser.parse(url)
    for key, value in data.items():
        print(f"{key}: {value}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())