from dataclasses import dataclass, field, asdict
from typing import Optional, Dict, Any, List
import re

@dataclass
class ResidentialComplex:
    name: Optional[str] = None
    developer: Optional[str] = None
    completion_year: Optional[int] = None
    completion_quarter: Optional[int] = None # 1, 2, 3, 4
    enclosed_area: Optional[bool] = None     # Закрытая территория
    security: Optional[bool] = None          # Охрана
    parking_complex: Optional[str] = None    # Тип паркинга в ЖК (подземный, наземный и т.д.)
    infrastructure_features: Optional[List[str]] = field(default_factory=list) # Особенности инфраструктуры ЖК

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class DistrictInfo:
    region_name: str
    city_name: Optional[str] = None
    district_name: Optional[str] = None
    population: Optional[int] = None
    avg_price_per_sqm: Optional[float] = None              # Средняя цена за кв.м в районе
    schools_count: Optional[int] = None                     # Количество школ в районе
    hospitals_count: Optional[int] = None                   # Количество больниц или крупных медицинских учреждений в районе
    crime_rate: Optional[float] = None                      # Уровень преступности в районе (например, на 1000 жителей, или индекс)
    metro_distance: Optional[float] = None                  # Расстояние до метро в км
    public_transport_accessibility: Optional[float] = None  # Индекс доступности общественного транспорта
    green_area_percentage: Optional[float] = None           # Процент зеленых зон
    commercial_density: Optional[float] = None              # Плотность коммерческой недвижимости/объектов

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class EconomicData:
    region_name: str
    avg_life_expectancy: Optional[float] = None         # Средняя продолжительность жизни
    key_interest_rate: Optional[float] = None           # Ключевая ставка
    credit_approval_rate: Optional[float] = None        # Процент одобрения кредитов
    avg_earnings: Optional[float] = None                # Средний заработок
    gdp_per_capita: Optional[float] = None              # ВВП на душу населения
    unemployment_rate: Optional[float] = None           # Уровень безработицы

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class PosterData:
    """
    Основной dataclass для хранения всей информации об объекте недвижимости.
    ИСПРАВЛЕНО: Все обязательные поля идут ПЕРЕД опциональными
    """
    # Обязательные поля (БЕЗ значений по умолчанию)
    id: str                                     # Уникальный идентификатор объявления
    url: str                                    # URL объявления  
    section: str                                # purchase/rent
    property_type: str                          # house/flat
    
    # Опциональные поля (СО значениями по умолчанию)
    _id: Optional[str] = field(default=None, repr=False) # MongoDB-специфичное поле
    
    price: Optional[int] = None                 # Цена в рублях
    address: Optional[str] = None               # Полный адрес объекта
    area_total: Optional[float] = None          # Общая площадь в кв метрах
    rooms: Optional[int] = None                 # Количество комнат (студия - 0)
    floor: Optional[int] = None                 # Этаж
    building_total_floors: Optional[int] = None # Этажей в доме
    description: Optional[str] = None           # Описание объявления
    balcony: Optional[bool] = False             # Наличие балкона/лоджии

    kitchen_area: Optional[float] = None        # Площадь кухни в кв метрах
    living_area: Optional[float] = None         # Жилая площадь в кв метрах
    year_built: Optional[int] = None            # Год постройки
    repair_type: Optional[str] = None           # Тип ремонта (дизайнерский, евро, косметический, без ремонта)
    building_type: Optional[str] = None         # Тип дома (панельный, кирпичный, монолитный и т.д.)
    parking: Optional[bool] = False             # Наличие парковки (подразумевает любую, кроме паркинга ЖК)
    elevator: Optional[bool] = False            # Наличие лифта
    image_urls: Optional[List[str]] = field(default_factory=list) # Список URL изображений
    coordinates: Optional[Dict[str, float]] = None # Географические координаты {"latitude": ..., "longitude": ...}

    # Вложенные dataclass
    residential_complex: Optional[ResidentialComplex] = None
    district_info: Optional[DistrictInfo] = None
    economic_data: Optional[EconomicData] = None

    def __post_init__(self):
        # Приведение типов и очистка данных после инициализации
        if self.price is not None:
            try:
                self.price = int(re.sub(r'\D', '', str(self.price)))
            except (ValueError, TypeError):
                self.price = None

        if self.area_total is not None:
            try:
                self.area_total = float(str(self.area_total).replace(',', '.'))
            except (ValueError, TypeError):
                self.area_total = None

        # Пример для rooms, если парсер возвращает строку
        if isinstance(self.rooms, str):
            if "студия" in self.rooms.lower():
                self.rooms = 0
            else:
                match = re.search(r'(\d+)-комн', self.rooms)
                if match:
                    self.rooms = int(match.group(1))
                else:
                    self.rooms = None
        
        # Если residential_complex - это словарь, пытаемся создать объект
        if isinstance(self.residential_complex, dict):
            try:
                self.residential_complex = ResidentialComplex(**self.residential_complex)
            except TypeError as e:
                print(f"Ошибка при создании ResidentialComplex из словаря: {e} -> {self.residential_complex}")
                self.residential_complex = None
        
        if isinstance(self.district_info, dict):
            try:
                self.district_info = DistrictInfo(**self.district_info)
            except TypeError as e:
                print(f"Ошибка при создании DistrictInfo из словаря: {e} -> {self.district_info}")
                self.district_info = None

        if isinstance(self.economic_data, dict):
            try:
                self.economic_data = EconomicData(**self.economic_data)
            except TypeError as e:
                print(f"Ошибка при создании EconomicData из словаря: {e} -> {self.economic_data}")
                self.economic_data = None

    def to_dict(self) -> Dict[str, Any]:
        data_dict = asdict(self)

        if data_dict.get('_id') is None:
            data_dict.pop('_id', None)
            
        return data_dict


# Добавляем enum для совместимости (если используется в улучшенной версии)
from enum import Enum

class ProcessingStatus(Enum):
    PENDING = "pending"
    PARSING = "parsing"
    ENRICHING = "enriching"
    ANALYZING = "analyzing"
    COMPLETED = "completed"
    FAILED = "failed"