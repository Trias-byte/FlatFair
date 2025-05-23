from abc import ABC, abstractmethod
from urllib.parse import urlparse
import re

class UnsupportedSiteError(ValueError):
    def __init__(self, url):
        super().__init__(f"Сайт {url} не поддерживается")

class BaseParser(ABC):
    @abstractmethod
    async def parse(self, url: str) -> dict:
        pass

class ParserFactory:
    _parsers = {
        'cian.ru': 'cian_parser.CianParser',
        'avito.ru': 'avito_parser.AvitoParser'
    }

    @classmethod
    def get_parser(cls, url: str) -> BaseParser:
        """Возвращает парсер для указанного URL"""
        domain = cls._extract_domain(url)
        
        if domain not in cls._parsers:
            raise UnsupportedSiteError(domain)
            
        module_path, class_name = cls._parsers[domain].rsplit('.', 1)
        module = __import__(f'parser.{module_path}', fromlist=[class_name])
        parser_class = getattr(module, class_name)
        return parser_class()

    @abstractmethod
    def _extract_domain(url: str) -> str:
        """Извлекает домен второго уровня"""
        parsed = urlparse(url)
        domain_parts = parsed.netloc.split('.')
        return '.'.join(domain_parts[-2:]) if len(domain_parts) >= 2 else parsed.netloc
    
    @abstractmethod
    def get_city() -> str:
        pass
    
    @abstractmethod
    def get_type_deal() -> str:
        pass
    
    @abstractmethod
    def get_type_property() -> str:
        pass

    @abstractmethod
    def get_price() -> int:
        pass
    
    @abstractmethod
    def get_area_total() -> float:
        pass

    @abstractmethod
    def get_num_rooms() -> int:
        pass

    @abstractmethod
    def get_total_floors() -> int:
        pass
    
    @abstractmethod
    def get_flat_desc_for_nlp() -> str:
        pass