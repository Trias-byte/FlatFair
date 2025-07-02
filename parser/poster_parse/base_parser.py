from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from bs4 import BeautifulSoup

class BaseParser(ABC):
    _SELECTORS: Dict[str, str] = {}
    _KEYWORDS: Dict[str, List[str]] = {}

    @abstractmethod
    def parse(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Абстрактный метод для парсинга HTML-страницы и извлечения данных объявления.
        Каждый конкретный парсер должен реализовать этот метод.

        Args:
            soup (BeautifulSoup): Объект BeautifulSoup, представляющий разобранный HTML страницы.

        Returns:
            Dict[str, Any]: Словарь с распарсенными данными. Ключи словаря
                            должны соответствовать полям PosterData.
                            Например: {"price": "10000000", "address": "ул. Пушкина, 1"}
        """
        pass

    def _get_text(self, soup: BeautifulSoup, selector: str) -> Optional[str]:
        """
        Вспомогательный метод для извлечения текстового содержимого элемента по CSS-селектору.

        Args:
            soup (BeautifulSoup): Объект BeautifulSoup.
            selector (str): CSS-селектор для поиска элемента.

        Returns:
            Optional[str]: Текстовое содержимое элемента или None, если элемент не найден.
        """
        element = soup.select_one(selector)
        return element.get_text(strip=True) if element else None

    def _get_attribute(self, soup: BeautifulSoup, selector: str, attr: str) -> Optional[str]:
        """
        Вспомогательный метод для извлечения значения атрибута элемента по CSS-селектору.

        Args:
            soup (BeautifulSoup): Объект BeautifulSoup.
            selector (str): CSS-селектор для поиска элемента.
            attr (str): Имя атрибута для извлечения (например, 'src', 'href', 'data-value').

        Returns:
            Optional[str]: Значение атрибута элемента или None, если элемент не найден
                           или атрибут отсутствует.
        """
        element = soup.select_one(selector)
        return element.get(attr) if element else None

    def _find_by_partial_text(self, soup: BeautifulSoup, tag: str, partial_text: str) -> Optional[str]:
        """
        Вспомогательный метод для поиска элемента по частичному совпадению текста.
        Полезно для поиска характеристик, которые могут быть в списке без явных селекторов.

        Args:
            soup (BeautifulSoup): Объект BeautifulSoup.
            tag (str): Тег элемента для поиска (например, 'li', 'div', 'span').
            partial_text (str): Часть текста, которую нужно найти в элементе.

        Returns:
            Optional[str]: Полный текст найденного элемента или None, если не найден.
        """
        for element in soup.find_all(tag):
            if partial_text.lower() in element.get_text(strip=True).lower():
                return element.get_text(strip=True)
        return None
    
    def _check_keyword_presence(self, text: Optional[str], keywords: List[str]) -> bool:
        """
        Проверяет наличие любого из ключевых слов в заданном тексте (без учета регистра).

        Args:
            text (Optional[str]): Текст для проверки.
            keywords (List[str]): Список ключевых слов для поиска.

        Returns:
            bool: True, если хотя бы одно ключевое слово найдено, иначе False.
        """
        if text is None:
            return False
        text_lower = text.lower()
        return any(keyword.lower() in text_lower for keyword in keywords)