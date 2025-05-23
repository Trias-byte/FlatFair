from dataclasses import dataclass

@dataclass
class FlatData:
    type_deal: str
    price: int
    area_total: float
    num_rooms: int
    total_floors: int
    city: str
    address_raw: str
    year_built: int = None
    has_balcony: bool = False
    flat_desc_for_nlp: str = ''

@dataclass
class HouseData:
    city: str
    address: str
    