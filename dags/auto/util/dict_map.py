from typing import Dict, Generic, TypeVar, Optional, Iterable, Tuple

from pydantic import BaseModel

# from pydantic import BaseModel, field_validator, ConfigDict

K = TypeVar("K")
V = TypeVar("V")


class DictMap(BaseModel, Generic[K, V]):
    _map: Dict[K, V] = {}

    def clear(self) -> None:
        self._map.clear()

    def has(self, key: K) -> bool:
        return key in self._map

    def get(self, key: K) -> Optional[V]:
        return self._map.get(key)

    def add(self, key: K, val: V) -> None:
        self._map[key] = val

    def remove(self, key: K) -> None:
        self._map.pop(key, None)

    def get_map(self) -> Dict[K, V]:
        return self._map

    def get_items(self) -> Iterable[Tuple[K, V]]:
        return self._map.items()

    def get_connector(self, flow_id, alias) -> Optional[V]:
        return self.get(alias)
