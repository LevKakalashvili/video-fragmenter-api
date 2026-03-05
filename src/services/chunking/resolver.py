from src.services.chunking.base import ChunkingStrategy
from src.services.chunking.registry import ChunkingStrategyRegistry


class ChunkingStrategyResolver:
    """Резолвер стратегии фрагментации по-входному `mode`."""

    def __init__(self, registry: ChunkingStrategyRegistry):
        """Инициализирует резолвер ссылкой на реестр стратегий."""
        self.registry = registry

    def resolve(self, mode: str) -> ChunkingStrategy:
        """Разрешает и возвращает стратегию по значению режима."""
        return self.registry.get(mode=mode)
