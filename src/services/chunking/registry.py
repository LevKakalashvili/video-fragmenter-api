from src.domain.errors import UnsupportedChunkingModeError
from src.services.chunking.base import ChunkingStrategy


class ChunkingStrategyRegistry:
    """Реестр стратегий фрагментации по значению `mode`."""

    def __init__(self) -> None:
        """Создает пустой реестр стратегий."""
        self._strategies: dict[str, ChunkingStrategy] = {}

    def register(self, strategy: ChunkingStrategy) -> None:
        """Регистрирует стратегию по ее полю `mode`."""
        self._strategies[strategy.mode] = strategy

    def get(self, mode: str) -> ChunkingStrategy:
        """Возвращает стратегию по `mode` или бросает доменную ошибку."""
        strategy = self._strategies.get(mode)
        if strategy is None:
            raise UnsupportedChunkingModeError(mode=mode, available_modes=tuple(self._strategies))
        return strategy

    def available_modes(self) -> tuple[str, ...]:
        """Возвращает список доступных режимов фрагментации."""
        return tuple(self._strategies)
