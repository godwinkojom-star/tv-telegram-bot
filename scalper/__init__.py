"""SmartFX experimental Gold reversal scalper."""

from .config import ScalperConfig
from .strategy import evaluate, should_reverse

__all__ = ["ScalperConfig", "evaluate", "should_reverse"]
