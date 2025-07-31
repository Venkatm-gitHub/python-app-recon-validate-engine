# api_client/__init__.py
"""
API Client Package

A flexible API client that uses the Strategy and Factory patterns
to handle different programming language APIs.
"""

__version__ = "1.0.0"
__author__ = "Your Name"

from .client.api_client import APIClient
from .factory.strategy_factory import APIStrategyFactory
from .interfaces.api_strategy import APIStrategy

__all__ = ["APIClient", "APIStrategyFactory", "APIStrategy"]


# interfaces/__init__.py
"""Interface definitions for the API Client"""

from .api_strategy import APIStrategy

__all__ = ["APIStrategy"]


# strategies/__init__.py
"""Strategy implementations for different programming languages"""

from .all_strategies import (
    PythonAPIStrategy,
    SQLAPIStrategy,
    JavaAPIStrategy,
    GoAPIStrategy,
    CSharpAPIStrategy,
    RustAPIStrategy,
    AVAILABLE_STRATEGIES,
    get_strategy_class,
    get_available_languages
)

__all__ = [
    "PythonAPIStrategy",
    "SQLAPIStrategy", 
    "JavaAPIStrategy",
    "GoAPIStrategy",
    "CSharpAPIStrategy",
    "RustAPIStrategy",
    "AVAILABLE_STRATEGIES",
    "get_strategy_class",
    "get_available_languages"
]


# factory/__init__.py
"""Factory pattern implementation for creating strategies"""

from .strategy_factory import APIStrategyFactory

__all__ = ["APIStrategyFactory"]


# client/__init__.py
"""Client context classes"""

from .api_client import APIClient

__all__ = ["APIClient"]



#################################

# All commands work exactly the same
python main.py python --data '{"code": "print(\"Hello World\")"}'
python main.py sql --data '{"query": "SELECT * FROM users"}'

# Now supports additional languages too
python main.py go --data '{"code": "fmt.Println(\"Hello World\")"}'
python main.py csharp --data '{"code": "Console.WriteLine(\"Hello World\");"}'
python main.py rust --data '{"code": "println!(\"Hello World\");"}'