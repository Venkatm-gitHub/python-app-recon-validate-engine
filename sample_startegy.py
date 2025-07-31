from abc import ABC, abstractmethod
from typing import Dict, Any
import requests


# Strategy Interface (Abstraction)
class APIStrategy(ABC):
    """Abstract base class for API calling strategies"""
    
    @abstractmethod
    def call_api(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Call the appropriate API for the programming language"""
        pass
    
    @abstractmethod
    def get_language_name(self) -> str:
        """Return the programming language name"""
        pass


# Concrete Strategy Implementations
class PythonAPIStrategy(APIStrategy):
    """Strategy for calling Python-related APIs"""
    
    def __init__(self, base_url: str = "https://api.python-service.com"):
        self.base_url = base_url
    
    def call_api(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Call Python API"""
        try:
            # Example API call - replace with your actual endpoint
            response = requests.post(f"{self.base_url}/python/execute", json=data)
            response.raise_for_status()
            return {
                "status": "success",
                "language": "python",
                "result": response.json()
            }
        except requests.RequestException as e:
            return {
                "status": "error",
                "language": "python",
                "error": str(e)
            }
    
    def get_language_name(self) -> str:
        return "python"


class SQLAPIStrategy(APIStrategy):
    """Strategy for calling SQL-related APIs"""
    
    def __init__(self, base_url: str = "https://api.sql-service.com"):
        self.base_url = base_url
    
    def call_api(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Call SQL API"""
        try:
            # Example API call - replace with your actual endpoint
            response = requests.post(f"{self.base_url}/sql/query", json=data)
            response.raise_for_status()
            return {
                "status": "success",
                "language": "sql",
                "result": response.json()
            }
        except requests.RequestException as e:
            return {
                "status": "error",
                "language": "sql",
                "error": str(e)
            }
    
    def get_language_name(self) -> str:
        return "sql"


class JavaAPIStrategy(APIStrategy):
    """Strategy for calling Java-related APIs"""
    
    def __init__(self, base_url: str = "https://api.java-service.com"):
        self.base_url = base_url
    
    def call_api(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Call Java API"""
        try:
            # Example API call - replace with your actual endpoint
            response = requests.post(f"{self.base_url}/java/compile", json=data)
            response.raise_for_status()
            return {
                "status": "success",
                "language": "java",
                "result": response.json()
            }
        except requests.RequestException as e:
            return {
                "status": "error",
                "language": "java",
                "error": str(e)
            }
    
    def get_language_name(self) -> str:
        return "java"


# Factory Pattern Implementation
class APIStrategyFactory:
    """Factory class to create appropriate API strategy instances"""
    
    _strategies = {
        "python": PythonAPIStrategy,
        "sql": SQLAPIStrategy,
        "java": JavaAPIStrategy
    }
    
    @classmethod
    def create_strategy(cls, language: str) -> APIStrategy:
        """Create and return appropriate strategy instance"""
        language_lower = language.lower()
        
        if language_lower not in cls._strategies:
            raise ValueError(f"Unsupported programming language: {language}")
        
        strategy_class = cls._strategies[language_lower]
        return strategy_class()
    
    @classmethod
    def register_strategy(cls, language: str, strategy_class: type):
        """Register a new strategy for future extension"""
        if not issubclass(strategy_class, APIStrategy):
            raise TypeError("Strategy class must inherit from APIStrategy")
        
        cls._strategies[language.lower()] = strategy_class
    
    @classmethod
    def get_supported_languages(cls) -> list:
        """Return list of supported programming languages"""
        return list(cls._strategies.keys())


# Context Class
class APIClient:
    """Context class that uses the strategy pattern"""
    
    def __init__(self, language: str):
        """Initialize with appropriate strategy based on language"""
        self.strategy = APIStrategyFactory.create_strategy(language)
    
    def execute_api_call(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute API call using the selected strategy"""
        return self.strategy.call_api(data)
    
    def get_current_language(self) -> str:
        """Get current programming language"""
        return self.strategy.get_language_name()


# Example usage and main application
def main():
    """Main application function"""
    import argparse
    
    # Set up command line argument parsing
    parser = argparse.ArgumentParser(description="API Client for different programming languages")
    parser.add_argument(
        "language", 
        choices=APIStrategyFactory.get_supported_languages(),
        help="Programming language to use for API calls"
    )
    parser.add_argument(
        "--data", 
        type=str, 
        default='{"code": "print(\'Hello World\')"}',
        help="JSON data to send to API"
    )
    
    args = parser.parse_args()
    
    try:
        # Create API client with specified language
        client = APIClient(args.language)
        
        # Parse JSON data
        import json
        data = json.loads(args.data)
        
        # Execute API call
        print(f"Using {client.get_current_language()} API strategy...")
        result = client.execute_api_call(data)
        
        # Print result
        print("API Response:")
        print(json.dumps(result, indent=2))
        
    except ValueError as e:
        print(f"Error: {e}")
    except json.JSONDecodeError:
        print("Error: Invalid JSON data provided")
    except Exception as e:
        print(f"Unexpected error: {e}")


# Example of extending with a new language (demonstrates extensibility)
class GoAPIStrategy(APIStrategy):
    """Strategy for calling Go-related APIs"""
    
    def __init__(self, base_url: str = "https://api.go-service.com"):
        self.base_url = base_url
    
    def call_api(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Call Go API"""
        try:
            response = requests.post(f"{self.base_url}/go/run", json=data)
            response.raise_for_status()
            return {
                "status": "success",
                "language": "go",
                "result": response.json()
            }
        except requests.RequestException as e:
            return {
                "status": "error",
                "language": "go",
                "error": str(e)
            }
    
    def get_language_name(self) -> str:
        return "go"


# Register the new strategy (this could be done in a separate module)
# APIStrategyFactory.register_strategy("go", GoAPIStrategy)


if __name__ == "__main__":
    main()
