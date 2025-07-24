from abc import ABC, abstractmethod

# Interface definitions
class IAddition(ABC):
    @abstractmethod
    def add(self, a, b):
        pass

class ISubtraction(ABC):
    @abstractmethod
    def sub(self, a, b):
        pass

class IMultiplication(ABC):
    @abstractmethod
    def multiply(self, a, b):
        pass

# Implementation
class CalculatorOperations(IAddition, ISubtraction, IMultiplication):
    def add(self, a, b):
        return a + b
    
    def sub(self, a, b):
        return a - b
    
    def multiply(self, a, b):
        return a * b

# Main execution
def main():
    calc = CalculatorOperations()
    print(calc.add(5, 3))
    print(calc.sub(10, 4))
    print(calc.multiply(2, 6))

if __name__ == "__main__":
    main()
