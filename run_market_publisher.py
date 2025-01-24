import sys
import os

# Get the absolute path to the project root (2 levels up from management/commands)
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
print("Project root:", project_root)

# Add project root to Python path
sys.path.append(project_root)

print("Current working directory:", os.getcwd())
print("Python path after:", sys.path)
print("File location:", __file__)

from markets.consumers import MarketDataService

# Your code here... 