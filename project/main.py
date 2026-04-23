import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from src.project import project

def main():
    project()

if __name__ == "__main__":
    main()