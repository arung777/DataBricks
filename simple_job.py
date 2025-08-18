#!/usr/bin/env python3
"""
Simple Hello World Databricks Job
This is a basic Python script that just prints Hello World
"""

def main():
    """Main function - just prints Hello World"""
    print("Hello World from Databricks!")
    print("This is a simple Python job running in Databricks")
    
    # Return a simple success message
    return "Hello World completed successfully!"

if __name__ == "__main__":
    try:
        result = main()
        print(f"Job Result: {result}")
    except Exception as e:
        print(f"Error: {e}")
        raise
