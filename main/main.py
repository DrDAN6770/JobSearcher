from webcrawler104_async import webcrawler_main
from DataToLake import DataToLake_main
from DataToWarehouse import DataToWarehouse_main

def main():
    webcrawler_main()
    DataToLake_main()
    DataToWarehouse_main()

if __name__ == "__main__":
    main()