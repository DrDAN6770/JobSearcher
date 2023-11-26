from datetime import datetime, timedelta
import pandas as pd

def checknewdata() -> str:
    current_date = datetime.now().date() + timedelta(days=1)
    count = 7

    while count != 0:
        try:
            file_name = f"JBLIST_{current_date}.csv"
            df = pd.read_csv(f'output/{file_name}')
            print(f"There're new data")
            return f'output/{file_name}'
        except:
            current_date -= timedelta(days=1)
            count -= 1
    print('No new data')
    return 