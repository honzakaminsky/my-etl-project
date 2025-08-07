import pandas as pd

def read_csv(Ffilepath: str) -> pd.DataFrame:
    return pd.read_csv(filepath, sep=r'\s+')

