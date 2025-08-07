import pandas as pd
from etl.transform import clean_data

def test_clean_data_basic():
    data = {
        'DateTime': ['2023.08.01 12:00:00'],
        'Open': [100],
        'High': [110],
        'Low': [95],
        'Close': [105]
    }
    df= pd.DataFrame(data)

    cleaned_df = clean_data(df)

    assert 'DateTime' in cleaned_df.columns
    assert 'source' in cleaned_df.columns
    assert 'Timestamp' in cleaned_df.columns
    assert cleaned_df.shape[0] == 1