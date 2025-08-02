from pipeline.scripts.utils import read_from_minio, save_to_minio
import pandas as pd
from io import StringIO

def transform_data():
    raw_csv = read_from_minio('bronze', 'exchange_rates/raw.csv')
    df = pd.read_csv(StringIO(raw_csv))

    df_clean = df[['dataHoraCotacao', 'cotacaoCompra', 'cotacaoVenda']]
    df_clean['data'] = pd.to_datetime(df_clean['dataHoraCotacao']).dt.date
    df_clean = df_clean.drop(columns=['dataHoraCotacao'])

    out = StringIO()
    df_clean.to_csv(out, index=False)
    save_to_minio('silver', 'exchange_rates/cleaned.csv', out.getvalue())
