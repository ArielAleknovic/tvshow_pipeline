from extract import extract_exchange_data
from transform import transform_data
from aggregate import aggregate_data

if __name__ == '__main__':
    extract_exchange_data()
    print("✅ Etapa Bronze concluída")

    transform_data()
    print("✅ Etapa Silver concluída")

    aggregate_data()
    print("✅ Etapa Gold concluída")
