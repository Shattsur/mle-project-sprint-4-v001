import pandas as pd
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config
import os
from dotenv import load_dotenv
import tempfile

# Загрузка переменных окружения
load_dotenv()

def check_s3_data_files():
    # Конфигурация S3
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    bucket_name = os.getenv("S3_BUCKET_NAME")
    
    # Создаём клиент S3
    s3_config = Config(
        region_name='ru-central1',
        signature_version='s3v4',
        s3={'addressing_style': 'path'}
    )

    s3 = boto3.client(
        's3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_access_key,
        config=s3_config
    )
    
    # Файлы для проверки в S3
    files = {
        "items.parquet": "recsys/data/items.parquet",
        "similar.parquet": "recsys/recommendations/similar.parquet",
        "personal_als.parquet": "recsys/recommendations/personal_als.parquet", 
        "top_popular.parquet": "recsys/recommendations/top_popular.parquet"
    }
    
    print("Проверка файлов данных в S3:")
    print("=" * 50)
    print(f"Бакет: {bucket_name}")
    print("=" * 50)
    
    for name, s3_key in files.items():
        try:
            # Проверяем существование файла в S3 
            s3.head_object(Bucket=bucket_name, Key=s3_key)
            print(f"✅ {name}: Файл найден в S3")
            
            # Скачиваем и читаем файл для проверки содержимого
            try:
                # Создаем временный файл
                with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as temp_file:
                    temp_path = temp_file.name
                
                # Скачиваем файл во временный файл
                s3.download_file(Bucket=bucket_name, Key=s3_key, Filename=temp_path)
                
                # Читаем из временного файла
                df = pd.read_parquet(temp_path)
                print(f"   📊 Записей: {len(df):,}")
                if len(df) > 0:
                    print(f"   📋 Колонки: {list(df.columns)}")
                    if 'track_id' in df.columns:
                        sample_ids = df['track_id'].iloc[:3].tolist()
                        print(f"   🎵 Пример track_id: {sample_ids}")
                
                # Удаляем временный файл
                os.unlink(temp_path)
                
            except Exception as e:
                print(f"   ❌ Ошибка чтения файла: {e}")
                # Убедимся, что временный файл удален даже при ошибке
                if 'temp_path' in locals() and os.path.exists(temp_path):
                    os.unlink(temp_path)
                
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                print(f"❌ {name}: Файл не найден в S3 по пути {s3_key}")
            else:
                print(f"❌ {name}: Ошибка доступа к S3 - {e}")

def check_s3_connection():
    """Проверка подключения к S3 и существования бакета"""
    try:
        aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        bucket_name = os.getenv("S3_BUCKET_NAME")
        
        s3_config = Config(
            region_name='ru-central1',
            signature_version='s3v4',
            s3={'addressing_style': 'path'}
        )

        s3 = boto3.client(
            's3',
            endpoint_url='https://storage.yandexcloud.net',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_access_key,
            config=s3_config
        )
        
        # Проверяем существование бакета 
        s3.head_bucket(Bucket=bucket_name)
        print("✅ Подключение к S3 успешно")
        print(f"✅ Бакет '{bucket_name}' доступен")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка подключения к S3: {e}")
        return False

def get_file_sizes():
    """Показывает размеры файлов в S3"""
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    bucket_name = os.getenv("S3_BUCKET_NAME")
    
    s3_config = Config(
        region_name='ru-central1',
        signature_version='s3v4',
        s3={'addressing_style': 'path'}
    )

    s3 = boto3.client(
        's3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_access_key,
        config=s3_config
    )
    
    files = {
        "items.parquet": "recsys/data/items.parquet",
        "similar.parquet": "recsys/recommendations/similar.parquet",
        "personal_als.parquet": "recsys/recommendations/personal_als.parquet", 
        "top_popular.parquet": "recsys/recommendations/top_popular.parquet"
    }
    
    print(f"\n📏 Размеры файлов в S3:")
    print("=" * 50)
    
    for name, s3_key in files.items():
        try:
            response = s3.head_object(Bucket=bucket_name, Key=s3_key)
            size_mb = response['ContentLength'] / (1024 * 1024)
            last_modified = response['LastModified']
            print(f"   {name}: {size_mb:.2f} MB (обновлен: {last_modified})")
        except Exception as e:
            print(f"   {name}: Не удалось получить размер - {e}")

if __name__ == "__main__":
    print("Проверка подключения к S3...")
    if check_s3_connection():
        print("\n" + "="*50)
        check_s3_data_files()
        get_file_sizes()
    else:
        print("Не удалось подключиться к S3. Проверьте переменные окружения.")