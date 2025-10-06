from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional
import logging
from datetime import datetime
import os
from pydantic import BaseModel
import boto3
from botocore.config import Config
from io import BytesIO

from dotenv import load_dotenv
load_dotenv()

app = FastAPI(title="Music Recommendation Service", version="1.0.0")

# Добавляем CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Конфигурация
MODEL_FILES = {
    "items": "data/items.parquet",
    "similar": "data/similar.parquet", 
    "personal": "data/personal_als.parquet",
    "top_popular": "data/top_popular.parquet"
}

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Pydantic модели для корректной сериализации
class TrackRecommendation(BaseModel):
    track_id: int
    track_name: str
    artists: List[str]
    genres: List[str]
    score: float
    type: str
    source: str
    based_on_track: Optional[int] = None

class RecommendationResponse(BaseModel):
    user_id: int
    recommendations: List[TrackRecommendation]
    statistics: Dict[str, Any]
    timestamp: str

class TrackInfo(BaseModel):
    track_name: str
    artist_names: List[str]
    genre_names: List[str]

class HealthResponse(BaseModel):
    status: str
    data_loaded: bool
    timestamp: str

class RecommendationService:
    def __init__(self):
        self.items_df = None
        self.similar_df = None
        self.personal_recs_df = None
        self.top_popular_df = None
        self.s3_client = None
        self.bucket_name = os.getenv("S3_BUCKET_NAME")
        
    def _get_s3_client(self):
        """Initialize and return S3 client"""
        if self.s3_client is None:
            aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
            aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
            
            s3_config = Config(
                region_name='ru-central1',
                signature_version='s3v4',
                s3={'addressing_style': 'path'}
            )

            self.s3_client = boto3.client(
                's3',
                endpoint_url='https://storage.yandexcloud.net',
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_access_key,
                config=s3_config
            )
        return self.s3_client
    
    def _read_parquet_from_s3(self, s3_key):
        """Read parquet file from S3 and return as DataFrame"""
        try:
            s3 = self._get_s3_client()
            response = s3.get_object(Bucket=self.bucket_name, Key=s3_key)
            return pd.read_parquet(BytesIO(response['Body'].read()))
        except Exception as e:
            logger.error(f"Error reading {s3_key} from S3: {e}")
            return None
    
    def load_data(self):
        """Загрузка всех необходимых данных из S3"""
        try:
            logger.info("Starting data loading from S3...")
            
            # S3 file paths - update these to match your actual S3 paths
            s3_files = {
                "items": "recsys/data/items.parquet",
                "similar": "recsys/recommendations/similar.parquet", 
                "personal": "recsys/recommendations/personal_als.parquet",
                "top_popular": "recsys/recommendations/top_popular.parquet"
            }
            
            # Load items.parquet
            self.items_df = self._read_parquet_from_s3(s3_files["items"])
            if self.items_df is not None:
                logger.info(f"Loaded items from S3: {len(self.items_df)} tracks")
            else:
                logger.error("Failed to load items from S3")
                return False
                
            # Load similar tracks
            self.similar_df = self._read_parquet_from_s3(s3_files["similar"])
            if self.similar_df is not None:
                logger.info(f"Loaded similar tracks from S3: {len(self.similar_df)} pairs")
            else:
                logger.warning("Failed to load similar tracks from S3")
                
            # Load personal recommendations
            self.personal_recs_df = self._read_parquet_from_s3(s3_files["personal"])
            if self.personal_recs_df is not None:
                logger.info(f"Loaded personal recommendations from S3: {len(self.personal_recs_df)} records")
            else:
                logger.warning("Failed to load personal recommendations from S3")
                
            # Load top popular
            self.top_popular_df = self._read_parquet_from_s3(s3_files["top_popular"])
            if self.top_popular_df is not None:
                logger.info(f"Loaded top popular from S3: {len(self.top_popular_df)} tracks")
            else:
                logger.warning("Failed to load top popular from S3")
            
            logger.info("All data successfully loaded from S3")
            return True
            
        except Exception as e:
            logger.error(f"Error loading data from S3: {e}")
            return False
    
    def get_offline_recommendations(self, user_id: int, limit: int = 10) -> List[TrackRecommendation]:
        """Получение офлайн-рекомендаций (ALS + популярные)"""
        recommendations = []
        
        # 1. Персональные рекомендации из ALS
        if self.personal_recs_df is not None:
            try:
                user_recs = self.personal_recs_df[
                    self.personal_recs_df['user_id'] == user_id
                ].head(limit)
                
                for _, row in user_recs.iterrows():
                    track_info = self.get_track_info(int(row['track_id']))
                    if track_info:
                        # Преобразуем numpy типы в Python типы
                        track_id = int(row['track_id'])
                        score = float(row.get('score', 0.5))
                        
                        recommendation = TrackRecommendation(
                            track_id=track_id,
                            track_name=track_info.track_name,
                            artists=track_info.artist_names,
                            genres=track_info.genre_names,
                            score=score,
                            type='personal_als',
                            source='offline'
                        )
                        recommendations.append(recommendation)
            except Exception as e:
                logger.warning(f"Error getting personal recommendations for user {user_id}: {e}")
        
        # 2. Если персональных рекомендаций мало, добавляем популярные
        if len(recommendations) < limit and self.top_popular_df is not None:
            try:
                remaining = limit - len(recommendations)
                for _, row in self.top_popular_df.head(remaining).iterrows():
                    track_info = self.get_track_info(int(row['track_id']))
                    if track_info and not any(rec.track_id == int(row['track_id']) for rec in recommendations):
                        recommendation = TrackRecommendation(
                            track_id=int(row['track_id']),
                            track_name=track_info.track_name,
                            artists=track_info.artist_names,
                            genres=track_info.genre_names,
                            score=float(row.get('users', 1)),
                            type='top_popular',
                            source='offline'
                        )
                        recommendations.append(recommendation)
            except Exception as e:
                logger.warning(f"Error getting popular recommendations: {e}")
        
        # 3. Если все еще мало, добавляем случайные треки из каталога
        if len(recommendations) < limit and self.items_df is not None:
            try:
                remaining = limit - len(recommendations)
                random_tracks = self.items_df.sample(min(remaining, len(self.items_df)))
                for _, row in random_tracks.iterrows():
                    if not any(rec.track_id == int(row['track_id']) for rec in recommendations):
                        # Обеспечиваем, что все данные правильных типов
                        artists = list(row['artist_names']) if isinstance(row['artist_names'], list) else []
                        genres = list(row['genre_names']) if isinstance(row['genre_names'], list) else []
                        
                        recommendation = TrackRecommendation(
                            track_id=int(row['track_id']),
                            track_name=str(row['track_name']),
                            artists=artists,
                            genres=genres,
                            score=0.1,
                            type='random',
                            source='offline'
                        )
                        recommendations.append(recommendation)
            except Exception as e:
                logger.warning(f"Error getting random recommendations: {e}")
        
        return recommendations[:limit]
    
    def get_online_recommendations(self, user_history: List[int], limit: int = 5) -> List[TrackRecommendation]:
        """Онлайн-рекомендации на основе последних прослушиваний"""
        if not user_history or self.similar_df is None:
            return []
        
        recommendations = []
        
        try:
            # Берем последние 5 треков из истории
            recent_tracks = user_history[-5:]
            
            for track_id in recent_tracks:
                # Ищем похожие треки
                similar_tracks = self.similar_df[
                    self.similar_df['track_id'] == track_id
                ].sort_values('similarity_score', ascending=False).head(3)
                
                for _, similar_row in similar_tracks.iterrows():
                    similar_track_id = int(similar_row['similar_track_id'])
                    track_info = self.get_track_info(similar_track_id)
                    if track_info:
                        recommendation = TrackRecommendation(
                            track_id=similar_track_id,
                            track_name=track_info.track_name,
                            artists=track_info.artist_names,
                            genres=track_info.genre_names,
                            score=float(similar_row['similarity_score']),
                            type='similar_to_history',
                            source='online',
                            based_on_track=int(track_id)
                        )
                        recommendations.append(recommendation)
            
            # Убираем дубликаты и сортируем по score
            seen = set()
            unique_recs = []
            for rec in recommendations:
                if rec.track_id not in seen:
                    seen.add(rec.track_id)
                    unique_recs.append(rec)
            
            return sorted(unique_recs, key=lambda x: x.score, reverse=True)[:limit]
            
        except Exception as e:
            logger.warning(f"Error getting online recommendations: {e}")
            return []
    
    def get_track_info(self, track_id: int) -> Optional[TrackInfo]:
        """Получение информации о треке"""
        if self.items_df is None:
            return None
        
        try:
            track_info = self.items_df[self.items_df['track_id'] == track_id]
            if track_info.empty:
                return None
            
            row = track_info.iloc[0]
            
            # Преобразуем в правильные типы
            track_name = str(row['track_name'])
            artist_names = list(row['artist_names']) if isinstance(row['artist_names'], list) else []
            genre_names = list(row['genre_names']) if isinstance(row['genre_names'], list) else []
            
            return TrackInfo(
                track_name=track_name,
                artist_names=artist_names,
                genre_names=genre_names
            )
        except Exception as e:
            logger.warning(f"Error getting track info for {track_id}: {e}")
            return None
    
    def blend_recommendations(self, offline_recs: List[TrackRecommendation], 
                            online_recs: List[TrackRecommendation], 
                            total_limit: int = 10) -> List[TrackRecommendation]:
        """Смешивание онлайн и офлайн рекомендаций"""
        blended = []
        
        # Стратегия смешивания: 70% офлайн, 30% онлайн
        offline_limit = int(total_limit * 0.7)
        online_limit = total_limit - offline_limit
        
        # Добавляем офлайн рекомендации
        blended.extend(offline_recs[:offline_limit])
        
        # Добавляем онлайн рекомендации, исключая дубликаты
        offline_track_ids = {rec.track_id for rec in blended}
        for online_rec in online_recs[:online_limit]:
            if online_rec.track_id not in offline_track_ids:
                blended.append(online_rec)
        
        # Если рекомендаций меньше лимита, добиваем офлайн
        if len(blended) < total_limit:
            remaining = total_limit - len(blended)
            additional_offline = [rec for rec in offline_recs[offline_limit:] 
                                if rec.track_id not in {r.track_id for r in blended}]
            blended.extend(additional_offline[:remaining])
        
        return blended[:total_limit]

# Инициализация сервиса
service = RecommendationService()

@app.on_event("startup")
async def startup_event():
    """Загрузка данных при старте сервиса"""
    success = service.load_data()
    if not success:
        logger.error("Failed to load data on service startup")

@app.get("/")
async def root():
    return {"message": "Music Recommendation Service", "status": "active"}

@app.get("/health", response_model=HealthResponse)
async def health():
    data_status = all([
        service.items_df is not None,
        service.personal_recs_df is not None or service.top_popular_df is not None
    ])
    return HealthResponse(
        status="healthy" if data_status else "degraded",
        data_loaded=data_status,
        timestamp=datetime.now().isoformat()
    )

@app.get("/recommend/{user_id}", response_model=RecommendationResponse)
async def get_recommendations(
    user_id: int,
    limit: int = 10,
    online_history: str = None
):
    """
    Получение рекомендаций для пользователя
    """
    try:
        # Валидация параметров
        if limit <= 0 or limit > 100:
            raise HTTPException(status_code=400, detail="Limit must be between 1 and 100")
        
        # Парсинг онлайн истории
        user_online_history = []
        if online_history:
            try:
                user_online_history = [int(x.strip()) for x in online_history.split(',')]
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid online_history format")
        
        logger.info(f"Getting recommendations for user_id={user_id}, online_history={len(user_online_history)} tracks")
        
        # Получение офлайн рекомендаций
        offline_recs = service.get_offline_recommendations(user_id, limit)
        
        # Получение онлайн рекомендаций
        online_recs = service.get_online_recommendations(user_online_history, limit)
        
        # Смешивание рекомендаций
        final_recommendations = service.blend_recommendations(offline_recs, online_recs, limit)
        
        # Статистика
        stats = {
            "total_recommendations": len(final_recommendations),
            "offline_count": len(offline_recs),
            "online_count": len(online_recs),
            "user_id": user_id,
            "online_history_provided": len(user_online_history) > 0
        }
        
        return RecommendationResponse(
            user_id=user_id,
            recommendations=final_recommendations,
            statistics=stats,
            timestamp=datetime.now().isoformat()
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting recommendations for user_id={user_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/track/{track_id}", response_model=TrackInfo)
async def get_track_info(track_id: int):
    """Получение информации о треке"""
    track_info = service.get_track_info(track_id)
    if not track_info:
        raise HTTPException(status_code=404, detail="Track not found")
    return track_info

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)