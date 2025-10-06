import requests
import json
import time
import logging
from typing import Dict, Any

# Настройка логирования с поддержкой Unicode
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('test_service.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class ServiceTester:
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.session = requests.Session()
    
    def test_health(self) -> bool:
        """Тест здоровья сервиса"""
        try:
            response = self.session.get(f"{self.base_url}/health")
            if response.status_code == 200:
                data = response.json()
                logger.info("[OK] Сервис здоров")
                logger.info(f"   Data loaded: {data.get('data_loaded', False)}")
                return True
            else:
                logger.error(f"[ERROR] Сервис не здоров: {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"[ERROR] Ошибка подключения к сервису: {e}")
            return False
    
    def test_user_without_personal_recommendations(self):
        """Тест для пользователя без персональных рекомендаций"""
        logger.info("\n" + "="*60)
        logger.info("ТЕСТ 1: Пользователь без персональных рекомендаций")
        logger.info("="*60)
        
        # user_id, которого нет в обучающих данных
        test_user_id = 9999999
        
        try:
            response = self.session.get(f"{self.base_url}/recommend/{test_user_id}")
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"[OK] Успешный ответ для user_id={test_user_id}")
                logger.info(f"   Статистика: {json.dumps(data['statistics'], indent=2)}")
                logger.info(f"   Рекомендаций получено: {len(data['recommendations'])}")
                
                # Проверяем, что получили рекомендации
                if data['recommendations']:
                    logger.info("   Пример рекомендаций:")
                    for i, rec in enumerate(data['recommendations'][:3], 1):
                        logger.info(f"     {i}. {rec['track_name']} - {rec['artists']} ({rec['type']})")
                    return True
                else:
                    logger.warning("   [WARNING] Нет рекомендаций")
                    return False
            else:
                logger.error(f"[ERROR] Ошибка: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"[ERROR] Исключение при тестировании: {e}")
            return False
    
    def test_user_with_personal_no_online_history(self):
        """Тест для пользователя с персональными рекомендациями, но без онлайн-истории"""
        logger.info("\n" + "="*60)
        logger.info("ТЕСТ 2: Пользователь с персональными рекомендациями (без онлайн-истории)")
        logger.info("="*60)
        
        # user_id, который есть в обучающих данных (берем из реальных данных)
        test_user_id = 0  # Первый пользователь из данных
        
        try:
            response = self.session.get(f"{self.base_url}/recommend/{test_user_id}")
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"[OK] Успешный ответ для user_id={test_user_id}")
                logger.info(f"   Статистика: {json.dumps(data['statistics'], indent=2)}")
                logger.info(f"   Рекомендаций получено: {len(data['recommendations'])}")
                
                # Анализ типов рекомендаций
                rec_types = {}
                for rec in data['recommendations']:
                    rec_type = rec['type']
                    rec_types[rec_type] = rec_types.get(rec_type, 0) + 1
                
                logger.info(f"   Распределение типов рекомендаций: {rec_types}")
                
                if data['recommendations']:
                    logger.info("   Пример рекомендаций:")
                    for i, rec in enumerate(data['recommendations'][:3], 1):
                        logger.info(f"     {i}. {rec['track_name']} - {rec['artists']} ({rec['type']})")
                
                return True
            else:
                logger.error(f"[ERROR] Ошибка: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"[ERROR] Исключение при тестировании: {e}")
            return False
    
    def test_user_with_personal_and_online_history(self):
        """Тест для пользователя с персональными рекомендациями и онлайн-историей"""
        logger.info("\n" + "="*60)
        logger.info("ТЕСТ 3: Пользователь с персональными рекомендациями и онлайн-историей")
        logger.info("="*60)
        
        # user_id, который есть в обучающих данных
        test_user_id = 0
        
        # Онлайн история - популярные треки из данных
        online_history = "53404,33311009,178529,35505245,65851540"  # Популярные треки
        
        try:
            response = self.session.get(
                f"{self.base_url}/recommend/{test_user_id}",
                params={"online_history": online_history, "limit": 15}
            )
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"[OK] Успешный ответ для user_id={test_user_id} с онлайн-историей")
                logger.info(f"   Статистика: {json.dumps(data['statistics'], indent=2)}")
                logger.info(f"   Рекомендаций получено: {len(data['recommendations'])}")
                
                # Анализ источников рекомендаций
                sources = {}
                types = {}
                for rec in data['recommendations']:
                    source = rec['source']
                    rec_type = rec['type']
                    sources[source] = sources.get(source, 0) + 1
                    types[rec_type] = types.get(rec_type, 0) + 1
                
                logger.info(f"   Распределение по источникам: {sources}")
                logger.info(f"   Распределение по типам: {types}")
                
                if data['recommendations']:
                    logger.info("   Пример рекомендаций:")
                    for i, rec in enumerate(data['recommendations'][:5], 1):
                        source_info = f"{rec['source']}/{rec['type']}"
                        if rec['type'] == 'similar_to_history':
                            source_info += f" (на основе {rec['based_on_track']})"
                        logger.info(f"     {i}. {rec['track_name']} - {rec['artists']} [{source_info}]")
                
                # Проверяем наличие онлайн-рекомендаций
                online_recs = [r for r in data['recommendations'] if r['source'] == 'online']
                if online_recs:
                    logger.info(f"[OK] Обнаружены онлайн-рекомендации: {len(online_recs)}")
                else:
                    logger.warning("[WARNING] Онлайн-рекомендации не обнаружены")
                
                return True
            else:
                logger.error(f"[ERROR] Ошибка: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"[ERROR] Исключение при тестировании: {e}")
            return False
    
    def test_track_info(self):
        """Тест получения информации о треке"""
        logger.info("\n" + "="*60)
        logger.info("ТЕСТ 4: Получение информации о треке")
        logger.info("="*60)
        
        test_track_id = 53404  # Smells Like Teen Spirit
        
        try:
            response = self.session.get(f"{self.base_url}/track/{test_track_id}")
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"[OK] Информация о треке {test_track_id}:")
                logger.info(f"   Название: {data['track_name']}")
                logger.info(f"   Артисты: {data['artist_names']}")
                logger.info(f"   Жанры: {data['genre_names']}")
                return True
            else:
                logger.error(f"[ERROR] Ошибка получения информации о треке: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"[ERROR] Исключение при тестировании: {e}")
            return False
    
    def run_all_tests(self):
        """Запуск всех тестов"""
        logger.info("ЗАПУСК ТЕСТИРОВАНИЯ СЕРВИСА РЕКОМЕНДАЦИЙ")
        logger.info("="*60)
        
        # Проверка здоровья сервиса
        if not self.test_health():
            logger.error("Сервис не доступен, прекращаем тестирование")
            return False
        
        # Небольшая пауза для уверенности, что сервис готов
        time.sleep(2)
        
        test_results = []
        
        # Запуск тестов
        test_results.append(("Здоровье сервиса", True))  # Уже проверили
        
        test_results.append(("Пользователь без персональных рекомендаций", 
                           self.test_user_without_personal_recommendations()))
        
        test_results.append(("Пользователь с персональными рекомендациями (без онлайн-истории)", 
                           self.test_user_with_personal_no_online_history()))
        
        test_results.append(("Пользователь с персональными рекомендациями и онлайн-историей", 
                           self.test_user_with_personal_and_online_history()))
        
        test_results.append(("Получение информации о треке", 
                           self.test_track_info()))
        
        # Итоги
        logger.info("\n" + "="*60)
        logger.info("ИТОГИ ТЕСТИРОВАНИЯ")
        logger.info("="*60)
        
        passed = 0
        total = len(test_results)
        
        for test_name, result in test_results:
            status = "[PASS]" if result else "[FAIL]"
            logger.info(f"{status}: {test_name}")
            if result:
                passed += 1
        
        logger.info(f"\nРЕЗУЛЬТАТ: {passed}/{total} тестов пройдено успешно")
        
        return passed == total

if __name__ == "__main__":
    tester = ServiceTester()
    success = tester.run_all_tests()
    
    exit(0 if success else 1)