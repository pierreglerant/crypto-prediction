import requests
from typing import List, Dict, Any
import time


class GDELTClient:
    BASE_URL = "https://api.gdeltproject.org/api/v2/doc/doc"

    def __init__(self, timeout: int = 30):
        self.timeout = timeout

    def _get(self, params: Dict[str, Any]) -> Dict[str, Any]:
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                response = requests.get(self.BASE_URL, params=params, timeout=self.timeout)
                
                # Handle rate limiting (429 Too Many Requests)
                if response.status_code == 429:
                    retry_count += 1
                    wait_time = 10 * retry_count  # 60s, 120s, 180s
                    print(f"Rate limited. Waiting {wait_time}s before retry {retry_count}/{max_retries}...")
                    time.sleep(wait_time)
                    continue
                
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                raise RuntimeError(f"GDELT API error: {e}")
        
        raise RuntimeError("Max retries for rate limiting exceeded")

    def get_articles_with_sentiment(
        self,
        query: str,
        timespan: str = "1d",
        max_records: int = 50,
        sleep: float = 15.0,
        pages: int = 3,
    ) -> List[Dict[str, Any]]:
        """
        Récupère les articles AVEC leurs scores de sentiment.
        Utilise le mode 'artlist' avec paramètres pour inclure les sentiments.
        """
        all_articles = []

        for i in range(pages):
            params = {
                "query": f'"{query}"',
                "mode": "artlist",  # Mode pour liste d'articles
                "maxrecords": max_records,
                "timespan": timespan,
                "format": "json",
                "sort": "DateDesc",
                # Paramètres pour inclure les sentiments
                "tonechart": "true",  # Inclure les données de tonalité
            }

            data = self._get(params)

            articles = data.get("articles", [])
            all_articles.extend(articles)

            time.sleep(sleep)  # éviter rate limit

        return all_articles
        """
        GDELT ne supporte PAS la pagination classique.
        On simule en refaisant plusieurs requêtes avec delay.
        """
        all_articles = []

        for i in range(pages):
            params = {
                "query": f'"{query}"',
                "mode": "artlist",
                "maxrecords": max_records,
                "timespan": timespan,
                "format": "json",
                "sort": "DateDesc",
            }

            data = self._get(params)

            articles = data.get("articles", [])
            all_articles.extend(articles)

            time.sleep(sleep)  # éviter rate limit

        return all_articles

    # ------------------------
    # SENTIMENT (timeline)
    # ------------------------

    def get_sentiment(
        self,
        query: str,
        timespan: str = "7d",
    ) -> Dict[str, Any]:
        params = {
            "query": f'"{query}"',
            "mode": "timelinetone",
            "timespan": timespan,
            "format": "json",
        }

        return self._get(params)

    # ------------------------
    # SENTIMENT PAR ARTICLE
    # ------------------------

    def extract_sentiment_from_articles(self, articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Extrait les scores de sentiment de chaque article.
        GDELT fournit différents champs selon le mode utilisé.
        """
        sentiment_data = []
        
        # Debug: afficher les clés disponibles du premier article
        if articles:
            print(f"Clés disponibles dans les articles: {list(articles[0].keys())}")
        
        for article in articles:
            sentiment_info = {
                "title": article.get("title", ""),
                "url": article.get("url", ""),
                "domain": article.get("domain", ""),
                # Différents noms possibles pour les scores de sentiment
                "tone": article.get("tone", article.get("Tone", 0.0)),
                "positive_score": article.get("positive_score", article.get("Positive_Score", 0.0)),
                "negative_score": article.get("negative_score", article.get("Negative_Score", 0.0)),
                "polarity": article.get("polarity", article.get("Polarity", 0.0)),
                "activity_density": article.get("activity_density", article.get("Activity_Density", 0.0)),
                "self_references": article.get("self_references", article.get("Self_References", 0)),
                "word_count": article.get("word_count", article.get("Word_Count", 0)),
                "language": article.get("language", article.get("Language", "unknown")),
                "seendate": article.get("seendate", article.get("Seendate", "")),
                # Champs supplémentaires possibles
                "source_country": article.get("source_country", article.get("Source_Country", "")),
                "themes": article.get("themes", article.get("Themes", [])),
            }
            sentiment_data.append(sentiment_info)
        
        return sentiment_data