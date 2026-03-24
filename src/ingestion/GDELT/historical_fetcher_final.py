import csv
import time
from datetime import datetime, timedelta
import sys
import os
import requests

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from gdelt_client import GDELTClient
import os

class GDELTHistoricalFetcher:
    """
    Récupération historique GDELT avec sauvegarde progressive
    Remonte le temps depuis aujourd'hui jusqu'au point où les données ne sont plus disponibles
    GDELT API limite: 1 requête par 5 secondes
    """

    def __init__(self, csv_file="bitcoin_sentiment_historical.csv"):
        self.csv_file = csv_file
        self.client = GDELTClient(timeout=60)
        self.min_request_interval = 5.0  # API requiert minimum 5s entre requêtes

        # Créer le fichier CSV avec headers si inexistant
        if not os.path.exists(csv_file):
            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'date', 'tone', 'volume', 'positive_mentions',
                    'negative_mentions', 'neutral_mentions', 'status'
                ])

    def get_existing_dates(self):
        """Récupère les dates déjà traitées"""
        existing_dates = set()
        if os.path.exists(self.csv_file):
            with open(self.csv_file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader, None)  # Skip header
                for row in reader:
                    if row and row[0] != 'date':
                        existing_dates.add(row[0])
        return existing_dates

    def fetch_month_sentiment(self, year, month):
        """
        Récupère le sentiment pour un mois entier via périodes de 7 jours
        """
        from calendar import monthrange
        first_day = datetime(year, month, 1)
        last_day = datetime(year, month, monthrange(year, month)[1])

        month_data = {}

        # Diviser le mois en périodes de 7 jours
        current_date = first_day
        while current_date <= last_day:
            period_end = min(current_date + timedelta(days=6), last_day)

            # Boucle de retry en cas de rate limit / erreurs réseau
            period_attempt = 0
            max_period_attempts = 7
            backoff_base = 60  # 60s initial backoff
            
            while period_attempt < max_period_attempts:
                try:
                    # Récupérer 7 jours de données
                    sentiment_data = self.client.get_sentiment(
                        query="bitcoin",
                        timespan="7d"
                    )

                    if sentiment_data and "timeline" in sentiment_data:
                        timeline = sentiment_data["timeline"]
                        # Timeline contains series objects, each with a 'data' array
                        for series_obj in timeline:
                            if 'data' not in series_obj:
                                continue
                            # Each data point has: {'date': 'YYYYMMDDTHHMMSSZ', 'value': float}
                            for data_point in series_obj['data']:
                                date_key = data_point.get('date', '')
                                if date_key:
                                    month_data[date_key] = {
                                        'tone': data_point.get('value', 0.0),  # 'value' = tone score
                                        'volume': 0,  # Not available in timelinetone API
                                        'positive_mentions': 0,  # Not available
                                        'negative_mentions': 0,  # Not available
                                        'neutral_mentions': 0,  # Not available
                                    }

                    time.sleep(5)  # Pause entre requêtes
                    break

                except Exception as e:
                    period_attempt += 1
                    if period_attempt >= max_period_attempts:
                        print(f"⚠️  Erreur semaine {current_date.strftime('%Y-%m-%d')} après {period_attempt} tentatives: {str(e)[:100]}")
                        break

                    # Exponential backoff: 60s, 120s, 240s, 480s ...
                    wait_seconds = backoff_base * (2 ** (period_attempt - 1))
                    wait_seconds = min(wait_seconds, 600)  # Cap at 10 minutes
                    print(f"🔄 Rate limit/timeout {current_date.strftime('%Y-%m-%d')} (tentative {period_attempt}/{max_period_attempts}), attente {wait_seconds}s...")
                    time.sleep(wait_seconds)

            current_date = period_end + timedelta(days=1)

        return month_data

    def save_to_csv(self, data):
        """Sauvegarde une ligne de données dans le CSV"""
        with open(self.csv_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                data['date'],
                data['tone'],
                data['volume'],
                data['positive_mentions'],
                data['negative_mentions'],
                data['neutral_mentions'],
                data['status']
            ])

    def fetch_with_date_range(self, start_date, end_date):
        """
        Récupère sentiment avec plage de dates précises (format: YYYYMMDDHHMMSS)
        GDELT API limite: dates doivent être dans les 3 derniers mois max
        """
        try:
            time.sleep(self.min_request_interval)
            sentiment_data = self.client.get_sentiment(
                query="bitcoin",
                start_datetime=start_date,  # YYYYMMDDHHMMSS
                end_datetime=end_date       # YYYYMMDDHHMMSS
            )
            return sentiment_data
        except Exception as e:
            print(f"   Erreur [{start_date} -> {end_date}]: {str(e)[:100]}")
            return None

    def fetch_historical_data(self, start_date="2017-01-01", end_date=None):
        """
        Récupère données en remontant depuis aujourd'hui dans les 3 derniers mois
        (limitation API: GDELT DOC API ne supporte que les 3 derniers mois)
        """
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")

        end = datetime.strptime(end_date, "%Y-%m-%d")
        start_limit = end - timedelta(days=90)  # 3 mois = ~90 jours max
        
        # Ne pas dépasser la limite
        start = max(datetime.strptime(start_date, "%Y-%m-%d"), start_limit)

        existing_dates = self.get_existing_dates()
        print(f"Dates déjà traitées: {len(existing_dates)}")
        print(f"⚠️  Limite API GDELT: max 3 mois historiques")
        print(f"   Plage: {start.strftime('%Y-%m-%d')} → {end.strftime('%Y-%m-%d')}")

        # Récupérer jour par jour dans les 3 derniers mois
        current_date = start
        day_num = 0

        try:
            while current_date <= end:
                year, month, day = current_date.year, current_date.month, current_date.day
                day_str = f"{year}-{month:02d}-{day:02d}"

                # Format GDELT: YYYYMMDDHHMMSS
                start_dt = f"{year}{month:02d}{day:02d}000000"
                end_dt = f"{year}{month:02d}{day:02d}235959"

                day_num += 1
                print(f"\n📅 Jour {day_num}: {day_str}")

                sentiment_data = self.fetch_with_date_range(start_dt, end_dt)

                days_saved = 0
                if sentiment_data and "timeline" in sentiment_data:
                    timeline = sentiment_data["timeline"]
                    for series_obj in timeline:
                        if 'data' not in series_obj:
                            continue
                        for data_point in series_obj['data']:
                            date_key = data_point.get('date', '')
                            if date_key and date_key not in existing_dates:
                                csv_data = {
                                    'date': date_key,
                                    'tone': data_point.get('value', 0.0),
                                    'volume': 0,
                                    'positive_mentions': 0,
                                    'negative_mentions': 0,
                                    'neutral_mentions': 0,
                                    'status': 'success'
                                }
                                self.save_to_csv(csv_data)
                                existing_dates.add(date_key)
                                days_saved += 1
                
                print(f"   ✓ {days_saved} points temps sauvegardés")

                current_date += timedelta(days=1)

        except KeyboardInterrupt:
            print("\n🛑 Interruption détectée!")
            print("Données sauvegardées dans le CSV. Relance pour continuer.")
        except Exception as e:
            print(f"\n💥 Erreur: {e}")
            print("Données sauvegardées dans le CSV.")
        finally:
            print("État sauvegardé en fin de session (ou après erreur).")

        print("\n📊 === RÉSUMÉ FINAL ===")
        print(f"Total jours traités: {day_num}")
        print(f"Fichier: {self.csv_file}")
        print(f"Nombre de points temps avec données: {len(existing_dates)}")
        print("✓ Les 3 derniers mois de données Bitcoin sauvegardés!")

def main():
    print("🚀 === GDELT HISTORICAL SENTIMENT FETCHER ===")
    print("Récupération données Bitcoin: remonte depuis aujourd'hui")
    print("Arrête cuando les données ne sont plus disponibles ou timeout")
    print("Respect de la limite API: 1 requête par 5 secondes minimum")
    print()

    fetcher = GDELTHistoricalFetcher()

    # Remonte depuis aujourd'hui vers 2017 (limite GDELT API)
    fetcher.fetch_historical_data(
        start_date="2018-01-01",
        end_date=datetime.now().strftime("%Y-%m-%d")
    )

if __name__ == "__main__":
    main()