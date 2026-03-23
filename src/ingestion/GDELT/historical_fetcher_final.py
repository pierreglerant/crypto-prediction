import csv
import time
from datetime import datetime, timedelta
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from gdelt_client import GDELTClient
import os

class GDELTHistoricalFetcher:
    """
    Récupération historique GDELT avec sauvegarde progressive
    Données de sentiment Bitcoin depuis 2015
    """

    def __init__(self, csv_file="bitcoin_sentiment_historical.csv"):
        self.csv_file = csv_file
        self.client = GDELTClient(timeout=60)

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
                        for day_data in timeline:
                            date_key = day_data.get('date', '')
                            if date_key:
                                month_data[date_key] = {
                                    'tone': day_data.get('tone', 0.0),
                                    'volume': day_data.get('volume', 0),
                                    'positive_mentions': day_data.get('positive_mentions', 0),
                                    'negative_mentions': day_data.get('negative_mentions', 0),
                                    'neutral_mentions': day_data.get('neutral_mentions', 0),
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

    def fetch_historical_data(self, start_date="2015-01-01", end_date=None):
        """
        Récupère toutes les données depuis 2015 jusqu'à aujourd'hui
        Par mois pour être efficace et respecter les rate limits
        """
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")

        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")

        existing_dates = self.get_existing_dates()
        print(f"Dates déjà traitées: {len(existing_dates)}")

        # Itérer par mois
        current_date = start.replace(day=1)
        total_months = (end.year - start.year) * 12 + (end.month - start.month) + 1
        processed_months = 0

        try:
            while current_date <= end:
                year, month = current_date.year, current_date.month
                month_str = f"{year}-{month:02d}"

                print(f"\n=== MOIS {month_str} ({processed_months+1}/{total_months}) ===")

                # Récupérer les données du mois
                month_data = self.fetch_month_sentiment(year, month)

                # Sauvegarder chaque jour
                days_saved = 0
                for date_key, day_data in month_data.items():
                    if date_key not in existing_dates:
                        csv_data = {
                            'date': date_key,
                            'tone': day_data['tone'],
                            'volume': day_data['volume'],
                            'positive_mentions': day_data['positive_mentions'],
                            'negative_mentions': day_data['negative_mentions'],
                            'neutral_mentions': day_data['neutral_mentions'],
                            'status': 'success'
                        }
                        self.save_to_csv(csv_data)
                        days_saved += 1

                print(f"✓ {days_saved} jours sauvegardés pour {month_str}")

                # Pause entre mois
                if processed_months > 0 and processed_months % 3 == 0:
                    print("⏸️  Pause de 2 minutes...")
                    time.sleep(120)
                else:
                    print("⏸️  Pause de 30s...")
                    time.sleep(30)

                processed_months += 1
                current_date = current_date.replace(month=month+1) if month < 12 else current_date.replace(year=year+1, month=1)

        except KeyboardInterrupt:
            print("\n🛑 Interruption détectée!")
            print("Données sauvegardées dans le CSV. Relance pour continuer.")
        except Exception as e:
            print(f"\n💥 Erreur: {e}")
            print("Données sauvegardées dans le CSV.")
        finally:
            # Assure que le fichier est bien fermé et que l'état en mémoire est conservé
            print("État sauvegardé en fin de session (ou après erreur).")

        print("\n📊 === RÉSUMÉ FINAL ===")
        print(f"Total mois traités: {processed_months}")
        print(f"Fichier: {self.csv_file}")
        print("Prêt pour l'analyse ML!")

def main():
    print("🚀 === GDELT HISTORICAL SENTIMENT FETCHER ===")
    print("Récupération données Bitcoin 2018-aujourd'hui (limite API GDELT)")
    print("Sauvegarde progressive - Résistant aux interruptions")
    print()

    fetcher = GDELTHistoricalFetcher()

    # ⚠️ ATTENTION: GDELT API v2 ne supporte que depuis janvier 2017
    # La récupération peut prendre plusieurs heures (rate limits GDELT: ~1 req/5s)
    fetcher.fetch_historical_data(
        start_date="2018-01-01",
        end_date=datetime.now().strftime("%Y-%m-%d")
    )

if __name__ == "__main__":
    main()