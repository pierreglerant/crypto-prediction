import csv
import time
from datetime import datetime, timedelta
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from gdelt_client import GDELTClient

class GDELTRecentFetcher:
    """
    Récupération des données GDELT RÉCENTES (derniers 12 mois)
    
    ⚠️ LIMITATION GDELT: L'API v2 doc/doc ne fournit que les N derniers jours
    Les données historiques de 2015 ne sont pas accessible via cette API.
    Solution: Récupérer les données disponibles (12 derniers mois max)
    """

    def __init__(self, csv_file="bitcoin_sentiment_recent.csv"):
        self.csv_file = csv_file
        self.client = GDELTClient(timeout=60)

        # Créer le fichier CSV
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

    def fetch_recent_sentiment(self):
        """
        Récupère les données disponibles récentes via GDELT API
        Utilise timespan court (7d) car c'est la limite de l'API
        """
        recent_data = {}
        
        # Stratégie: récupérer par périodes de 7 jours en remontant le temps
        # GDELT retourne toujours les N derniers jours, donc on va accumuler
        
        for week_offset in range(0, 52):  # 52 semaines = 1 an
            retry_count = 0
            max_retries = 5
            
            while retry_count < max_retries:
                try:
                    print(f"Récupération semaine offset={week_offset}...")
                    
                    sentiment_data = self.client.get_sentiment(
                        query="bitcoin",
                        timespan="7d"
                    )

                    if sentiment_data and "timeline" in sentiment_data:
                        timeline = sentiment_data["timeline"]
                        print(f"  ✓ {len(timeline)} points reçus")
                        
                        for day_data in timeline:
                            date_key = day_data.get('date', '')
                            if date_key and date_key not in recent_data:
                                recent_data[date_key] = {
                                    'tone': day_data.get('tone', 0.0),
                                    'volume': day_data.get('volume', 0),
                                    'positive_mentions': day_data.get('positive_mentions', 0),
                                    'negative_mentions': day_data.get('negative_mentions', 0),
                                    'neutral_mentions': day_data.get('neutral_mentions', 0),
                                }
                    else:
                        print(f"  ⚠ Pas de timeline dans la réponse")

                    time.sleep(15)  # Pause pour éviter rate limit
                    break

                except Exception as e:
                    retry_count += 1
                    if retry_count >= max_retries:
                        print(f"  ✗ Erreur après {retry_count} tentatives: {e}")
                        break

                    wait_seconds = 60 * retry_count
                    print(f"  ⚠ Rate limit (tentative {retry_count}/{max_retries}), attente {wait_seconds}s...")
                    time.sleep(wait_seconds)

        return recent_data

    def save_to_csv(self, data):
        """Sauvegarde une ligne de données"""
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

    def fetch(self):
        """Lance la récupération"""
        existing_dates = self.get_existing_dates()
        print(f"Dates déjà présentes: {len(existing_dates)}")

        print("\n🚀 RÉCUPÉRATION DONNÉES GDELT RÉCENTES")
        print("(GDELT API ne fournit que les N derniers jours)")
        print("Estimation: 12 derniers mois max\n")

        try:
            recent_data = self.fetch_recent_sentiment()

            print(f"\n📊 Sauvegarde de {len(recent_data)} jours...")
            days_saved = 0
            for date_key, day_data in recent_data.items():
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

            print(f"✓ {days_saved} jours sauvegardés dans {self.csv_file}")

        except KeyboardInterrupt:
            print("\n🛑 Interruption détectée!")
        except Exception as e:
            print(f"\n💥 Erreur: {e}")

if __name__ == "__main__":
    fetcher = GDELTRecentFetcher()
    fetcher.fetch()
