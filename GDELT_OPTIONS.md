# Options pour Données Historiques GDELT 2015-2024

## Problem Statement
GDELT API v2 `/doc/doc` retourne uniquement les **N derniers jours** (max ~365j)
Ne peut pas requêter date spécifique arbitraire (2015-01-01, etc.)

## Solutions Possibles

### 1. **GDELT Event Database (GKG) - Archive Complète**
- URL: https://www.gdeltproject.org/data.html
- Télécharger fichiers monthly/daily historiques (2015-2024)
- Format: TSV compressé, ~1-2GB pour 10 ans
- Avantages: Données authentiques GDELT complètes
- Inconvénients: Parsing complex, gros volume, local processing

**Implémentation:**
```python
# Télécharger files from:
# https://www.googleapis.com/download/storage/v1/b/gdelt-archival/o/[YYYYMM]?alt=media
# Parser les events + extraire sentiment
```

### 2. **NewsAPI.org - Historical News (Payant)**
- API: https://newsapi.org/docs/endpoints/everything
- ~30 jours history gratuit, 1+ ans avec plan payant
- Sortmenti via TextBlob local (NLP gratuit)
- Avantages: Simple API, bonne couverture Bitcoin
- Coût: $50-200/mois pour archive complet

### 3. **Hybrid Approach: Mix de Sources**
- **2024 (Real):** GDELT API v2 direct (fetcher_recent.py)
- **2015-2024 (Synthetic):** Générer sentiment via:
  - Twitter/Reddit mentions archive (pyweet, pushshiftAPI)
  - Alternative: Corrélation simple avec prix BTC (pics = sentiment positif)

### 4. **Fichiers GDELT Raw (Complex)**
- Télécharger archives GKG monthly
- Parser TSV → extraire Bitcoin mentions
- Calcul sentiment ~ tone score
- ~4-6 heures dev + storage local

## Recommandation Rapide

**Préférence:** Option 1 (GKG) si tu veux **authentique GDELT**
- Télécharger ~2-3GB données compressées (5min)
- Parser localement (30min code)
- Avoir les vraies données GDELT 2015-2024
- Zéro coût additionnel

**Alternative:** Option 3 (Hybrid)
- GDELT récent (real) + prix BTC correlated (2015-2024)
- Plus rapide à implémenter
- Suffisant pour ML (prix = proxy du sentiment)

## Choix?
Quelle option préfères-tu?