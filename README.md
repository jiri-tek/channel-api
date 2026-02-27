# BigQuery Export Skripty

Sada Python skriptů pro export marketingových dat do BigQuery projektu `big-query-exports-488618`.

## Instalace závislostí

```bash
pip install google-cloud-bigquery requests certifi
```

## Autentizace do BigQuery

Před prvním použitím nastavte Google Cloud autentizaci:

```bash
gcloud auth application-default login
```

---

## 1. Sklik → BigQuery

**Soubor:** `sklik_to_bigquery.py`

### Konfigurace
- BigQuery dataset: `sklik_raw`
- BigQuery tabulka: `campaigns`
- API klíč: hardcoded v souboru (řádek 7)

### Použití

```bash
# Export včerejších dat
python sklik_to_bigquery.py
```

### Co exportuje
- 19 klientských účtů ze Skliku
- Metriky: spend, impressions, clicks, conversions, CPC, CTR
- Granularita: denní

---

## 2. Meta Ads → BigQuery

**Soubor:** `meta_to_bigquery.py`

### Konfigurace
- BigQuery dataset: `meta_ads_raw`
- BigQuery tabulka: `campaigns`
- Access token: načítá z proměnné prostředí `META_ACCESS_TOKEN`

### Nastavení access tokenu

```bash
export META_ACCESS_TOKEN="your_meta_access_token_here"
```

### Použití

```bash
# Export včerejších dat
python meta_to_bigquery.py

# Export konkrétního dne
python meta_to_bigquery.py --date 2026-02-20
```

### Co exportuje
- 12 Meta Ads účtů
- Metriky: spend, impressions, reach, clicks, CPC, CTR, CPM, frequency, purchases, ROAS, adds to cart, checkouts, outbound clicks, view content
- Level: campaign
- Granularita: denní

### Účty
```
barberco.cz, prager.cz, nakupujdrevo.online, stary-vrch.cz,
insta360.cz, cihlomat.cz, rnp-nk41, rnp-vpk, resort-na-pasece.cz,
zlatakrasa.cz, fotocopy.sk, niftyminds.cz
```

---

## 3. Shoptet → BigQuery

**Soubor:** `shoptet_to_bigquery.py`

### Konfigurace
- BigQuery dataset: `shoptet_raw`
- BigQuery tabulka: `orders`
- Autentizace: **Private API Token** (hardcoded v souboru, řádky 17-32)

### Nastavení tokenů

**Pro shopy s API přístupem:**
1. Přihlaste se do Shoptet Admin → **Aplikace** → **API**
2. Vygenerujte **Private API Token**
3. Vložte token do `shoptet_to_bigquery.py`:
   ```python
   "shop-name.cz": {
       "method": "api",
       "token": "váš-token-zde"
   }
   ```

**Pro shopy bez API přístupu:**
- Použijte CSV export (zatím neimplementováno)
- Doporučujeme získat API přístup (standardizované schéma pro všechny Shoptet shopy)

### Použití

```bash
# Export včerejších objednávek
python shoptet_to_bigquery.py

# Export objednávek z konkrétního dne
python shoptet_to_bigquery.py --date 2026-02-20
```

### Co exportuje
- 4 Shoptet shopy
- Pouze objednávky se statusem `paid` nebo `dispatched`
- Data: datum, ID, kód objednávky, ceny, email zákazníka, město, typ zákazníka

### Shopy
```
zlatakrasa.cz, prager.cz, nakupujdrevo.online, stary-vrch.cz
```

---

## 4. Shopify → BigQuery

**Soubor:** `shopify_to_bigquery.py`

### Konfigurace
- BigQuery dataset: `shopify_raw`
- BigQuery tabulka: `orders`
- Autentizace: **Private App Access Token** (v souboru)

### Nastavení tokenu

**Jak získat Shopify access token:**

1. Přihlaste se do **Shopify Admin**
2. Jděte na **Settings** → **Apps and sales channels** → **Develop apps**
3. Klikněte **"Create an app"** nebo **"Allow custom app development"** (pokud je první)
4. Pojmenujte app: "BigQuery Export"
5. V **Configuration** → **Admin API integration**:
   - Zaškrtněte: `read_orders`, `read_customers`
6. Klikněte **"Save"** a pak **"Install app"**
7. Zkopírujte **"Admin API access token"** (začíná `shpat_...`)

**Vložte do souboru:**
```bash
cp shopify_credentials.json.example shopify_credentials.json
```

Upravte soubor:
```json
{
  "myshop.myshopify.com": {
    "shop_url": "myshop.myshopify.com",
    "access_token": "shpat_xxxxx..."
  }
}
```

### Použití

```bash
# Export včerejších objednávek
python shopify_to_bigquery.py

# Export objednávek z konkrétního dne
python shopify_to_bigquery.py --date 2026-02-20
```

### Co exportuje
- Pouze objednávky se statusem `paid`, `authorized`, `partially_paid`
- Data: datum, ID, order number, status, ceny, email zákazníka, město, typ zákazníka

---

## Prevence duplicit

Všechny skripty automaticky **mažou existující záznamy** pro dané datum a účet před insertem nových dat. To umožňuje bezpečné opakované spouštění (např. pokud se data aktualizují).

---

## Automatizace (cron)

Pro denní automatický export přidejte do cronu:

```bash
# Denní export v 6:00 ráno
0 6 * * * cd /Users/jiristaffa/Documents/vibecoding && python sklik_to_bigquery.py
0 6 * * * cd /Users/jiristaffa/Documents/vibecoding && export META_ACCESS_TOKEN="..." && python meta_to_bigquery.py
0 6 * * * cd /Users/jiristaffa/Documents/vibecoding && python shoptet_to_bigquery.py
```

---

## Troubleshooting

### BigQuery autentizace chyba
```
google.auth.exceptions.DefaultCredentialsError
```
**Řešení:** Spusťte `gcloud auth application-default login`

### Meta API chyba
```
CHYBA: Nastavte proměnnou prostředí META_ACCESS_TOKEN
```
**Řešení:** `export META_ACCESS_TOKEN="your_token"`

### Shoptet credentials chyba
```
CHYBA: Soubor shoptet_credentials.json nenalezen
```
**Řešení:** Vytvořte soubor podle `shoptet_credentials.json.example`

---

## Struktura BigQuery tabulek

### sklik_raw.campaigns
```sql
date, account_id, account_name, campaign_id, campaign_name,
spend, impressions, clicks, conversions, cpc, ctr
```

### meta_ads_raw.campaigns
```sql
date, account_id, account_name, campaign_id, campaign_name,
spend, impressions, reach, clicks, cpc, ctr, cpm, frequency,
purchases, purchase_roas, adds_to_cart, checkouts_initiated,
outbound_clicks, view_content, _imported_at
```

### shoptet_raw.orders
```sql
order_date, order_id, account_name, order_code, status,
total_price_with_vat, total_price_without_vat,
customer_email, delivery_city, customer_type, _imported_at
```
