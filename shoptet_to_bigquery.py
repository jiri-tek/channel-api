import requests
import json
import sys
import argparse
from google.cloud import bigquery
from datetime import date, timedelta, datetime

# ── Konfigurace ──────────────────────────────────────────────
BQ_PROJECT = "big-query-exports-488618"
BQ_DATASET = "shoptet_raw"
BQ_TABLE = "orders"

CREDENTIALS_FILE = "shoptet_credentials.json"

# Shoptet klienti
SHOPTET_CLIENTS = [
    "zlatakrasa.cz",
    "prager.cz",
    "nakupujdrevo.online",
    "stary-vrch.cz"
]

def load_credentials():
    """Načte Shoptet credentials ze souboru"""
    try:
        with open(CREDENTIALS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"CHYBA: Soubor {CREDENTIALS_FILE} nenalezen")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"CHYBA: Nelze parsovat {CREDENTIALS_FILE}: {e}")
        sys.exit(1)

def get_access_token(shop_name, credentials):
    """Získá OAuth2 access token pro Shoptet API"""
    creds = credentials.get(shop_name)
    if not creds:
        print(f"  Chyba: Nenalezeny credentials pro {shop_name}")
        return None

    client_id = creds.get("client_id")
    client_secret = creds.get("client_secret")

    if not client_id or not client_secret:
        print(f"  Chyba: Neúplné credentials pro {shop_name}")
        return None

    # Shoptet OAuth2 token endpoint
    token_url = "https://shoptet.cz/action/ApiOAuthServer/token"

    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret
    }

    try:
        response = requests.post(token_url, data=data)
        response.raise_for_status()
        token_data = response.json()
        return token_data.get("access_token")
    except requests.exceptions.RequestException as e:
        print(f"  Chyba při získávání access tokenu pro {shop_name}: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"  Chyba při parsování token response pro {shop_name}: {e}")
        return None

def get_shop_url(shop_name):
    """Vrátí URL pro Shoptet API daného shopu"""
    # Odstranit .cz z názvu pokud je přítomno
    base_name = shop_name.replace(".cz", "").replace(".", "-")
    return f"https://{base_name}.myshoptet.com"

def get_orders(shop_name, access_token, date_from, date_to):
    """Načte objednávky ze Shoptet API pro dané období"""
    shop_url = get_shop_url(shop_name)
    api_url = f"{shop_url}/api/orders"

    headers = {
        "Shoptet-Access-Token": access_token,
        "Content-Type": "application/vnd.shoptet.v1.0+json"
    }

    # Převést datum na datetime formát pro API
    date_from_dt = datetime.strptime(date_from, "%Y-%m-%d")
    date_to_dt = datetime.strptime(date_to, "%Y-%m-%d")

    # Shoptet API očekává ISO 8601 formát
    params = {
        "creationTimeFrom": date_from_dt.strftime("%Y-%m-%dT00:00:00+00:00"),
        "creationTimeTo": date_to_dt.strftime("%Y-%m-%dT23:59:59+00:00")
    }

    all_orders = []

    try:
        response = requests.get(api_url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()

        if "data" not in data:
            print(f"  Neočekávaná struktura odpovědi: {data}")
            return []

        orders = data["data"].get("orders", [])

        # Filtrovat pouze paid nebo dispatched objednávky
        filtered_orders = [
            order for order in orders
            if order.get("status", {}).get("id") in ["paid", "dispatched"]
        ]

        all_orders.extend(filtered_orders)

        # Shoptet API používá paginaci
        while "paginator" in data["data"] and data["data"]["paginator"].get("nextPage"):
            next_page_url = data["data"]["paginator"]["nextPage"]
            response = requests.get(next_page_url, headers=headers)
            response.raise_for_status()
            data = response.json()

            orders = data["data"].get("orders", [])
            filtered_orders = [
                order for order in orders
                if order.get("status", {}).get("id") in ["paid", "dispatched"]
            ]

            all_orders.extend(filtered_orders)

        return all_orders

    except requests.exceptions.RequestException as e:
        print(f"  Chyba při volání API pro {shop_name}: {e}")
        return []
    except (json.JSONDecodeError, KeyError) as e:
        print(f"  Chyba při parsování dat pro {shop_name}: {e}")
        return []

def process_orders(orders, shop_name):
    """Zpracuje objednávky do formátu pro BigQuery"""
    rows = []

    for order in orders:
        # Extrahovat potřebná data
        creation_time = order.get("creationTime", "")
        order_date = creation_time.split("T")[0] if creation_time else ""

        price = order.get("price", {})
        customer = order.get("customer", {})
        billing_address = order.get("billingAddress", {})

        row = {
            "order_date": order_date,
            "order_id": str(order.get("id", "")),
            "account_name": shop_name,
            "order_code": order.get("code", ""),
            "status": order.get("status", {}).get("id", ""),
            "total_price_with_vat": round(float(price.get("withVat", 0)), 2),
            "total_price_without_vat": round(float(price.get("withoutVat", 0)), 2),
            "customer_email": customer.get("email", ""),
            "delivery_city": billing_address.get("city", ""),
            "customer_type": "company" if customer.get("company") else "individual",
            "_imported_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        rows.append(row)

    return rows

def delete_existing_data(client, date_str, shop_name):
    """Smaže existující záznamy pro dané datum a shop"""
    table_ref = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    query = f"""
    DELETE FROM `{table_ref}`
    WHERE order_date = '{date_str}' AND account_name = '{shop_name}'
    """

    try:
        query_job = client.query(query)
        query_job.result()
    except Exception as e:
        print(f"  Varování: Nelze smazat existující data: {e}")

def upload_to_bigquery(rows, date_str, shop_name):
    """Nahraje data do BigQuery"""
    client = bigquery.Client(project=BQ_PROJECT)
    table_ref = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    # Smazat existující data pro tento den a shop
    delete_existing_data(client, date_str, shop_name)

    # Nahrát nová data
    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        print(f"  BQ chyby: {errors}")
    else:
        print(f"  Nahráno {len(rows)} objednávek do BigQuery")

def main():
    parser = argparse.ArgumentParser(description="Export Shoptet objednávek do BigQuery")
    parser.add_argument("--date", help="Datum ve formátu YYYY-MM-DD (výchozí: včerejšek)")
    args = parser.parse_args()

    # Určit datum
    if args.date:
        date_from = args.date
        date_to = args.date
    else:
        yesterday = date.today() - timedelta(days=1)
        date_from = yesterday.strftime("%Y-%m-%d")
        date_to = date_from

    print(f"Shoptet -> BigQuery | {date_from}")

    # Načíst credentials
    credentials = load_credentials()

    all_orders_count = 0

    for shop_name in SHOPTET_CLIENTS:
        print(f"Stahuji: {shop_name}")

        # Získat access token
        access_token = get_access_token(shop_name, credentials)
        if not access_token:
            print(f"  Přeskočeno (chyba autentizace)")
            continue

        # Načíst objednávky
        orders = get_orders(shop_name, access_token, date_from, date_to)

        if not orders:
            print(f"  0 objednávek")
            continue

        # Zpracovat a nahrát
        rows = process_orders(orders, shop_name)
        print(f"  {len(rows)} objednávek")

        if rows:
            upload_to_bigquery(rows, date_from, shop_name)
            all_orders_count += len(rows)

    print(f"\nCelkem zpracováno {all_orders_count} objednávek")
    print("Hotovo!")

if __name__ == "__main__":
    main()
