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

def load_credentials():
    """Načte Shoptet OAuth credentials ze souboru"""
    try:
        with open(CREDENTIALS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"CHYBA: Soubor {CREDENTIALS_FILE} nenalezen")
        print(f"Vytvořte ho podle vzoru shoptet_credentials.json.example")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"CHYBA: Nelze parsovat {CREDENTIALS_FILE}: {e}")
        sys.exit(1)

def get_oauth_token(shop_name, credentials):
    """Získá OAuth access token pomocí Client Credentials flow"""
    creds = credentials.get(shop_name)
    if not creds:
        print(f"  Chyba: Nenalezeny credentials pro {shop_name}")
        return None, None

    client_id = creds.get("client_id")
    client_secret = creds.get("client_secret")
    eshop_url = creds.get("eshop_url")

    if not client_id or not client_secret or not eshop_url:
        print(f"  Chyba: Neúplné credentials pro {shop_name}")
        return None, None

    # Shoptet OAuth token endpoint
    token_url = f"{eshop_url}/action/OAuthServer/token"

    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "api"
    }

    try:
        response = requests.post(token_url, data=data)

        if response.status_code != 200:
            try:
                error = response.json()
                print(f"  OAuth chyba pro {shop_name}: {error}")
            except:
                print(f"  OAuth chyba pro {shop_name}: {response.status_code} - {response.text[:200]}")
            return None, None

        token_data = response.json()
        access_token = token_data.get("access_token")

        if not access_token:
            print(f"  Chyba: Access token nebyl vrácen pro {shop_name}")
            return None, None

        return access_token, eshop_url

    except requests.exceptions.RequestException as e:
        print(f"  Chyba při získávání OAuth tokenu pro {shop_name}: {e}")
        return None, None

def get_orders_from_api(shop_name, access_token, eshop_url, date_from, date_to):
    """Načte objednávky ze Shoptet API pomocí OAuth token"""

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/vnd.shoptet.v1.0+json"
    }

    # Převést datum na datetime formát pro API
    date_from_dt = datetime.strptime(date_from, "%Y-%m-%d")
    date_to_dt = datetime.strptime(date_to, "%Y-%m-%d")

    params = {
        "creationTimeFrom": date_from_dt.strftime("%Y-%m-%dT00:00:00+00:00"),
        "creationTimeTo": date_to_dt.strftime("%Y-%m-%dT23:59:59+00:00")
    }

    api_url = "https://api.myshoptet.com/api/orders"
    all_orders = []

    try:
        response = requests.get(api_url, headers=headers, params=params)

        if response.status_code != 200:
            try:
                error_data = response.json()
                print(f"  API chyba: {error_data}")
            except:
                print(f"  API chyba: {response.status_code} - {response.text[:200]}")
            return []

        data = response.json()

        if "data" not in data or "orders" not in data["data"]:
            print(f"  Neočekávaná struktura odpovědi")
            return []

        orders = data["data"]["orders"]

        # Filtrovat pouze paid nebo dispatched objednávky
        filtered_orders = [
            order for order in orders
            if order.get("paid") == True or order.get("status", {}).get("id") in [18, 19, 20]
        ]

        all_orders.extend(filtered_orders)

        # Shoptet API používá paginaci
        paginator = data["data"].get("paginator", {})
        while paginator.get("nextPage"):
            response = requests.get(paginator["nextPage"], headers=headers)
            if response.status_code != 200:
                break

            data = response.json()
            orders = data["data"].get("orders", [])
            filtered_orders = [
                order for order in orders
                if order.get("paid") == True or order.get("status", {}).get("id") in [18, 19, 20]
            ]
            all_orders.extend(filtered_orders)
            paginator = data["data"].get("paginator", {})

        return all_orders

    except requests.exceptions.RequestException as e:
        print(f"  Chyba při volání API: {e}")
        return []
    except (json.JSONDecodeError, KeyError) as e:
        print(f"  Chyba při parsování dat: {e}")
        return []

def process_orders(orders, shop_name):
    """Zpracuje objednávky do formátu pro BigQuery"""
    rows = []

    for order in orders:
        # Extrahovat potřebná data
        creation_time = order.get("creationTime", "")
        order_date = creation_time.split("T")[0] if creation_time else ""

        price = order.get("price", {})
        email = order.get("email", "")

        # Billing address pro město
        billing_address = order.get("billingAddress", {})
        delivery_city = billing_address.get("city", "")

        # Company = typ zákazníka
        company = order.get("company")
        customer_type = "company" if company else "individual"

        row = {
            "order_date": order_date,
            "order_id": str(order.get("guid", "")),
            "account_name": shop_name,
            "order_code": order.get("code", ""),
            "status": order.get("status", {}).get("name", ""),
            "total_price_with_vat": round(float(price.get("withVat", 0)), 2),
            "total_price_without_vat": round(float(price.get("withoutVat", 0)), 2),
            "customer_email": email,
            "delivery_city": delivery_city,
            "customer_type": customer_type,
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

    for shop_name in credentials.keys():
        print(f"Stahuji: {shop_name}")

        # Získat OAuth access token
        access_token, eshop_url = get_oauth_token(shop_name, credentials)
        if not access_token:
            print(f"  Přeskočeno (OAuth chyba)")
            continue

        # Načíst objednávky
        orders = get_orders_from_api(shop_name, access_token, eshop_url, date_from, date_to)

        if not orders:
            print(f"  0 objednávek")
            continue

        # Zpracovat a nahrát
        rows = process_orders(orders, shop_name)
        print(f"  {len(rows)} objednávek (zaplacené/vyřízené)")

        if rows:
            upload_to_bigquery(rows, date_from, shop_name)
            all_orders_count += len(rows)

    print(f"\nCelkem zpracováno {all_orders_count} objednávek")
    print("Hotovo!")

if __name__ == "__main__":
    main()
