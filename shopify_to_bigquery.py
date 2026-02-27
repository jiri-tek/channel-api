import requests
import json
import sys
import argparse
from google.cloud import bigquery
from datetime import date, timedelta, datetime

# ── Konfigurace ──────────────────────────────────────────────
BQ_PROJECT = "big-query-exports-488618"
BQ_DATASET = "shopify_raw"
BQ_TABLE = "orders"

CREDENTIALS_FILE = "shopify_credentials.json"

def load_credentials():
    """Načte Shopify credentials ze souboru"""
    try:
        with open(CREDENTIALS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"CHYBA: Soubor {CREDENTIALS_FILE} nenalezen")
        print(f"Vytvořte ho podle vzoru shopify_credentials.json.example")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"CHYBA: Nelze parsovat {CREDENTIALS_FILE}: {e}")
        sys.exit(1)

def get_orders_from_api(shop_name, shop_url, access_token, date_from, date_to):
    """Načte objednávky ze Shopify API"""

    headers = {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json"
    }

    # Převést datum na ISO 8601 formát pro Shopify API
    date_from_dt = datetime.strptime(date_from, "%Y-%m-%d")
    date_to_dt = datetime.strptime(date_to, "%Y-%m-%d")

    params = {
        "created_at_min": date_from_dt.strftime("%Y-%m-%dT00:00:00+00:00"),
        "created_at_max": date_to_dt.strftime("%Y-%m-%dT23:59:59+00:00"),
        "status": "any",  # Všechny statusy, filtrujeme později
        "limit": 250  # Max limit pro Shopify API
    }

    # Shopify API endpoint - používáme nejnovější stable verzi
    api_url = f"https://{shop_url}/admin/api/2024-01/orders.json"
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

        if "orders" not in data:
            print(f"  Neočekávaná struktura odpovědi")
            return []

        orders = data["orders"]

        # Filtrovat pouze uzavřené/zaplacené objednávky
        # Shopify financial_status: paid, pending, authorized, partially_paid, refunded, voided
        # fulfillment_status: fulfilled, partial, null (nezpracované)
        filtered_orders = [
            order for order in orders
            if order.get("financial_status") in ["paid", "authorized", "partially_paid"]
        ]

        all_orders.extend(filtered_orders)

        # Shopify používá link-based pagination
        # Zkontrolovat Link header pro další stránku
        if "Link" in response.headers:
            links = response.headers["Link"]
            # Parsovat Link header: <url>; rel="next"
            for link in links.split(","):
                if 'rel="next"' in link:
                    next_url = link.split(";")[0].strip("<>")
                    # Rekurzivně získat další stránky
                    while next_url:
                        response = requests.get(next_url, headers=headers)
                        if response.status_code != 200:
                            break

                        data = response.json()
                        orders = data.get("orders", [])
                        filtered_orders = [
                            order for order in orders
                            if order.get("financial_status") in ["paid", "authorized", "partially_paid"]
                        ]
                        all_orders.extend(filtered_orders)

                        # Zkontrolovat další stránku
                        if "Link" in response.headers:
                            links = response.headers["Link"]
                            next_url = None
                            for link in links.split(","):
                                if 'rel="next"' in link:
                                    next_url = link.split(";")[0].strip("<>")
                                    break
                        else:
                            next_url = None

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
        created_at = order.get("created_at", "")
        order_date = created_at.split("T")[0] if created_at else ""

        # Email zákazníka
        customer = order.get("customer", {})
        email = customer.get("email", "") if customer else order.get("email", "")

        # Billing/Shipping address pro město
        billing_address = order.get("billing_address", {})
        shipping_address = order.get("shipping_address", {})
        delivery_city = shipping_address.get("city", "") if shipping_address else billing_address.get("city", "")

        # Company = typ zákazníka
        company = billing_address.get("company", "") if billing_address else ""
        customer_type = "company" if company else "individual"

        # Ceny - Shopify vrací jako string
        total_price = float(order.get("total_price", 0))
        total_tax = float(order.get("total_tax", 0))
        total_without_tax = total_price - total_tax

        row = {
            "order_date": order_date,
            "order_id": str(order.get("id", "")),
            "account_name": shop_name,
            "order_code": order.get("name", ""),  # Shopify používá "name" jako order number (např. #1001)
            "status": order.get("financial_status", ""),
            "total_price_with_vat": round(total_price, 2),
            "total_price_without_vat": round(total_without_tax, 2),
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
    parser = argparse.ArgumentParser(description="Export Shopify objednávek do BigQuery")
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

    print(f"Shopify -> BigQuery | {date_from}")

    # Načíst credentials
    credentials = load_credentials()

    all_orders_count = 0

    for shop_name, config in credentials.items():
        print(f"Stahuji: {shop_name}")

        shop_url = config.get("shop_url")
        access_token = config.get("access_token")

        if not shop_url or not access_token:
            print(f"  Přeskočeno (neúplné credentials)")
            continue

        # Načíst objednávky
        orders = get_orders_from_api(shop_name, shop_url, access_token, date_from, date_to)

        if not orders:
            print(f"  0 objednávek")
            continue

        # Zpracovat a nahrát
        rows = process_orders(orders, shop_name)
        print(f"  {len(rows)} objednávek (zaplacené)")

        if rows:
            upload_to_bigquery(rows, date_from, shop_name)
            all_orders_count += len(rows)

    print(f"\nCelkem zpracováno {all_orders_count} objednávek")
    print("Hotovo!")

if __name__ == "__main__":
    main()
