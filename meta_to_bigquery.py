import requests
import os
import sys
import argparse
from google.cloud import bigquery
from datetime import date, datetime, timedelta

# ── Konfigurace ──────────────────────────────────────────────
META_ACCESS_TOKEN = "EAAXn4ZBx55UUBQ1TozEwzUeZC1Vv90WUrdlySBtnZB9E4oYaA0fpu6TQGSCpZCgw6YeU5S3bgF9NRScEejRPs7YBX0618ojJhBcJVZAxmx9jTZA0ZCRbJH4L3VS46R107ONVCC9A2qQBIqnwJtSRCZCpkblZCoXMp3AhxO94vG7vC1W0eZBEXayDGwrI4lSiSzUPmZAOQpr"
BQ_PROJECT = "big-query-exports-488618"
BQ_DATASET = "meta_ads_raw"
BQ_TABLE = "campaigns"

# Meta účty
META_ACCOUNTS = {
    "act_144231553812024": "barberco.cz",
    "act_1658402667697463": "prager.cz",
    "act_1459578055242107": "nakupujdrevo.online",
    "act_2578290482474113": "stary-vrch.cz",
    "act_218070523781008": "insta360.cz",
    "act_233095930410520": "cihlomat.cz",
    "act_1998472290650240": "rnp-nk41",
    "act_591256823887994": "rnp-vpk",
    "act_816380312985902": "resort-na-pasece.cz",
    "act_1520244194814277": "zlatakrasa.cz",
    "act_592366144608183": "fotocopy.sk",
    "act_747790166942878": "niftyminds.cz",
}

# Metriky k načtení z Meta API
METRICS = [
    "spend", "impressions", "reach", "clicks", "cpc", "ctr", "cpm", "frequency",
    "purchases", "purchase_roas", "actions"  # actions obsahuje adds_to_cart, checkouts_initiated atd.
]

# Action types pro conversion metriky
ACTION_TYPES = {
    "omni_purchase": "purchases",
    "omni_add_to_cart": "adds_to_cart",
    "omni_initiated_checkout": "checkouts_initiated",
    "outbound_click": "outbound_clicks",
    "omni_view_content": "view_content"
}

def get_meta_campaigns(account_id, account_name, access_token, date_from, date_to):
    """Načte kampaně z Meta Ads API pro dané období"""
    url = f"https://graph.facebook.com/v19.0/{account_id}/insights"

    params = {
        "access_token": access_token,
        "level": "campaign",
        "time_increment": 1,
        "time_range": f'{{"since":"{date_from}","until":"{date_to}"}}',
        "fields": ",".join([
            "campaign_id", "campaign_name", "spend", "impressions", "reach",
            "clicks", "cpc", "ctr", "cpm", "frequency", "purchase_roas",
            "actions", "outbound_clicks"
        ]),
        "limit": 500
    }

    try:
        response = requests.get(url, params=params)

        # Pokud je chyba, vypsat detaily
        if response.status_code != 200:
            try:
                error_data = response.json()
                print(f"  Chyba API pro {account_name}: {error_data}")
            except:
                print(f"  Chyba API pro {account_name}: {response.status_code} - {response.text[:200]}")
            return []

        data = response.json()

        if "error" in data:
            print(f"  Chyba API pro {account_name}: {data['error'].get('message', data['error'])}")
            return []

        campaigns = data.get("data", [])

        # Procházet paginaci pokud existuje
        while "paging" in data and "next" in data["paging"]:
            response = requests.get(data["paging"]["next"])
            if response.status_code != 200:
                break
            data = response.json()
            campaigns.extend(data.get("data", []))

        return campaigns

    except requests.exceptions.RequestException as e:
        print(f"  Chyba při volání API pro {account_name}: {e}")
        return []

def parse_actions(actions, action_type):
    """Extrahuje hodnotu z actions pole podle action_type"""
    if not actions:
        return 0

    for action in actions:
        if action.get("action_type") == action_type:
            return int(float(action.get("value", 0)))

    return 0

def process_campaigns(campaigns, account_id, account_name, date_str):
    """Zpracuje kampaně z Meta API do formátu pro BigQuery"""
    rows = []

    for camp in campaigns:
        actions = camp.get("actions", [])
        outbound_clicks_data = camp.get("outbound_clicks", [])

        # Extrahovat conversion metriky z actions
        purchases = parse_actions(actions, "omni_purchase")
        adds_to_cart = parse_actions(actions, "omni_add_to_cart")
        checkouts_initiated = parse_actions(actions, "omni_initiated_checkout")
        view_content = parse_actions(actions, "omni_view_content")

        # Outbound clicks
        outbound_clicks = 0
        if outbound_clicks_data:
            outbound_clicks = int(float(outbound_clicks_data[0].get("value", 0)))

        row = {
            "date": date_str,
            "account_id": account_id,
            "account_name": account_name,
            "campaign_id": camp.get("campaign_id"),
            "campaign_name": camp.get("campaign_name"),
            "spend": round(float(camp.get("spend", 0)), 2),
            "impressions": int(camp.get("impressions", 0)),
            "reach": int(camp.get("reach", 0)),
            "clicks": int(camp.get("clicks", 0)),
            "cpc": round(float(camp.get("cpc", 0)), 2),
            "ctr": round(float(camp.get("ctr", 0)), 2),
            "cpm": round(float(camp.get("cpm", 0)), 2),
            "frequency": round(float(camp.get("frequency", 0)), 2),
            "purchases": purchases,
            "purchase_roas": round(float(camp.get("purchase_roas", [{}])[0].get("value", 0) if camp.get("purchase_roas") else 0), 2),
            "adds_to_cart": adds_to_cart,
            "checkouts_initiated": checkouts_initiated,
            "outbound_clicks": outbound_clicks,
            "view_content": view_content,
            "_imported_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        rows.append(row)

    return rows

def delete_existing_data(client, date_str, account_id):
    """Smaže existující záznamy pro dané datum a účet"""
    table_ref = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    query = f"""
    DELETE FROM `{table_ref}`
    WHERE date = '{date_str}' AND account_id = '{account_id}'
    """

    try:
        query_job = client.query(query)
        query_job.result()
    except Exception as e:
        print(f"  Varování: Nelze smazat existující data: {e}")

def upload_to_bigquery(rows, date_str, account_id):
    """Nahraje data do BigQuery"""
    client = bigquery.Client(project=BQ_PROJECT)
    table_ref = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    # Smazat existující data pro tento den a účet
    delete_existing_data(client, date_str, account_id)

    # Nahrát nová data
    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        print(f"  BQ chyby: {errors}")
    else:
        print(f"  Nahráno {len(rows)} řádků do BigQuery")

def main():
    parser = argparse.ArgumentParser(description="Export Meta Ads data do BigQuery")
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

    print(f"Meta Ads -> BigQuery | {date_from}")

    # Získat access token (z konfigurace nebo ENV proměnné)
    access_token = META_ACCESS_TOKEN if META_ACCESS_TOKEN != "YOUR_META_ACCESS_TOKEN_HERE" else os.getenv("META_ACCESS_TOKEN")
    if not access_token:
        print("CHYBA: Nastavte META_ACCESS_TOKEN v kódu nebo jako proměnnou prostředí")
        sys.exit(1)

    all_rows_count = 0

    for account_id, account_name in META_ACCOUNTS.items():
        print(f"Stahuji: {account_name} ({account_id})")

        campaigns = get_meta_campaigns(account_id, account_name, access_token, date_from, date_to)

        if not campaigns:
            print(f"  0 kampaní")
            continue

        rows = process_campaigns(campaigns, account_id, account_name, date_from)
        print(f"  {len(rows)} kampaní")

        if rows:
            upload_to_bigquery(rows, date_from, account_id)
            all_rows_count += len(rows)

    print(f"\nCelkem zpracováno {all_rows_count} řádků")
    print("Hotovo!")

if __name__ == "__main__":
    main()
