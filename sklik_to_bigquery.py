import requests
import certifi
from google.cloud import bigquery
from datetime import date, timedelta

# ── Konfigurace ──────────────────────────────────────────────
API_KEY = "0xd11814ab66b073cd7c9201c1e16d9778fbbfc00808817fc447810eadf42cfe4ed5e35"
BQ_PROJECT = "big-query-exports-488618"
BQ_DATASET = "sklik_raw"
BQ_TABLE = "campaigns"

# Období — výchozí je včerejšek
DATE_FROM = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
DATE_TO = DATE_FROM

# Klientské účty (ID ze Skliku)
CLIENT_ACCOUNTS = {
    611792:  "barberco.cz",
    1126690: "cihlomat.cz",
    568988:  "directalpine.cz",
    600763:  "esoxbrno.cz",
    714938:  "ebikepartners.cz",
    1132324: "nakupujdrevo.online",
    1132396: "vanicepodklinovcem.cz",
    1132405: "nakovarne41.cz",
    1021984: "prager.cz",
    1078719: "stablecam.com",
    1029733: "bohdalice",
    550733:  "monkeymum",
    563283:  "on.running",
    858909:  "smart4smart.cz",
    1065882: "realitarna",
    458029:  "solc.vojta12",
    173946:  "swixshop",
    1049085: "fotocopy.sk",
    613817:  "zlatakrasa.cz",
}

def login(api_key):
    r = requests.post(
        "https://api.sklik.cz/jsonApi/drak/client.loginByToken",
        json=[api_key], verify=certifi.where()
    )
    return r.json()["session"]

def get_campaign_stats(session, user_id, account_name, date_from, date_to):
    r = requests.post(
        "https://api.sklik.cz/jsonApi/drak/campaigns.createReport",
        json=[
            {"session": session, "userId": user_id},
            {"dateFrom": date_from, "dateTo": date_to},
            {"statGranularity": "total"}
        ],
        verify=certifi.where()
    ).json()

    if r.get("status") != 200:
        print(f"  Chyba createReport pro {account_name}: {r}")
        return []

    report_id = r.get("reportId")
    if not report_id:
        print(f"  Chybí reportId pro {account_name}: {r}")
        return []

    data = requests.post(
        "https://api.sklik.cz/jsonApi/drak/campaigns.readReport",
        json=[
            {"session": session, "userId": user_id},
            report_id,
            {
                "offset": 0,
                "limit": 1000,
                "displayColumns": ["name", "clicks", "impressions", "conversions",
                                   "totalMoney", "avgCpc", "ctr"]
            }
        ],
        verify=certifi.where()
    ).json()

    if data.get("status") != 200:
        print(f"  Chyba readReport pro {account_name}: {data}")
        return []

    report = data.get("report", [])

    rows = []
    for camp in report:
        # Získat statistiky z prvního stats záznamu (protože statGranularity = "total")
        stats = camp.get("stats", [])
        if not stats:
            continue

        stat = stats[0]  # První (a jediný) záznam pro "total"

        rows.append({
            "date": date_from,
            "account_id": user_id,
            "account_name": account_name,
            "campaign_id": camp.get("id"),
            "campaign_name": camp.get("name"),
            "spend": round(stat.get("totalMoney", 0) / 100, 2),
            "impressions": stat.get("impressions", 0),
            "clicks": stat.get("clicks", 0),
            "conversions": stat.get("conversions", 0),
            "cpc": round(stat.get("avgCpc", 0) / 100, 2),
            "ctr": stat.get("ctr", 0.0),
        })
    return rows

def upload_to_bigquery(rows):
    client = bigquery.Client(project=BQ_PROJECT)
    table_ref = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        print("BQ chyby:", errors)
    else:
        print(f"  Nahráno {len(rows)} radku do BigQuery")

def main():
    print(f"Sklik -> BigQuery | {DATE_FROM}")
    session = login(API_KEY)
    print("Prihlaseni OK")

    all_rows = []
    for user_id, account_name in CLIENT_ACCOUNTS.items():
        print(f"Stahuji: {account_name} ({user_id})")
        rows = get_campaign_stats(session, user_id, account_name, DATE_FROM, DATE_TO)
        print(f"  {len(rows)} kampani")
        all_rows.extend(rows)

    if all_rows:
        print(f"\nCelkem {len(all_rows)} radku k nahrání")
        upload_to_bigquery(all_rows)
    else:
        print("\nŽádná data k nahrání")
    print("Hotovo!")

if __name__ == "__main__":
    main()