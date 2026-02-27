import requests
import certifi

API_KEY = "0xd11814ab66b073cd7c9201c1e16d9778fbbfc00808817fc447810eadf42cfe4ed5e35"

session = requests.post(
    "https://api.sklik.cz/jsonApi/drak/client.loginByToken",
    json=[API_KEY], verify=certifi.where()
).json()["session"]

r = requests.post(
    "https://api.sklik.cz/jsonApi/drak/client.get",
    json=[{"session": session}], verify=certifi.where()
)
data = r.json()
print("Muj ucet ID:", data.get("user", {}).get("userId"))
print("Muj ucet name:", data.get("user", {}).get("username"))
print("\nKlientske ucty:")
for acc in data.get("foreignAccounts", []):
    print(f"  ID: {acc['userId']} | Nazev: {acc['username']} | Pristup: {acc['access']}")