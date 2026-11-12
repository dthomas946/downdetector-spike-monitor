"""Standalone fake downdetectorapi.com for manual testing: python tests/fake_server.py"""
import random, time
from fastapi import FastAPI, Request
import uvicorn
app = FastAPI()
START = time.time()
COS = {101: ("Amazon Web Services", "aws", "https://aws.amazon.com"), 202: ("Chase", "chase", "https://www.chase.com"),
       303: ("AT&T", "att", "https://www.att.com")}
def co(i):
    n, s, u = COS[i]
    spike = i == 101 and time.time() - START > 5          # AWS "goes down" 5s after boot
    stats = [random.randint(2, 6) for _ in range(95)] + [random.randint(80, 120) if spike else 4]
    return {"id": i, "name": n, "slug": s, "url": u, "status": "danger" if spike else "success",
            "country_iso": "US", "baseline_current": 4, "stats_24": stats, "stats_60": sum(stats[-4:])}
@app.post("/v2/tokens")
def tok(): return {"access_token": "tok", "expires_in": 3600, "token_type": "Bearer"}
@app.get("/v2/companies/search")
def search(name: str = "", slug: str = ""):
    q = (slug or name).lower()
    return {"data": [co(i) for i, (n, s, _) in COS.items() if q in s or q in n.lower()]}
@app.get("/v2/companies")
def companies(ids: str): return {"data": [co(int(i)) for i in ids.split(",") if int(i) in COS]}
if __name__ == "__main__": uvicorn.run(app, port=8555, log_level="warning")
