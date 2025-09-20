"""
Decentralized Analytics: SQL Queries over IPFS Data
Single-file program demonstrating read-only SQL queries over IPFS datasets with local caching.
"""

import ipfshttpclient
import duckdb
import pandas as pd
import os

CACHE_DIR = "./cache"

SAMPLE_CSV_CONTENT = """id,name,value
1,Alice,10
2,Bob,20
3,Charlie,30
"""

def ensure_cache_dir():
    if not os.path.exists(CACHE_DIR):
        os.makedirs(CACHE_DIR)

def fetch_from_ipfs(cid):
    ensure_cache_dir()
    local_path = os.path.join(CACHE_DIR, f"{cid}.csv")
    if not os.path.exists(local_path):
        with open(local_path, "w") as f:
            # Simulate IPFS fetch with sample content
            f.write(SAMPLE_CSV_CONTENT)
    return local_path

def query_csv(cid, sql_query):
    csv_path = fetch_from_ipfs(cid)
    con = duckdb.connect()
    result = con.execute(f"SELECT * FROM read_csv_auto('{csv_path}') WHERE {sql_query}").fetchdf()
    print(result)

if __name__ == "__main__":
    print("Decentralized Analytics: SQL Queries over IPFS Data\n")
    while True:
        cid = input("Enter dataset CID (or 'exit'): ")
        if cid.lower() == "exit":
            break
        sql_filter = input("Enter SQL filter condition (e.g., value>15): ")
        query_csv(cid, sql_filter)
