
import argparse
import time
import requests
import pandas as pd
import joblib

# ───────── CONFIG ─────────
API_KEY = "your_semantic_scholar_api_key_here"
EXPECTED_COLS = ["paper_id","title","abstract","year","venue","doi","arxiv"]

def search_by_venue_bulk(session_ss, venue_name, year_from, limit, max_retries=5):
    """
    Enumerate all papers in a venue using Semantic Scholar's bulk-search endpoint,
    with retry-on-429 support.
    """
    BASE = "https://api.semanticscholar.org/graph/v1/paper/search/bulk"
    params = {
        "query":  "",             # rely on the venue filter
        "venue":  venue_name,
        "year":   f"{year_from}-",# e.g. "2018-"
        "fields": "paperId,title,abstract,year,venue,externalIds",
        "limit":  limit
    }
    all_rows = []
    print(f"🔍 Bulk-searching venue='{venue_name}', year>={year_from}")

    while True:
        # Attempt (with retries) to fetch one page
        for attempt in range(1, max_retries+1):
            r = session_ss.get(BASE, params=params)
            if r.status_code == 200:
                break
            if r.status_code == 429:
                retry_after = int(r.headers.get("Retry-After","1"))
                print(f"    ⚠️ 429 on bulk-search, sleeping {retry_after}s (try {attempt}/{max_retries})")
                time.sleep(retry_after)
                continue
            r.raise_for_status()  # for other errors
        else:
            # exhausted retries
            print(f"    ❌ Giving up bulk-search on venue='{venue_name}' after {max_retries} retries")
            return pd.DataFrame(all_rows, columns=EXPECTED_COLS)

        # Success!
        j     = r.json()
        data  = j.get("data", [])
        token = j.get("next") or j.get("token")

        # Collect this page
        for p in data:
            all_rows.append({
                "paper_id": p["paperId"],
                "title":    p.get("title",""),
                "abstract": p.get("abstract",""),
                "year":     p.get("year",0),
                "venue":    p.get("venue",""),
                "doi":      (p.get("externalIds") or {}).get("DOI"),
                "arxiv":    (p.get("externalIds") or {}).get("ArXiv"),
            })

        print(f"  ▶ got {len(data)} items, total collected {len(all_rows)}")

        if not token:
            break
        params["token"] = token
        time.sleep(1)  # throttle

    return pd.DataFrame(all_rows, columns=EXPECTED_COLS)


def fetch_forward_citations(session, paper_id, year_from, batch_size, max_retries=5):
    """
    Papers that *cite* this seed (forward citations).
    """
    API = f"https://api.semanticscholar.org/graph/v1/paper/{paper_id}/citations"
    rows, offset = [], 0
    print(f"🔍 Fetching forward citations for paper_id={paper_id}, year>={year_from}")
    batch_size = 1000  # max page size is 1000
    while True:
        params = {
            "fields": "citingPaper.paperId,citingPaper.title,citingPaper.year",
            "limit":   batch_size,
            "offset":  offset
        }
        # retry‐on‐429/500, treat 400 as none, identical to before…
        for attempt in range(1, max_retries+1):
            r = session.get(API, params=params)
            # r = session_ss.get(url, params=params)
            code = r.status_code
            if code == 200:
                print(f"    ↪ 200 fetched {len(rows)} forward citations so far (next offset={offset})")
                break
            if code == 400:
                print(f"    ℹ️ 400 @ offset={offset} → no forward citations for {paper_id}")
                return pd.DataFrame(rows, columns=EXPECTED_COLS)
            if code in (429, 500):
                print(f"    ⚠️ {code} on forward citations, retrying after 1s (attempt {attempt}/{max_retries})")
                wait = int(r.headers.get("Retry-After","1"))
                time.sleep(wait)
                continue
            r.raise_for_status()
        else:
            return pd.DataFrame(rows, columns=EXPECTED_COLS)

        data = r.json().get("data",[])
        if not data:
            break
        print(f"    ↪ fetched {len(rows)} forward citations so far (next offset={offset})")
        for item in data:
            p = item["citingPaper"]
            yr = p.get("year")
            if yr is not None and yr < year_from: continue
            rows.append({
                "paper_id": p["paperId"],
                "title":    p.get("title",""),
                "abstract": p.get("abstract",""),
                "year":     yr,
                "venue":    p.get("venue",""),
                "doi":      (p.get("externalIds") or {}).get("DOI"),
                "arxiv":    (p.get("externalIds") or {}).get("ArXiv"),
            })
            
        offset += batch_size
        time.sleep(1)

    return pd.DataFrame(rows, columns=EXPECTED_COLS)

# The API used in this function currently does not work as expected, so it isn't used in the main script.
def fetch_backward_references(session, paper_id, year_from, batch_size, max_retries=5):
    """
    Fetch all papers *referenced by* `paper_id` (its bibliography),
    handling 429/500 with retries and treating 400 as “no references.”
    Returns a DataFrame with columns: paper_id, title, abstract, year, venue, doi, arxiv.
    """
    API = f"https://api.semanticscholar.org/graph/v1/paper/{paper_id}/references"
    rows, offset = [], 0
    batch_size = 1000 
    print(f"🔍 Fetching refs for paper_id={paper_id}, year>={year_from}")
    while True:
        params = {
            "fields":  "citedPaper.paperId,citedPaper.title,citedPaper.year",
            "limit":   batch_size,
            "offset":  offset
        }

        # retry loop for 429 & 500
        for attempt in range(1, max_retries+1):
            r = session.get(API, params=params)
            code = r.status_code

            if code == 200:
                print(f"    ↪ 200 fetched {len(rows)} references so far (next offset={offset})")
                break
            if code == 400:
                # no references at all
                print(f"    ℹ️ 400 @ offset={offset} → no references for {paper_id}")
                return pd.DataFrame(rows, columns=EXPECTED_COLS)
            if code in (429, 500):
                wait = int(r.headers.get("Retry-After", "1"))
                typ  = "429 rate-limit" if code == 429 else "500 server error"
                print(f"    ⚠️ {typ}, retrying after {wait}s ({attempt}/{max_retries})")
                time.sleep(wait)
                continue
            # other errors
            r.raise_for_status()
        else:
            # exhausted retries
            print(f"    ❌ Giving up on references for {paper_id} after {max_retries} attempts")
            return pd.DataFrame(rows, columns=EXPECTED_COLS)

        data = r.json().get("data", [])
        if not data:
            print(f"    ℹ️ no data @ offset={offset} → end of references for {paper_id}")
            break

        print(f"    ↪ fetched {len(data)} references @ offset={offset}")

        for item in data:
            p = item["citedPaper"]
            yr = p.get("year")
            # include if no year or year >= cutoff
            if yr is not None and yr < year_from:
                continue
            rows.append({
                "paper_id": p.get("paperId"),
                "title":    p.get("title",""),
                "abstract": p.get("abstract",""),
                "year":     yr or 0,
                "venue":    p.get("venue",""),
                "doi":      (p.get("externalIds") or {}).get("DOI"),
                "arxiv":    (p.get("externalIds") or {}).get("ArXiv"),
            })

        offset += batch_size
        time.sleep(1)

    return pd.DataFrame(rows, columns=EXPECTED_COLS)



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--venues",       nargs="+",     default=None, help="List of venue names")
    parser.add_argument("--venues_file",  default=None,   help="File of venue names, one per line")
    parser.add_argument("--year_from",    type=int,       default=2005, help="Earliest year")
    parser.add_argument("--limit",        type=int,       default=4000, help="Bulk-search page size")
    parser.add_argument("--batch_size",   type=int,       default=100, help="Citation page size")
    parser.add_argument("--threshold",    type=float,     default=0.0, help="Relevance threshold")
    parser.add_argument("--model",        default="relevance_head.joblib", help="Path to model")
    parser.add_argument("--keywords_file",default=None,   help="File of keywords, one per line")
    parser.add_argument("--out_csv",      default="harvest_bulk.csv", help="Output CSV")
    args = parser.parse_args()

    # Load venues
    if args.venues_file:
        with open(args.venues_file) as f:
            venues = [l.strip() for l in f if l.strip()]
    elif args.venues:
        venues = args.venues
    else:
        parser.error("Must supply --venues or --venues_file")

    # Load keywords
    if args.keywords_file:
        kws = [l.strip().lower() for l in open(args.keywords_file) if l.strip()]
    else:
        kws = []

    # Prepare Semantic Scholar session
    session_ss = requests.Session()
    session_ss.headers.update({"x-api-key": API_KEY})

    all_results = []

    for venue in venues:
        df_v = search_by_venue_bulk(session_ss, venue, args.year_from, args.limit)
        if df_v.empty:
            print(f"⚠️ No papers found for venue='{venue}'")
            continue

        # Row‐wise keyword filter
        if kws:
            df_v["combined"] = df_v.fillna("").astype(str).agg(" ".join, axis=1).str.lower()
            mask = df_v["combined"].apply(lambda txt: any(kw in txt for kw in kws))
            df_v = df_v[mask].drop(columns=["combined"])
            print(f"  ▶ {len(df_v)} papers after keyword filter")

        
        if df_v.empty:
            print(f"⚠️ No papers cleared the keyword filter for '{venue}'")
            continue
        
        
        df_v["source"]= f"venue:{venue}"
        all_results.append(df_v)


        for _, seed in df_v.iterrows():
            pid = seed["paper_id"]
            df_c = fetch_forward_citations(session_ss, pid, args.year_from, args.batch_size)
            if df_c.empty:
                seed_df = pd.DataFrame([seed.to_dict()])
                seed_df["source"]         = f"venue:{venue}"
                seed_df["cited_by_empty"] = True
                all_results.append(seed_df)
                continue
            if kws:
                df_c["combined"] = df_c.fillna("").astype(str).agg(" ".join, axis=1).str.lower()
                mask_c = df_c["combined"].apply(lambda txt: any(kw in txt for kw in kws))
                df_c = df_c[mask_c].drop(columns=["combined"])
                print(f"    ▶ {len(df_c)} citations after keyword filter")
            if df_c.empty:
                continue
            df_c["source"] = f"cited_by:{pid}"
            all_results.append(df_c)
            # Uncomment if you want to fetch backward references as well but this wasn't working when we 
            # tried it last time. 
            # df_r = fetch_backward_references(session_ss, pid, args.year_from, args.batch_size)
            # if df_r.empty:
            #     seed_dr = pd.DataFrame([seed.to_dict()])
            #     seed_dr["source"]         = f"venue:{venue}"
            #     seed_dr["cited_by_empty"] = True
            #     all_results.append(seed_dr)
            #     continue
            # if kws:
            #     df_r["combined"] = df_r.fillna("").astype(str).agg(" ".join, axis=1).str.lower()
            #     mask_r = df_r["combined"].apply(lambda txt: any(kw in txt for kw in kws))
            #     df_r = df_r[mask_r].drop(columns=["combined"])
            #     print(f"    ▶ {len(df_r)} backward references after keyword filter")
            # if df_r.empty:
            #     continue
            # all_results.append(df_r)

    if not all_results:
        print("⚠️ No papers cleared the keyword filter.")
        return

    df_all = pd.concat(all_results, ignore_index=True).drop_duplicates(subset=["paper_id"])
    df_all.to_csv(args.out_csv, index=False)
    print(f"✅ Wrote {len(df_all)} papers to {args.out_csv}")

    # debug export (no abstracts)
    noabs_path = args.out_csv.replace(".csv", "_noabs.csv")
    df_all.drop(columns=["abstract"]).to_csv(noabs_path, index=False)
    print(f"Wrote no-abstracts file with {len(df_all)} rows to {noabs_path}")

if __name__ == "__main__":
    main()
