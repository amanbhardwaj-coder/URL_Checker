import io
import re
import time
import pandas as pd
import streamlit as st
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

st.set_page_config(page_title="URL Checker", layout="wide")

st.title("✅ Bulk URL Checker (CSV upload or CSV URL)")
st.caption("Checks HTTP status codes and labels URLs as Working / Not Working / Could not connect.")

# ----------------------------
# Helpers
# ----------------------------
def normalize_url(u: str) -> str:
    if u is None:
        return ""
    u = str(u).strip().replace("\r", "")
    return u

def infer_url_column(df: pd.DataFrame) -> str | None:
    # Prefer common column names
    candidates = ["url", "URL", "link", "Link", "urls", "URLs"]
    for c in candidates:
        if c in df.columns:
            return c
    # If there's exactly one column, assume it's the URL column
    if df.shape[1] == 1:
        return df.columns[0]
    return None

def check_url(url: str, timeout: int = 10, user_agent: str = "Mozilla/5.0") -> dict:
    url = normalize_url(url)

    if url == "":
        return {"URL": "", "Status Code": "000", "Status": "Empty URL"}

    # If scheme missing, assume https (optional but helpful)
    if not re.match(r"^https?://", url, re.IGNORECASE):
        url = "https://" + url

    try:
        # HEAD first (faster), fallback to GET if server blocks HEAD
        headers = {"User-Agent": user_agent}
        resp = requests.head(url, allow_redirects=True, timeout=timeout, headers=headers)
        code = resp.status_code

        if code in (405, 403):  # some servers dislike HEAD
            resp = requests.get(url, allow_redirects=True, timeout=timeout, headers=headers)
            code = resp.status_code

        if code == 200:
            msg = "Working"
        else:
            msg = f"Not Working (Code: {code})"

        return {"URL": url, "Status Code": str(code), "Status": msg}

    except requests.exceptions.RequestException:
        # curl prints 000 for connection/timeout/dns failures; we emulate that
        return {"URL": url, "Status Code": "000", "Status": "Could not connect (Code: 000)"}

def run_checks(urls: list[str], workers: int, timeout: int) -> pd.DataFrame:
    results = [None] * len(urls)  # keep input order like `parallel --keep-order`

    with ThreadPoolExecutor(max_workers=workers) as ex:
        future_map = {
            ex.submit(check_url, urls[i], timeout): i for i in range(len(urls))
        }
        for fut in as_completed(future_map):
            i = future_map[fut]
            results[i] = fut.result()

    return pd.DataFrame(results)

# ----------------------------
# UI Inputs
# ----------------------------
colA, colB, colC = st.columns([1.2, 1, 1])

with colA:
    mode = st.radio("Input method", ["Upload CSV", "CSV via URL"], horizontal=True)

with colB:
    workers = st.slider("Parallel jobs (workers)", min_value=1, max_value=500, value=100, step=1)
    st.caption("More workers = faster, but can hit rate limits / your network limits.")

with colC:
    timeout = st.slider("Timeout (seconds)", min_value=2, max_value=60, value=10, step=1)

df = None

if mode == "Upload CSV":
    up = st.file_uploader("Upload CSV file", type=["csv"])
    if up is not None:
        df = pd.read_csv(up)

else:
    csv_url = st.text_input("Paste direct CSV URL", placeholder="https://example.com/urls.csv")
    if csv_url:
        try:
            r = requests.get(csv_url, timeout=20)
            r.raise_for_status()
            df = pd.read_csv(io.StringIO(r.text))
        except Exception as e:
            st.error(f"Could not fetch/read CSV from URL: {e}")

if df is not None:
    st.subheader("Preview")
    st.dataframe(df.head(20), use_container_width=True)

    url_col = infer_url_column(df)
    if url_col is None:
        url_col = st.selectbox("Pick the URL column", options=list(df.columns))
    else:
        st.info(f"Detected URL column: **{url_col}**")

    urls = [normalize_url(x) for x in df[url_col].tolist()]

    if st.button("Run URL Check", type="primary"):
        start = time.time()
        prog = st.progress(0)
        status_box = st.empty()

        # Run checks
        # (We keep it simple: run then display; if you want live-progress per URL,
        #  we can add a queue and update progress as futures complete.)
        status_box.write(f"Running checks for **{len(urls)}** URLs with **{workers}** workers…")
        results_df = run_checks(urls, workers=workers, timeout=timeout)

        prog.progress(100)
        took = time.time() - start
        status_box.success(f"Done in {took:.1f}s")

        st.subheader("Results")
        st.dataframe(results_df, use_container_width=True)

        # Download
        out = results_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "⬇️ Download results.csv",
            data=out,
            file_name="results.csv",
            mime="text/csv",
        )
