import io
import re
import time
import random
from urllib.parse import urlparse

import pandas as pd
import requests
import streamlit as st
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ----------------------------
# Streamlit setup
# ----------------------------
st.set_page_config(page_title="Fast URL Checker", layout="wide")
st.title("⚡ Fast & Reliable URL Checker (CSV upload or CSV URL)")


# ----------------------------
# URL + CSV helpers
# ----------------------------
def normalize_url(u: str) -> str:
    if u is None:
        return ""
    u = str(u).strip().replace("\r", "")
    return u

def ensure_scheme(url: str) -> str:
    if url and not re.match(r"^https?://", url, re.IGNORECASE):
        return "https://" + url
    return url

def infer_url_column(df: pd.DataFrame) -> str | None:
    for c in ["url", "URL", "link", "Link", "urls", "URLs"]:
        if c in df.columns:
            return c
    if df.shape[1] == 1:
        return df.columns[0]
    return None

def to_google_export_url(url: str) -> str | None:
    if not url:
        return None
    m = re.search(r"docs\.google\.com/spreadsheets/d/([a-zA-Z0-9-_]+)", url)
    if not m:
        return None
    sheet_id = m.group(1)
    gid = "0"
    gid_m = re.search(r"[?#&]gid=(\d+)", url)
    if gid_m:
        gid = gid_m.group(1)
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"

def fetch_text(url: str, timeout: int = 30) -> str:
    r = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    return r.text

def read_csv_flexible(text: str, sep: str = ",", header_mode: str = "infer") -> pd.DataFrame:
    header = "infer" if header_mode == "infer" else None
    try:
        return pd.read_csv(io.StringIO(text), sep=sep, header=header)
    except Exception:
        return pd.read_csv(
            io.StringIO(text),
            sep=sep,
            header=header,
            engine="python",
            on_bad_lines="skip",
        )


# ----------------------------
# HTTP client (pooled + retries)
# ----------------------------
def make_session(connect_timeout: int, read_timeout: int, total_retries: int) -> requests.Session:
    """
    Create a pooled session with retries for transient errors.
    This improves reliability vs. raw curl in massive parallel.
    """
    s = requests.Session()

    retry = Retry(
        total=total_retries,
        connect=total_retries,
        read=total_retries,
        status=total_retries,
        backoff_factor=0.4,  # exponential backoff
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["HEAD", "GET"]),
        raise_on_status=False,
    )

    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=200,
        pool_maxsize=200,
    )
    s.mount("http://", adapter)
    s.mount("https://", adapter)

    # store timeouts on session
    s._timeout = (connect_timeout, read_timeout)
    return s


# ----------------------------
# URL checker
# ----------------------------
def check_one(session: requests.Session, url: str, prefer_get: bool, follow_redirects: bool) -> dict:
    raw = normalize_url(url)
    if raw == "":
        return {"URL": "", "Status Code": "000", "Status": "Empty URL"}

    u = ensure_scheme(raw)
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; URLChecker/1.0)",
        "Accept": "*/*",
    }

    try:
        # Many CDNs handle GET more consistently than HEAD
        if prefer_get:
            resp = session.get(u, headers=headers, allow_redirects=follow_redirects, timeout=session._timeout, stream=True)
        else:
            resp = session.head(u, headers=headers, allow_redirects=follow_redirects, timeout=session._timeout)
            # fallback if server blocks HEAD
            if resp.status_code in (403, 405):
                resp = session.get(u, headers=headers, allow_redirects=follow_redirects, timeout=session._timeout, stream=True)

        code = resp.status_code

        # close streaming response quickly (don’t download full image)
        try:
            resp.close()
        except Exception:
            pass

        if code == 200:
            msg = "Working"
        else:
            msg = f"Not Working (Code: {code})"

        return {"URL": u, "Status Code": str(code), "Status": msg}

    except requests.exceptions.RequestException:
        # Equivalent of curl's 000: dns failure, timeout, tls handshake, reset, etc.
        return {"URL": u, "Status Code": "000", "Status": "Could not connect (Code: 000)"}


def run_checks(urls: list[str], workers: int, connect_timeout: int, read_timeout: int, retries: int,
               prefer_get: bool, follow_redirects: bool) -> pd.DataFrame:
    session = make_session(connect_timeout, read_timeout, retries)

    results = [None] * len(urls)

    # IMPORTANT: massive workers can create 000s. Bounded concurrency is “reliable fast”.
    with ThreadPoolExecutor(max_workers=workers) as ex:
        future_map = {
            ex.submit(check_one, session, urls[i], prefer_get, follow_redirects): i
            for i in range(len(urls))
        }

        done = 0
        total = len(urls)
        prog = st.progress(0)
        status = st.empty()

        for fut in as_completed(future_map):
            i = future_map[fut]
            results[i] = fut.result()
            done += 1
            if total:
                prog.progress(int(done * 100 / total))
            if done % 25 == 0 or done == total:
                status.write(f"Checked {done}/{total}")

    return pd.DataFrame(results)


# ----------------------------
# UI: input
# ----------------------------
st.subheader("1) Load URLs")

mode = st.radio("Input method", ["Upload CSV", "CSV via URL"], horizontal=True)

c1, c2, c3, c4 = st.columns(4)
with c1:
    workers = st.slider("Workers (parallel)", 1, 300, 80, 1)
with c2:
    connect_timeout = st.slider("Connect timeout (s)", 1, 20, 5, 1)
with c3:
    read_timeout = st.slider("Read timeout (s)", 1, 30, 10, 1)
with c4:
    retries = st.slider("Retries (for 000/429/5xx)", 0, 5, 2, 1)

c5, c6 = st.columns(2)
with c5:
    prefer_get = st.checkbox("Prefer GET (more reliable for CDNs/images)", value=True)
with c6:
    follow_redirects = st.checkbox("Follow redirects", value=True)

sep = st.selectbox("CSV delimiter", options=[",", "\t", ";", "|"], index=0)
header_mode = st.selectbox("CSV header", options=["infer", "none"], index=0)

df = None

if mode == "Upload CSV":
    up = st.file_uploader("Upload CSV", type=["csv"])
    if up is not None:
        df = pd.read_csv(up)
else:
    csv_url = st.text_input("Paste CSV URL (Google Sheets supported)")
    if csv_url:
        export_url = to_google_export_url(csv_url)
        effective_url = export_url or csv_url
        if export_url:
            st.info("Google Sheets link detected → using CSV export URL")
            st.code(effective_url)

        try:
            text = fetch_text(effective_url)
            if "<html" in text[:2000].lower():
                st.warning("Looks like HTML (sheet may be private). Make it 'Anyone with the link' or use export URL.")
                with st.expander("Preview first 2KB"):
                    st.code(text[:2000])
            df = read_csv_flexible(text, sep=sep, header_mode=header_mode)
        except Exception as e:
            st.error(f"Could not fetch/read CSV from URL: {e}")

if df is not None:
    st.subheader("2) Pick URL column")
    st.dataframe(df.head(20), use_container_width=True)

    url_col = infer_url_column(df)
    if url_col is None:
        url_col = st.selectbox("Select column containing URLs", options=list(df.columns))
    else:
        st.info(f"Detected URL column: **{url_col}**")

    urls = [normalize_url(x) for x in df[url_col].tolist()]

    st.subheader("3) Run checks")
    if st.button("Run URL Check ⚡", type="primary"):
        results_df = run_checks(
            urls=urls,
            workers=workers,
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            retries=retries,
            prefer_get=prefer_get,
            follow_redirects=follow_redirects,
        )

        st.subheader("Results")
        st.dataframe(results_df, use_container_width=True)

        # Summary
        ok = (results_df["Status Code"] == "200").sum()
        not_ok = (results_df["Status Code"] != "200").sum()
        st.write(f"✅ Working (200): **{ok}**  |  ❌ Not 200 / 000: **{not_ok}**")

        # Download
        out = results_df.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Download results.csv", data=out, file_name="results.csv", mime="text/csv")
