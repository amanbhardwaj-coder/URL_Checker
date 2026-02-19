import io
import pandas as pd
import requests
import streamlit as st

def fetch_text(url: str, timeout: int = 30) -> str:
    r = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    return r.text

def try_read_csv(text: str, sep: str, header_mode: str):
    # header_mode: "infer" or "none"
    header = "infer" if header_mode == "infer" else None

    # 1) Fast path (C engine)
    try:
        return pd.read_csv(io.StringIO(text), sep=sep, header=header)
    except Exception:
        pass

    # 2) More tolerant parser (python engine)
    try:
        return pd.read_csv(
            io.StringIO(text),
            sep=sep,
            header=header,
            engine="python",
            on_bad_lines="skip",   # skip malformed rows
        )
    except Exception as e:
        raise e

def read_as_url_list(text: str) -> pd.DataFrame:
    # Treat each non-empty line as a URL (handles plain text lists too)
    lines = [ln.strip().replace("\r", "") for ln in text.splitlines()]
    lines = [ln for ln in lines if ln]
    return pd.DataFrame({"URL": lines})

# ---- UI block for "CSV via URL" mode ----
st.subheader("CSV via URL")

csv_url = st.text_input("Paste direct CSV URL", placeholder="https://example.com/urls.csv")

col1, col2, col3 = st.columns([1, 1, 1])
with col1:
    sep = st.selectbox("Delimiter", options=[",", "\t", ";", "|"], index=0)
with col2:
    header_mode = st.selectbox("Header", options=["infer", "none"], index=0)
with col3:
    fallback_mode = st.selectbox("If parsing fails", options=["Show error", "Treat as URL list"], index=0)

df = None
if csv_url:
    try:
        text = fetch_text(csv_url)

        # Helpful debug: detect HTML or unexpected content
        preview = text[:2000]
        if "<html" in preview.lower():
            st.warning("Downloaded content looks like HTML (might not be a raw CSV link). Showing preview below.")

        with st.expander("Downloaded content preview (first 2KB)"):
            st.code(preview)

        # Try CSV parse
        try:
            df = try_read_csv(text, sep=sep, header_mode=header_mode)
        except Exception as e:
            if fallback_mode == "Treat as URL list":
                df = read_as_url_list(text)
                st.info("Couldn’t parse as CSV — using one-URL-per-line fallback.")
            else:
                raise e

    except Exception as e:
        st.error(f"Could not fetch/read CSV from URL: {e}")
