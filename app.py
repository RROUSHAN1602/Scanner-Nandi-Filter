import streamlit as st
import pandas as pd
import numpy as np
import datetime as dt
import pyotp
from SmartApi.smartConnect import SmartConnect
import time
import io

# ================= CONFIG =================
st.set_page_config(page_title="🟡 NANDI Batch Scanner", layout="wide")
st.title("🟡 NANDI Batch Scanner")

# ================= ANGEL LOGIN =================
api_key = "EKa93pFu"
client_id = "R59803990"
password = "1234"
totp_secret = "5W4MC6MMLANC3UYOAW2QDUIFEU"

@st.cache_resource
def angel_login():
    obj = SmartConnect(api_key=api_key)
    totp = pyotp.TOTP(totp_secret).now()
    obj.generateSession(client_id, password, totp)
    return obj

try:
    obj = angel_login()
    st.success("Login Successful")
except:
    st.error("Login Failed")
    st.stop()

# ================= STOCK LIST =================
from Stock_tokens import stock_list  # {"RELIANCE": 2885, ...}

items = list(stock_list.items())
batch_size = 100
batches = [items[i:i + batch_size] for i in range(0, len(items), batch_size)]

batch_no = st.selectbox("📦 Select Batch (100 stocks each)",
                        list(range(1, len(batches) + 1)))

selected_batch = batches[batch_no - 1]

# ================= USER INPUT =================
col1, col2, col3 = st.columns(3)

with col1:
    interval = st.selectbox("Interval", ["ONE_DAY", "ONE_HOUR"])

with col2:
    from_date = st.date_input("From Date", dt.date.today() - dt.timedelta(days=30))

with col3:
    to_date = st.date_input("To Date", dt.date.today())

scan_button = st.button("🚀 Run Scan")

# ================= HELPER FUNCTIONS =================

def fetch_data(token):
    params = {
        "exchange": "NSE",
        "symboltoken": str(token),
        "interval": interval,
        "fromdate": from_date.strftime("%Y-%m-%d 09:15"),
        "todate": to_date.strftime("%Y-%m-%d 15:30"),
    }

    response = obj.getCandleData(params)

    if not response or response["status"] != True:
        return None

    df = pd.DataFrame(response["data"],
                      columns=["timestamp","open","high","low","close","volume"])

    # 🔹 FIX 1: Remove timezone at source
    df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.tz_localize(None)

    df[["open","high","low","close","volume"]] = \
        df[["open","high","low","close","volume"]].astype(float)

    return df


def compute_cmo(close, length=9):
    diff = close.diff()
    up = diff.clip(lower=0)
    down = (-diff).clip(lower=0)

    sum_up = up.rolling(length).sum()
    sum_down = down.rolling(length).sum()

    return 100 * (sum_up - sum_down) / (sum_up + sum_down)


def detect_nandi(df):
    len_ = 20
    mult = 2.0
    volMultiplier = 1.5

    close = df["close"]

    sma = close.rolling(len_).mean()
    dev = mult * (close.sub(sma).abs().rolling(len_).std())

    kri = (close - sma).abs()
    changePerc = (close / close.shift(1) - 1) * 100

    condition1 = (kri > dev) & (changePerc >= 0)

    cmo = compute_cmo(close, 9)
    condition2 = (cmo > 0) & (cmo.shift(1) < 0)

    whiteCandle = condition1 & condition2

    vol_sma = df["volume"].rolling(20).mean()
    volSpike = df["volume"] > vol_sma * volMultiplier

    wcHigh = np.nan
    breakoutActive = False
    hasTriggered = False
    buySignals = []

    for i in range(len(df)):

        if whiteCandle.iloc[i]:
            wcHigh = df["high"].iloc[i]
            breakoutActive = True
            hasTriggered = False

        buy = False
        if breakoutActive and not hasTriggered:
            if df["close"].iloc[i] > wcHigh and volSpike.iloc[i]:
                buy = True
                hasTriggered = True

        buySignals.append(buy)

    df["buyTrigger"] = buySignals
    return df


# ================= SCAN =================

if scan_button:

    progress = st.progress(0)
    results = []
    total = len(selected_batch)

    for i, (symbol, token) in enumerate(selected_batch):

        df = fetch_data(token)

        if df is not None:
            df = detect_nandi(df)

            triggers = df[df["buyTrigger"] == True].copy()

            if not triggers.empty:

                # 🔹 FIX 2: Strictly filter inside selected date range
                triggers["date"] = triggers["timestamp"].dt.date

                triggers = triggers[
                    (triggers["date"] >= from_date) &
                    (triggers["date"] <= to_date)
                ]

                if not triggers.empty:
                    triggers = triggers.sort_values("timestamp")
                    last = triggers.iloc[-1]  # Always latest in range

                    results.append({
                        "Symbol": symbol,
                        "Trigger Date": last["timestamp"],
                        "Close": last["close"],
                        "Volume": last["volume"],
                        "Batch": batch_no
                    })

        progress.progress((i + 1) / total)
        time.sleep(0.4)  # avoid rate limit

    if results:

        result_df = pd.DataFrame(results).sort_values("Trigger Date", ascending=False)

        st.dataframe(result_df, use_container_width=True)

        # 🔹 EXTRA SAFETY: ensure timezone removed before Excel
        result_df["Trigger Date"] = pd.to_datetime(result_df["Trigger Date"]).dt.tz_localize(None)

        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            result_df.to_excel(writer, index=False)

        st.download_button(
            "⬇️ Download Excel",
            data=buffer.getvalue(),
            file_name=f"NANDI_Batch_{batch_no}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    else:
        st.warning("No NANDI Found in this batch")
