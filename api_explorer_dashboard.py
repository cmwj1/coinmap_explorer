import streamlit as st
import requests
import hmac
import hashlib
import base64
import time
import json
import pandas as pd
from datetime import datetime, timezone

# ==========================================
# 1. Authentication & API Helpers
# ==========================================

class BinanceClient:
    def __init__(self, api_key, api_secret, base_url='https://fapi.binance.com'):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url

    def get_signature(self, params):
        query_string = '&'.join([f"{k}={v}" for k, v in params.items()])
        return hmac.new(
            self.api_secret.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()

    def send_request(self, method, endpoint, params=None):
        if not self.api_key or not self.api_secret:
            return {"error": "API Key and Secret are required"}
            
        if params is None:
            params = {}
        
        # Add timestamp
        params['timestamp'] = int(time.time() * 1000)
        
        # Sign
        params['signature'] = self.get_signature(params)
        
        url = f"{self.base_url}{endpoint}"
        headers = {'X-MBX-APIKEY': self.api_key}
        
        try:
            if method.upper() == 'GET':
                response = requests.get(url, headers=headers, params=params)
            else:
                response = requests.post(url, headers=headers, data=params)
            
            try:
                return response.json()
            except json.JSONDecodeError:
                return response.text
        except Exception as e:
            return {"error": str(e)}

class OKXClient:
    def __init__(self, api_key, api_secret, passphrase, base_url='https://www.okx.com'):
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        self.base_url = base_url

    def get_timestamp(self):
        return datetime.utcnow().isoformat()[:-3] + 'Z'

    def sign_request(self, timestamp, method, request_path, body):
        message = timestamp + method + request_path + str(body or '')
        mac = hmac.new(
            bytes(self.api_secret, encoding='utf-8'),
            bytes(message, encoding='utf-8'),
            digestmod='sha256'
        )
        return base64.b64encode(mac.digest()).decode('utf-8')

    def send_request(self, method, endpoint, params=None):
        if not self.api_key or not self.api_secret or not self.passphrase:
            return {"error": "API Key, Secret, and Passphrase are required"}

        timestamp = self.get_timestamp()
        
        # Format params for URL
        request_path = endpoint
        body = ""

        if method.upper() == 'GET':
            if params:
                query_string = '&'.join([f"{k}={v}" for k, v in params.items()])
                request_path = f"{endpoint}?{query_string}"
        else:
            # For POST, body is JSON
            if params:
                body = json.dumps(params)
        
        signature = self.sign_request(timestamp, method.upper(), request_path, body)

        headers = {
            "OK-ACCESS-KEY": self.api_key,
            "OK-ACCESS-SIGN": signature,
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": self.passphrase,
            "Content-Type": "application/json"
        }

        url = f"{self.base_url}{request_path}"
        
        try:
            response = requests.request(method, url, headers=headers, data=body)
            try:
                return response.json()
            except json.JSONDecodeError:
                return response.text
        except Exception as e:
            return {"error": str(e)}

# ==========================================
# 2. Definitions: Endpoints & Scenarios
# ==========================================

OKX_ENDPOINTS = {
    "Balance": {
        "path": "/api/v5/account/balance",
        "method": "GET",
        "params": {},
        "desc": ""
    },
    "Positions": {
        "path": "/api/v5/account/positions",
        "method": "GET",
        "params": {},
        "desc": ""
    },
    "Bills": {
        "path": "/api/v5/account/bills",
        "method": "GET",
        "params": {}, 
        "desc": ""
    },
    "Fills History": {
        "path": "/api/v5/trade/fills-history",
        "method": "GET",
        "params": {"instType": "SPOT"},
        "desc": ""
    },
    "Positions History": {
        "path": "/api/v5/account/positions-history",
        "method": "GET",
        "params": {},
        "desc": ""
    }
}

BINANCE_ENDPOINTS = {
    "Account (계좌 정보)": {
        "path": "/fapi/v2/account",
        "method": "GET",
        "params": {},
        "desc": ""
    },
    "Income (입출금/펀딩비 내역)": {
        "path": "/fapi/v1/income",
        "method": "GET",
        "params": {},
        "desc": ""
    },
    "User Trades (체결 내역)": {
        "path": "/fapi/v1/userTrades",
        "method": "GET",
        "params": {},
        "desc": ""
    }
}

# ==========================================
# 3. Streamlit App Layout
# ==========================================

st.set_page_config(layout="wide", page_title="Coinmap API Explorer")


# --- Sidebar: Settings ---
st.sidebar.header("Exchange Settings")
# exchange_mode = st.sidebar.radio("Select Exchange", ["OKX (통합지갑)", "Binance (선물지갑)"])
exchange_mode = st.sidebar.radio("Select Exchange", ["OKX (통합지갑)"]) # Removed Binance for now

api_key = st.sidebar.text_input("API Key", type="password")
api_secret = st.sidebar.text_input("API Secret", type="password")
passphrase = ""
if exchange_mode.startswith("OKX"):
    passphrase = st.sidebar.text_input("Passphrase", type="password")

# --- Mode Selection ---
st.header("Coinmap API Explorer") 
mode = st.radio("Select Mode", ["Raw API Explorer", "Coinmap Simulator"], horizontal=True, label_visibility="collapsed")

if mode == "Raw API Explorer":
    # ==========================================
    # MODE A: Raw API Explorer
    # ==========================================

    current_endpoints = OKX_ENDPOINTS if exchange_mode.startswith("OKX") else BINANCE_ENDPOINTS
    endpoint_names = list(current_endpoints.keys())

    col1, col2 = st.columns([1, 2])

    with col1:
        selected_endpoint_name = st.selectbox("Select Endpoint", endpoint_names)
        selected_config = current_endpoints[selected_endpoint_name]
        st.info(f"**Path:** `{selected_config['path']}`")

    with col2:
        # Default params logic: Use params from config if available, else empty dict
        initial_params = selected_config.get('params', {})
        default_params_json = json.dumps(initial_params, indent=2)
        params_input = st.text_area("Edit Parameters (JSON)", value=default_params_json, height=150)
        
        try:
            params_dict = json.loads(params_input)
        except json.JSONDecodeError:
            st.error("Invalid JSON format in parameters!")
            params_dict = {}

    if st.button("조회하기", type="primary"):
        if not api_key or not api_secret:
            st.error("⚠️ Please enter API Key and Secret in the sidebar.")
            st.stop()
        
        result = None
        start_time = time.time()
        
        with st.spinner("Fetching data from exchange..."):
            if exchange_mode.startswith("OKX"):
                client = OKXClient(api_key, api_secret, passphrase)
                result = client.send_request(selected_config['method'], selected_config['path'], params_dict)
            else:
                client = BinanceClient(api_key, api_secret)
                result = client.send_request(selected_config['method'], selected_config['path'], params_dict)
                
        duration = time.time() - start_time
        
        st.subheader("Response Data")
        st.caption(f"Latency: {duration:.2f}s")
        st.json(result)
        
        if result:
            data_list = []
            if isinstance(result, dict) and 'data' in result and isinstance(result['data'], list):
                data_list = result['data']
            elif isinstance(result, list):
                data_list = result
            elif isinstance(result, dict) and 'code' not in result:
                 known_keys = ['assets', 'positions', 'otherAssets']
                 for k in known_keys:
                     if k in result:
                         st.write(f"**List found in key: `{k}`**")
                         st.dataframe(pd.DataFrame(result[k]))

            if data_list:
                st.success(f"Found {len(data_list)} records")
                try:
                    df = pd.DataFrame(data_list)
                    st.dataframe(df)
                except Exception:
                    pass

elif mode == "Coinmap Simulator":
    # ==========================================
    # MODE B: Logic Simulator
    # ==========================================
    st.info("기획에 포함된 데이터 수집 항목들을 시뮬레이션 해보세요.")

    scenarios = {}

    if exchange_mode.startswith("OKX"):
        scenarios = {
            "1. Current Balance (현재 자산)": {
                "exchange": "OKX",
                "endpoint": "/api/v5/account/balance",
                "params": {"ccy": "USDT"}, 
                "logic_type": "balance"
            },
            "2. Positions (포지션 현황)": {
                "exchange": "OKX",
                "endpoint": "/api/v5/account/positions",
                "params": {"instType": "SWAP"},
                "logic_type": "positions"
            },
            "3. External Deposit (외부 입금)": {
                "exchange": "OKX",
                "endpoint": "/api/v5/account/bills",
                "params": {"ccy": "USDT", "type": "1"}, # Transfer
                "logic_type": "bills_deposit"
            },
            "4. External Withdrawal (외부 출금)": {
                "exchange": "OKX",
                "endpoint": "/api/v5/account/bills",
                "params": {"ccy": "USDT", "type": "1"}, # Transfer
                "logic_type": "bills_withdrawal"
            },
            "5. Spot Sell (현물 매도)": {
                "exchange": "OKX",
                "endpoint": "/api/v5/trade/fills-history",
                "params": {"instType": "SPOT", "limit": "100"},
                "logic_type": "spot_sell"
            },
            "6. Spot Buy (현물 매수)": {
                "exchange": "OKX",
                "endpoint": "/api/v5/trade/fills-history",
                "params": {"instType": "SPOT", "limit": "100"},
                "logic_type": "spot_buy"
            },
            "7. Closed Positions (거래 내역)": {
                "exchange": "OKX",
                "endpoint": "/api/v5/account/positions-history",
                "params": {"instType": "SWAP", "limit": "100"},
                "logic_type": "closed_positions"
            }
        }
    else: # Binance
        scenarios = {
            "1. Current Balance (현재 자산)": {
                "exchange": "Binance",
                "endpoint": "/fapi/v2/account",
                "params": {},
                "logic_type": "binance_balance"
            },
            "2. Positions (포지션 현황)": {
                "exchange": "Binance",
                "endpoint": "/fapi/v2/account",
                "params": {},
                "logic_type": "binance_positions"
            },
            "3. External Deposit (외부 입금)": {
                "exchange": "Binance",
                "endpoint": "/fapi/v1/income",
                "params": {"limit": 1000},
                "logic_type": "binance_deposit"
            },
            "4. External Withdrawal (외부 출금)": {
                "exchange": "Binance",
                "endpoint": "/fapi/v1/income",
                "params": {"limit": 1000},
                "logic_type": "binance_withdrawal"
            }
        }
    
    scenario_name = st.selectbox("Select Scenario", list(scenarios.keys()))
    scenario = scenarios[scenario_name]
    
    
    # --- Inputs ---
    col_in1, col_in2 = st.columns(2)
    start_date = None
    start_ts_ms = 0
    
    # Balance check doesn't need start time
    # Also Binance Positions (snapshot) don't need start time
    no_start_time_needed = ['balance', 'positions', 'binance_balance', 'binance_positions']
    
    if scenario['logic_type'] not in no_start_time_needed:
        with col_in1:
            start_date = st.date_input("Start Date (Start_Time)", datetime.now())
            start_ts_ms = int(datetime.combine(start_date, datetime.min.time()).timestamp() * 1000)
            st.caption(f"Timestamp (ms): {start_ts_ms}")
        
    # --- Run ---
    if st.button("조회하기", type="primary"):
        if not api_key or not api_secret:
            st.error("⚠️ Please enter API Key and Secret in the sidebar.")
            st.stop()
            
        if scenario['exchange'] == "OKX" and not exchange_mode.startswith("OKX"):
            st.error("Please switch to OKX mode in sidebar.")
            st.stop()
        
        if scenario['exchange'] == "Binance" and not exchange_mode.startswith("Binance"):
             st.error("Please switch to Binance mode in sidebar.")
             st.stop()

        if scenario['exchange'] == "OKX":
            client = OKXClient(api_key, api_secret, passphrase)
        else:
            client = BinanceClient(api_key, api_secret)
        
        # 1. Fetch Data
        
        # Special handling for balance to fetch ALL currencies
        params = scenario['params'].copy()
        if scenario['logic_type'] == 'balance':
             # Remove ccy param to get all balances
             if 'ccy' in params:
                 del params['ccy']
        
        # Only show request details, not the raw JSON data
        st.write(f"Requesting `{scenario['endpoint']}` with params: `{params}`")
        
        if scenario['exchange'] == "OKX":
            raw_res = client.send_request("GET", scenario['endpoint'], params)
        else:
            raw_res = client.send_request("GET", scenario['endpoint'], params)
        
        # Error handling for both exchanges
        # OKX: {'code': '0', 'data': [...]}
        # Binance: list or dict. If error: {'code': -112, 'msg': ...}
        
        is_error = False
        if not raw_res:
             is_error = True
        elif isinstance(raw_res, dict):
             if scenario['exchange'] == "OKX" and raw_res.get('code') != '0':
                 is_error = True
             elif scenario['exchange'] == "Binance" and ('code' in raw_res or 'msg' in raw_res) and 'totalWalletBalance' not in raw_res:
                 # Binance account endpoint returns dict with 'totalWalletBalance', no 'code' on success
                 # But error returns 'code' and 'msg'
                 if 'code' in raw_res:
                     is_error = True
        
        if is_error:
            st.error(f"API Error: {raw_res}")
            st.stop()
            
        # Normalize Data to DF
        df = pd.DataFrame()
        
        if scenario['exchange'] == "OKX":
            raw_data = raw_res.get('data', [])
            if scenario['logic_type'] == 'balance':
                if raw_data:
                    details = raw_data[0].get('details', [])
                    df = pd.DataFrame(details)
            else:
                df = pd.DataFrame(raw_data)
                
        elif scenario['exchange'] == "Binance":
             # Binance /account returns dict (not list in 'data')
             if scenario['logic_type'] in ['binance_balance', 'binance_positions']:
                 pass 
             elif scenario['logic_type'] == 'binance_income':
                 if isinstance(raw_res, list):
                     df = pd.DataFrame(raw_res)
                 else:
                     st.warning("Unexpected format for Income")

        
        # 2. Filter & Calculate
        
        logic_type = scenario['logic_type']
        
        if logic_type == "balance":
             if df.empty:
                 st.warning("No balance data found.")
             else:
                 numeric_cols = ['eq', 'cashBal', 'uTime', 'availBal', 'frozenBal', 'eqUsd', 'upl']
                 for col in numeric_cols:
                     if col in df.columns:
                         df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

                 # --- 1. USDT (Futures/Unified) Section ---
                 st.markdown("##### [선물자산]")
                 usdt_row = df[df['ccy'] == 'USDT']
                 
                 if not usdt_row.empty:
                     # Extract values
                     row = usdt_row.iloc[0]
                     eq = float(row['eq'])           # Total Equity (Balance + UPL)
                     avail_bal = float(row['availBal']) # Available Balance
                     upl = float(row.get('upl', 0))   # Unrealized PnL
                     
                     # Calculate Breakdown
                     # 코인 (미실현손익 포함): eq - availBal
                     margin_used_incl_upl = eq - avail_bal
                     
                     # 코인 (미실현손익 미포함): eq - availBal - upl
                     margin_used_excl_upl = eq - avail_bal - upl
                     
                     # Create breakdown table
                     breakdown_data = [
                         {"구분": "포지션 증거금", "공식": "eq - availBal - upl", "값 (USDT)": f"{margin_used_excl_upl:,.4f}"},
                         {"구분": "미실현 손익", "공식": "upl", "값 (USDT)": f"{upl:,.4f}"},
                         {"구분": "코인 (포지션 증거금 + 미실현손익)", "공식": "eq - availBal", "값 (USDT)": f"{margin_used_incl_upl:,.4f}"},
                         {"구분": "현금 (여유 USDT)", "공식": "availBal", "값 (USDT)": f"{avail_bal:,.4f}"}
                     ]
                     st.table(pd.DataFrame(breakdown_data))

                     # Total Sum
                     st.success(f"**총 선물 자산 가치: ${eq:,.2f}**")
                 else:
                     st.warning("USDT Balance not found.")

                 # --- 2. Non-USDT (Spot Assets) Section ---
                 st.markdown("##### [현물자산]")
                 
                 non_usdt_df = df[df['ccy'] != 'USDT'].copy()
                 
                 if not non_usdt_df.empty:
                     # Select and Rename Columns
                     display_cols = ['ccy', 'eq', 'eqUsd']
                     non_usdt_table = non_usdt_df[display_cols].rename(columns={
                         'ccy': '코인명',
                         'eq': '수량 (eq)',
                         'eqUsd': '평가금액 (eqUsd)'
                     })
                     
                     # Sort by USD Value desc
                     non_usdt_table = non_usdt_table.sort_values(by='평가금액 (eqUsd)', ascending=False)
                     
                     st.table(non_usdt_table)
                     
                     # Total Sum
                     total_spot_usd = non_usdt_df['eqUsd'].sum()
                     st.success(f"**총 현물 자산 가치: ${total_spot_usd:,.2f}**")
                 else:
                     st.info("No non-USDT assets found.")

        elif logic_type == "positions":
            # Special handling for positions to get Contract Value (ctVal)
            st.markdown("##### [포지션 현황]")
            
            st.markdown(f"""
            **적용된 로직:**
            1. Filter: `instType == 'SWAP'` (Perpetual/Futures)
            2. Direction Check:
               - `posSide == 'net'`: Check `pos > 0` (Long) or `pos < 0` (Short)
               - `posSide == 'long'` or `short`
            3. Value Calculation:
               - **실제 수량 (Coins):** `Abs(pos) * ctVal` (Contract Size)
               - **평가 금액 (Notional):** `실제 수량 * markPx`
            4. Data Source: `/api/v5/account/positions` + `/api/v5/public/instruments` (for `ctVal`)
            """)
            
            # 1. Get Instruments for Contract Value
            status_placeholder = st.empty()
            status_placeholder.info("Fetching Instruments info to get Contract Value (`ctVal`)...")
            
            if df.empty:
                st.warning("No open positions found.")
                status_placeholder.empty()
            else:
                # Gather all unique instIds and instTypes
                unique_inst_ids = df['instId'].unique()
                present_inst_types = df['instType'].unique() if 'instType' in df.columns else ['SWAP']
                
                # Create a map for ctVal
                inst_map = {}
                
                # Fetch instrument details per instId to be robust (referencing okx_positions.py approach)
                try:
                    # Get unique (instId, instType) pairs
                    # Default instType to SWAP if missing
                    targets = []
                    if 'instType' in df.columns:
                        targets = df[['instId', 'instType']].drop_duplicates().to_dict('records')
                    else:
                        targets = [{'instId': x, 'instType': 'SWAP'} for x in df['instId'].unique()]
                    
                    for t in targets:
                        iid = t['instId']
                        itype = t.get('instType', 'SWAP')
                        
                        # Fetch specific instrument
                        # Using client.send_request (signed)
                        # okx_positions.py uses: GET /api/v5/public/instruments?instType=SWAP&instId={instId}
                        
                        params = {"instType": itype, "instId": iid}
                        inst_res = client.send_request("GET", "/api/v5/public/instruments", params)
                        
                        if inst_res and inst_res.get('code') == '0':
                            data = inst_res.get('data', [])
                            if data:
                                ct_val = data[0].get('ctVal')
                                if ct_val:
                                    inst_map[iid] = float(ct_val)
                                else:
                                    # Try to find it manually if missing?
                                    inst_map[iid] = 0.0
                        else:
                            st.warning(f"Failed to fetch info for {iid}: {inst_res}")
                            
                except Exception as e:
                    st.error(f"Error fetching instruments: {e}")
                
                # Clear the loading status
                status_placeholder.empty()
                
                # Process Positions Data
                display_data = []
                
                for _, pos in df.iterrows():
                    inst_id = pos['instId']
                    ct_val = inst_map.get(inst_id, 0.0) # Default to 0 if not found
                    pos_sz = float(pos['pos']) # Number of contracts
                    
                    # Calculate real quantity
                    real_qty = abs(pos_sz) * ct_val
                    
                    # Direction
                    # posSide: long, short, net
                    # If net, we check pos > 0 (Long) or pos < 0 (Short)
                    raw_side = pos['posSide']
                    final_side = raw_side.upper()
                    
                    if raw_side == 'net':
                        if pos_sz > 0:
                            final_side = 'LONG'
                        elif pos_sz < 0:
                            final_side = 'SHORT'
                        else:
                            final_side = 'FLAT'
                    elif raw_side == 'long':
                        final_side = 'LONG'
                    elif raw_side == 'short':
                        final_side = 'SHORT'

                    # Date (cTime)
                    try:
                        ctime_ts = int(pos['cTime']) / 1000
                        date_str = datetime.fromtimestamp(ctime_ts).strftime('%Y-%m-%d %H:%M:%S')
                    except:
                        date_str = pos['cTime']

                    display_data.append({
                        "날짜": date_str,
                        "코인명": inst_id,
                        "방향": final_side,
                        "평균매수가": f"{float(pos['avgPx']):,.4f}",
                        "계약수": f"{pos_sz:g}",
                        "ctVal": ct_val,
                        "실제 수량 (계약수 * ctVal)": f"{real_qty:g}",
                        "배율": pos['lever'],
                        "미실현손익": f"{float(pos['upl']):,.2f}",
                        "수익률 (%)": f"{float(pos.get('uplRatio', 0))*100:.2f}%"
                    })
                
                st.table(pd.DataFrame(display_data))

        elif logic_type in ["bills_deposit", "bills_withdrawal"]:
    
            # Bills Logic
            # Deposit: subType='11' (Transfer In), ccy='USDT'
            # Withdrawal: subType='12' (Transfer Out), ccy='USDT'
            
            target_subtype = '11' if logic_type == "bills_deposit" else '12'
            filter_desc = "Transfer In (11)" if logic_type == "bills_deposit" else "Transfer Out (12)"
            
            st.markdown(f"""
            **적용된 로직:**
            1. Filter `subType == '{target_subtype}'` ({filter_desc})
            2. Filter `ccy == 'USDT'`
            3. Filter `ts >= Start_Time`
            4. Calculate `Sum(|balChg|)`
            """)
            
            if df.empty:
                st.warning("No bills data returned.")
            else:
                df['ts'] = df['ts'].astype(int)
                df['balChg'] = df['balChg'].astype(float)
                
                filtered_df = df[
                    (df['subType'] == target_subtype) &
                    (df['ccy'] == 'USDT') &
                    (df['ts'] >= start_ts_ms)
                ].copy()
                
                
                st.dataframe(filtered_df)
                
                if not filtered_df.empty:
                    total_sum = filtered_df['balChg'].abs().sum()
                    st.success(f"**총 금액: {total_sum:,.2f} USDT**")
                else:
                    st.warning("No records match the filter criteria.")

        elif logic_type in ["spot_sell", "spot_buy"]:
            # Spot Logic
            target_side = 'sell' if logic_type == "spot_sell" else 'buy'
            
            st.markdown(f"""
            **적용된 로직:**
            1. Filter `side == '{target_side}'`
            2. Filter `ts >= Start_Time`
            3. Calculate `Sum(fillPx * fillSz)`
            """)
            
            if df.empty:
                st.warning("No trade data returned.")
            else:
                df['ts'] = df['ts'].astype(int)
                df['fillPx'] = df['fillPx'].astype(float)
                df['fillSz'] = df['fillSz'].astype(float)
                
                filtered_df = df[
                    (df['side'] == target_side) & 
                    (df['ts'] >= start_ts_ms)
                ].copy()
                
                
                st.dataframe(filtered_df)
                
                if not filtered_df.empty:
                    filtered_df['volume_usdt'] = filtered_df['fillPx'] * filtered_df['fillSz']
                    total_sum = filtered_df['volume_usdt'].sum()
                
                    st.dataframe(filtered_df[['ts', 'instId', 'side', 'fillPx', 'fillSz', 'volume_usdt']])
                    st.success(f"총 현물 {target_side.title()} 금액: {total_sum:,.2f} USDT")
                else:
                    st.warning("No records match the filter criteria.")

        elif logic_type == "closed_positions":
            st.markdown("##### [최근 거래 내역 (Closed Positions)]")
            
            if df.empty:
                st.warning("No closed positions found.")
            else:
                # Ensure numeric types for sorting
                df['uTime'] = pd.to_numeric(df['uTime'])
                # Sort by time descending (should be default but ensuring)
                df = df.sort_values('uTime', ascending=False)
                
                display_data = []
                for _, row in df.iterrows():
                    # uTime: Transaction update time
                    try:
                        ts = float(row['uTime']) / 1000
                        date_str = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
                    except:
                        date_str = row['uTime']
                        
                    display_data.append({
                        "날짜": date_str,
                        "코인명": row.get('instId'),
                        "수익금": f"{float(row.get('realizedPnl', 0)):,.2f}",
                        "진입가": f"{float(row.get('openAvgPx', 0)):,.4f}",
                        "청산가": f"{float(row.get('closeAvgPx', 0)):,.4f}",
                        "레버리지": row.get('lever')
                    })
                
                st.table(pd.DataFrame(display_data))
                
                # Summary
                total_pnl = df['realizedPnl'].astype(float).sum()
                st.info(f"**조회된 내역 총 실현 손익:** {total_pnl:,.2f} USDT")

        elif logic_type == "binance_balance":
            # raw_res is the dict from /fapi/v2/account
            st.markdown("##### [자산 현황]")
            
            if not raw_res:
                st.warning("No data received.")
            else:
                # Key metrics according to spec
                # Total Equity = totalWalletBalance + totalUnrealizedProfit
                wallet_bal = float(raw_res.get('totalWalletBalance', 0))
                upl = float(raw_res.get('totalUnrealizedProfit', 0))
                total_equity = wallet_bal + upl
                avail_bal = float(raw_res.get('availableBalance', 0))
                # margin_used = float(raw_res.get('totalInitialMargin', 0)) # Not used in this formula style
                
                # Calculate Breakdown (Matching OKX Logic)
                # 코인 (미실현손익 포함): eq - availBal
                margin_used_incl_upl = total_equity - avail_bal
                
                # 코인 (미실현손익 미포함): eq - availBal - upl
                margin_used_excl_upl = total_equity - avail_bal - upl
                
                # Create breakdown table
                breakdown_data = [
                    {"구분": "포지션 증거금", "공식": "totalWalletBalance - availableBalance", "값 (USDT)": f"{margin_used_excl_upl:,.4f}"},
                    {"구분": "미실현 손익", "공식": "totalUnrealizedProfit", "값 (USDT)": f"{upl:,.4f}"},
                    {"구분": "코인 (포지션 증거금 + 미실현손익)", "공식": "totalWalletBalance + totalUnrealizedProfit - availableBalance", "값 (USDT)": f"{margin_used_incl_upl:,.4f}"},
                    {"구분": "현금 (여유 USDT)", "공식": "availableBalance", "값 (USDT)": f"{avail_bal:,.4f}"}
                ]
                st.table(pd.DataFrame(breakdown_data))
                st.success(f"**총 선물 자산 가치: ${total_equity:,.2f}**")

        elif logic_type == "binance_positions":
             # raw_res is the dict from /fapi/v2/account
             st.markdown("##### [포지션 현황]")
             
             positions = raw_res.get('positions', [])
             if not positions:
                 st.warning("No position data found.")
             else:
                 pos_df = pd.DataFrame(positions)
                 # Filter only open positions (amt != 0)
                 pos_df['positionAmt'] = pd.to_numeric(pos_df['positionAmt'])
                 open_pos = pos_df[pos_df['positionAmt'] != 0].copy()
                 
                 if open_pos.empty:
                     st.warning("No active open positions.")
                 else:
                     display_data = []
                     for _, row in open_pos.iterrows():
                         amt = float(row['positionAmt'])
                         entry_price = float(row['entryPrice'])
                         unrealized_profit = float(row['unrealizedProfit'])
                         leverage = row['leverage']
                         symbol = row['symbol']
                         
                         # Direction
                         side = "LONG" if amt > 0 else "SHORT"
                         
                         # ROI Calculation: upl / initialMargin (approx)
                         # Binance provides 'initialMargin' per position in some endpoints, but here we might need to calc or use isolation logic.
                         # Simple ROI = (Unrealized PnL / (Entry Price * Size / Leverage)) * 100
                         # Margin = (Entry * |Size|) / Leverage
                         margin = (entry_price * abs(amt)) / float(leverage)
                         roi = (unrealized_profit / margin * 100) if margin > 0 else 0.0
                         
                         display_data.append({
                             "날짜": row.get('updateTime', '-'), # v2/account positions might not have updateTime per pos, check doc
                             "코인명": symbol,
                             "방향": side,
                             "수량 (Coins)": f"{abs(amt):g}",
                             "진입가": f"{entry_price:,.4f}",
                             "배율": leverage,
                             "수익금 (UPL)": f"{unrealized_profit:,.2f}",
                             "수익률 (%)": f"{roi:.2f}%"
                         })
                     
                     st.table(pd.DataFrame(display_data))

        elif logic_type in ["binance_deposit", "binance_withdrawal"]:
            target_type = "외부 입금" if logic_type == "binance_deposit" else "외부 출금"
            st.markdown(f"##### [{target_type}]")
        
            st.markdown(f"""
            1. Query param `incomeType = 'TRANSFER'`
            2. **입금:** `income > 0`
            3. **출금:** `income < 0`
            4. Filter `time >= Start_Time`
            """)
            
            
            with st.spinner("Re-fetching with incomeType='TRANSFER'..."):
                # Create new params specifically for this flow
                transfer_params = scenario['params'].copy()
                transfer_params['incomeType'] = 'TRANSFER'
                transfer_params['limit'] = 1000 

                if start_ts_ms > 0:
                    transfer_params['startTime'] = start_ts_ms
                
                # Re-fetch
                if scenario['exchange'] == "OKX":
                     # Should not happen here but safety check
                     res = client.send_request("GET", scenario['endpoint'], transfer_params)
                else:
                     res = client.send_request("GET", scenario['endpoint'], transfer_params)
                
                if isinstance(res, list):
                    df_transfer = pd.DataFrame(res)
                elif isinstance(res, dict) and 'code' in res:
                     st.error(f"API Error: {res}")
                     df_transfer = pd.DataFrame()
                else:
                     df_transfer = pd.DataFrame()

            if df_transfer.empty:
                st.warning("No transfer data found (API returned empty list).")
            else:
                 # Filter Transfers (Double check even though we asked for it)
                 if 'time' in df_transfer.columns:
                     df_transfer['time'] = pd.to_numeric(df_transfer['time'])
                 if 'income' in df_transfer.columns:
                     df_transfer['income'] = pd.to_numeric(df_transfer['income'])
                 
                 # Filter by start time
                 target_df = df_transfer[df_transfer['time'] >= start_ts_ms].copy()
                 
                 if target_df.empty:
                     st.info(f"Debug: No records found after {start_ts_ms} (Start Time).")
                     st.write("Raw Data Sample (first 5):")
                     st.write(df_transfer.head())
                 else:
                     # Filter based on logic_type
                     if logic_type == "binance_deposit":
                         final_df = target_df[target_df['income'] > 0].copy()
                         total_sum = final_df['income'].sum()
                         label = "총 입금액"
                     else:
                         final_df = target_df[target_df['income'] < 0].copy()
                         total_sum = final_df['income'].abs().sum()
                         label = "총 출금액"
                     
                     if final_df.empty:
                         st.warning(f"No {target_type} records found.")
                         # Additional Debug for split logic failure
                         st.write("Debug: Target records found but filtered out by direction (pos/neg).")
                         st.write(target_df.head())
                     else:
                         st.success(f"**{label}: {total_sum:,.2f} USDT**")
                         
                         # Convert timestamp for readability
                         display_df = final_df[['time', 'symbol', 'incomeType', 'income', 'asset', 'tranId']].copy()
                         display_df['날짜'] = display_df['time'].apply(
                             lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S')
                         )
                         # Move date column to front
                         cols = ['날짜'] + [c for c in display_df.columns if c != '날짜']
                         display_df = display_df[cols]
                         
                         st.dataframe(display_df)

    # --- ROI Simulator ---
    st.divider()
    st.header("ROI Simulator")
    st.info("조회한 데이터들을 바탕으로 최종 ROI를 시뮬레이션합니다.")
    
    # Initialize Trade Log in Session State
    if 'sim_trades' not in st.session_state:
        st.session_state.sim_trades = []
        
    # Callback to update balance based on trades and other inputs
    def update_end_bal_from_trades():
        sb = st.session_state.get('sim_start_bal', 0.0)
        ed = st.session_state.get('sim_ext_dep', 0.0)
        ew = st.session_state.get('sim_ext_wd', 0.0)
        manual_pnl = st.session_state.get('sim_manual_pnl', 0.0)
        
        # Calculate Totals from sim_trades
        ss = sum(t['amount'] for t in st.session_state.sim_trades if t['type'] == 'SELL')
        sb_amt = sum(t['amount'] for t in st.session_state.sim_trades if t['type'] == 'BUY')
        
        # Store totals in session state for access elsewhere if needed
        st.session_state.sim_spot_sell = ss
        st.session_state.sim_spot_buy = sb_amt
        
        pass


    pass 

    # Initialize Futures PnL Log in Session State
    if 'sim_futures_pnl' not in st.session_state:
        st.session_state.sim_futures_pnl = []

    # Define Callbacks
    def add_sell_callback():
        amt = st.session_state.new_sell_input
        pnl = st.session_state.get('new_sell_pnl', 0.0)
        if amt > 0:
            st.session_state.sim_trades.append({'type': 'SELL', 'amount': amt, 'pnl': pnl})
            recalc_balance()

    def add_buy_callback():
        amt = st.session_state.new_buy_input
        if amt > 0:
            st.session_state.sim_trades.append({'type': 'BUY', 'amount': amt, 'pnl': 0.0})
            recalc_balance()
            
    def del_trade_callback(idx):
        st.session_state.sim_trades.pop(idx)
        recalc_balance()
        
    def add_futures_pnl_callback():
        pnl = st.session_state.new_futures_pnl_input
        if pnl != 0:
            st.session_state.sim_futures_pnl.append(pnl)
            recalc_balance()
            
    def del_futures_pnl_callback(idx):
        st.session_state.sim_futures_pnl.pop(idx)
        recalc_balance()
        
    def reset_trades_callback():
        st.session_state.sim_trades = []
        st.session_state.sim_futures_pnl = []
        # Reset all input fields
        st.session_state.sim_start_bal = 0.0
        st.session_state.sim_end_bal = 0.0
        st.session_state.sim_ext_dep = 0.0
        st.session_state.sim_ext_wd = 0.0
        st.session_state.sim_manual_pnl = 0.0
        st.session_state.sim_init_spot = 0.0
        recalc_balance()
        
    def recalc_balance():
        sb = st.session_state.get('sim_start_bal', 0.0)
        ed = st.session_state.get('sim_ext_dep', 0.0)
        ew = st.session_state.get('sim_ext_wd', 0.0)
        
        # Sum manual PnL list
        manual_pnl_sum = sum(st.session_state.sim_futures_pnl)
        
        ss = sum(t['amount'] for t in st.session_state.sim_trades if t['type'] == 'SELL')
        sb_amt = sum(t['amount'] for t in st.session_state.sim_trades if t['type'] == 'BUY')
        
        st.session_state.sim_spot_sell = ss
        st.session_state.sim_spot_buy = sb_amt
        
        st.session_state.sim_end_bal = sb + ed - ew + ss - sb_amt + manual_pnl_sum

    # Row 1: Balances
    c1, c2 = st.columns(2)
    with c1:
        val_start_bal = st.number_input("1. 시작 자산", min_value=0.0, value=0.0, key="sim_start_bal", on_change=recalc_balance)
    with c2:
        if 'sim_end_bal' not in st.session_state:
            st.session_state.sim_end_bal = 0.0
        val_end_bal = st.number_input("2. 현재 자산", min_value=0.0, value=st.session_state.sim_end_bal, key="sim_end_bal")
        
    
    st.markdown("**선물 손익**")
    c_pnl_in, c_pnl_btn = st.columns([3, 1])
    with c_pnl_in:
        st.number_input("선물 PnL 추가 (+/-)", value=0.0, key="new_futures_pnl_input", label_visibility="collapsed")
    with c_pnl_btn:
        st.button("➕ PnL", key="add_fpnl_btn", on_click=add_futures_pnl_callback)
    
    # Display Futures PnL List
    if st.session_state.sim_futures_pnl:
        pnl_sum = sum(st.session_state.sim_futures_pnl)
        st.caption(f"Total Futures PnL: {pnl_sum:,.2f}")
        with st.expander(f"선물 PnL 내역 ({len(st.session_state.sim_futures_pnl)}건)", expanded=False):
            for i, p in enumerate(st.session_state.sim_futures_pnl):
                c_txt, c_del = st.columns([3, 1])
                with c_txt:
                    st.write(f"#{i+1}: {p:+,.2f}")
                with c_del:
                    st.button("❌", key=f"del_fpnl_{i}", on_click=del_futures_pnl_callback, args=(i,))

    st.markdown("---")
    
    # Row 2: Flows
    c3, c4, c5 = st.columns(3)
    
    # Calculate current totals for display
    current_sells = [t for t in st.session_state.sim_trades if t['type'] == 'SELL']
    current_buys = [t for t in st.session_state.sim_trades if t['type'] == 'BUY']
    val_spot_sell = sum(t['amount'] for t in current_sells)
    val_spot_buy = sum(t['amount'] for t in current_buys)
    
    with c3:
        st.markdown("**[입금]**")
        val_external_dep = st.number_input("3. 총 외부 입금액", min_value=0.0, value=0.0, key="sim_ext_dep", on_change=recalc_balance)
        
        if exchange_mode.startswith("OKX"):
            st.markdown(f"##### 현물 매도: {val_spot_sell:,.2f}")
            
            # Add Sell Interface
            c_sell_in, c_sell_pnl, c_sell_btn = st.columns([2, 2, 1])
            with c_sell_in:
                st.caption("매도 금액")
                st.number_input("매도 금액 추가", min_value=0.0, key="new_sell_input", label_visibility="collapsed")
            with c_sell_pnl:
                st.caption("매도 PnL")
                st.number_input("해당 거래 손익 (Optional)", value=0.0, key="new_sell_pnl", label_visibility="collapsed", help="현물 잔고 추적용 (수익: +, 손실: -)")
            with c_sell_btn:
                st.caption(" ") # Use caption as spacer to match height of labels
                st.button("➕", key="add_sell_btn", on_click=add_sell_callback)

            # List Sells
            if current_sells:
                with st.expander("매도 내역", expanded=True):
                    for i, t in enumerate(st.session_state.sim_trades):
                        if t['type'] == 'SELL':
                            c_txt, c_del = st.columns([3, 1])
                            with c_txt:
                                pnl_txt = f"(PnL: {t.get('pnl', 0):+g})" if t.get('pnl', 0) != 0 else ""
                                st.caption(f"#{i+1}: {t['amount']:,.2f} {pnl_txt}")
                            with c_del:
                                st.button("❌", key=f"del_trade_{i}", on_click=del_trade_callback, args=(i,))
        else:
            val_spot_sell = 0.0

    with c4:
        st.markdown("**[출금]**")
        val_external_wd = st.number_input("5. 총 외부 출금액", min_value=0.0, value=0.0, key="sim_ext_wd", on_change=recalc_balance)
        
        if exchange_mode.startswith("OKX"):
            st.markdown(f"##### 현물 매수: {val_spot_buy:,.2f}")
            
            # Add Buy Interface
            c_buy_in, c_buy_btn = st.columns([2, 1])
            with c_buy_in:
                st.caption("매수 금액")
                st.number_input("매수 금액 추가", min_value=0.0, key="new_buy_input", label_visibility="collapsed")
            with c_buy_btn:
                st.caption(" ") # Use caption as spacer to match height of labels
                st.button("➕", key="add_buy_btn", on_click=add_buy_callback)
                        
            # List Buys
            if current_buys:
                with st.expander("매수 내역", expanded=True):
                    for i, t in enumerate(st.session_state.sim_trades):
                        if t['type'] == 'BUY':
                            c_txt, c_del = st.columns([3, 1])
                            with c_txt:
                                st.caption(f"#{i+1}: {t['amount']:,.2f}")
                            with c_del:
                                st.button("❌", key=f"del_trade_{i}", on_click=del_trade_callback, args=(i,))
        else:
            val_spot_buy = 0.0

    with c5:
        st.markdown("**[현물 잔고]**")
        if exchange_mode.startswith("OKX"):
            val_initial_spot = st.number_input("초기 현물 잔고", min_value=0.0, value=0.0, key="sim_init_spot", on_change=recalc_balance)
            
            # Calculate Current Spot Balance using master list
            curr_init_spot = st.session_state.get('sim_init_spot', 0.0)
            
            # New Logic: Track Spot Balance strictly by chronological flow with PnL
            # Spot Bal = Initial
            # + Buy Amount
            # - (Sell Amount - PnL)  <-- (Sell Amount - PnL) is the COST BASIS that leaves the wallet.
            # Example: Buy 30. Bal 30.
            # Sell 10 (PnL 0, Partial). Cost Basis = 10. New Bal = 30 - 10 = 20. Correct.
            # Sell 10 (PnL -10, Full Loss). Received 10. PnL -10.
            # Cost Basis = 10 - (-10) = 20.
            # New Bal = 30 - 20 = 10. Correct.
            
            calc_spot_bal = curr_init_spot
            for t in st.session_state.sim_trades:
                if t['type'] == 'BUY':
                    calc_spot_bal += t['amount']
                elif t['type'] == 'SELL':
                    sell_amt = t['amount']
                    pnl = t.get('pnl', 0.0)
                    cost_basis = sell_amt - pnl
                    calc_spot_bal -= cost_basis
            
            val_current_spot = max(0.0, calc_spot_bal)
            
            st.number_input("현재 현물 잔고", value=val_current_spot, disabled=True, help="초기 + 매수 - (매도 - PnL). 순차적 계산 결과.")
            
            st.button("초기화", type="primary", on_click=reset_trades_callback)
        else:
            val_initial_spot = 0.0

    # --- High-Water Mark Logic (Anti-Abuse) ---
    hwm_spot_flow = 0.0
    use_hwm = True # Default to True now
    
    if exchange_mode.startswith("OKX"):
        st.markdown("---")
        # Calculate HWM from sim_trades
        cum_sell = 0.0
        cum_buy = 0.0
        max_val = 0.0
        
        debug_logs = []
        
        for t in st.session_state.sim_trades:
            side = t['type']
            amt = t['amount']
            
            if side == 'SELL':
                cum_sell += amt
            elif side == 'BUY':
                cum_buy += amt
                
            # Formula: (CumSell - CumBuy) - Min(CumSell, InitialSpot)
            # Explanation of Logic regarding partial/full close:
            # If we bought 30 (CumBuy=30), then sold 10 (CumSell=10).
            # If Initial=0. 
            # Flow = (10 - 30) - 0 = -20.
            # This correctly reflects that we spent more than we received back (net outflow 20).
            # Even if that "Sell 10" was a "Loss 20" (sold something worth 30 for 10), the CASH FLOW is just +10.
            # The ROI logic cares about CASH FLOW (Capital Injection/Extraction).
            # Capital Injection = Net Spot Buy (if negative net flow).
            # Capital Extraction = Net Spot Sell.
            # The HWM tracks the "Peak Capital Extraction" (adjusted for initial).
            # If Flow is negative (we bought more than sold), it contributes 0 to Principal Adjustment (Max(0, Flow)).
            # So yes, this logic works regardless of whether it's partial close or loss close.
            # It strictly tracks: "How much USDT did I pull out of Spot vs Put in?"
            
            deduction = min(cum_sell, val_initial_spot)
            flow = (cum_sell - cum_buy) - deduction
            
            if flow > max_val:
                max_val = flow
                
            debug_logs.append(f"{side} {amt} -> Net Spot Flow: {flow:,.2f} (HWM: {max_val:,.2f})")
            
        hwm_spot_flow = max_val
        
        with st.expander("기간내 Net Spot Flow 최대값 (HWM)", expanded=True):
            st.write(f"**HWM:** {hwm_spot_flow:,.2f}")
            st.caption("Calculated from Trade Sequence:")
            if debug_logs:
                st.text('\n'.join(debug_logs))
            else:
                st.text("No trades recorded.")
    
    # --- Calculations ---
    # Override manual inputs if HWM is active
    if exchange_mode.startswith("OKX") and use_hwm:
        val_spot_sell = val_spot_sell # Already set from sim_trades
        val_spot_buy = val_spot_buy # Already set from sim_trades

    # 1. Total Deposit & Withdrawal (for Adjusted PnL)
    total_deposit = val_external_dep + val_spot_sell
    total_withdrawal = val_external_wd + val_spot_buy
    


    # 2. Net Spot Flow
    # Net Spot Flow = Spot Sell - Spot Buy - min(Spot Sell, Init Spot Bal)
    net_spot_flow_raw = val_spot_sell - val_spot_buy
    spot_deduction = min(val_spot_sell, val_initial_spot)
    net_spot_flow = net_spot_flow_raw - spot_deduction
    
    # 3. Principal Adjustment (OKX Logic vs Binance Logic)
    # Principal Adj = External Dep + Max(0, Net Spot Flow)
    if exchange_mode.startswith("OKX"):
        if use_hwm:
            principal_adj = val_external_dep + max(0, hwm_spot_flow)
        else:
            principal_adj = val_external_dep + max(0, net_spot_flow)
    else:
        # Binance Logic: Principal Adj = External Dep only (Transfer In)
        principal_adj = val_external_dep
    
    # 4. Adjusted PnL
    # (End - Start) - Total Dep + Total Wd
    raw_pnl = val_end_bal - val_start_bal
    adjusted_pnl = raw_pnl - total_deposit + total_withdrawal
    
    # 5. ROI
    # (Adj PnL / (Start + Principal Adj)) * 100
    denominator = val_start_bal + principal_adj
    roi = (adjusted_pnl / denominator * 100) if denominator > 0 else 0.0
    
    # --- Display Results ---
    st.divider()
    
    # Result Metrics
    m1, m2, m3 = st.columns(3)
    m1.metric("조정 순이익", f"{adjusted_pnl:,.2f} USDT")
    m2.metric("시작 자산 보정액", f"{principal_adj:,.2f} USDT")
    m3.metric("ROI (%)", f"{roi:.2f}%")
    
    # Detailed Breakdown
    with st.expander("상세 계산 과정 보기 (Click to expand)", expanded=True):
        if exchange_mode.startswith("OKX"):
            st.markdown("#### 1. 입출금 집계")
            st.code(f"""
            총 입금액 = 외부 입금({val_external_dep}) + 현물 매도({val_spot_sell}) = {total_deposit:,.2f}
            총 출금액 = 외부 출금({val_external_wd}) + 현물 매수({val_spot_buy}) = {total_withdrawal:,.2f}
            """)
            
            st.markdown("#### 2. 시작 자산 보정")
            if use_hwm:
                st.code(f"""
                High-Water Mark (Peak Spot Flow) = {hwm_spot_flow:,.2f}
                (Calculated from Trade Log above)
                
                시작 자산 보정액 = 외부 입금({val_external_dep}) + Max(0, HWM({hwm_spot_flow:,.2f}))
                              = {val_external_dep} + {max(0, hwm_spot_flow):,.2f}
                              = {principal_adj:,.2f}
                """)
            else:
                st.code(f"""
                
                Net Spot Flow = 현물 매도({val_spot_sell}) - 현물 매수({val_spot_buy}) - [min(현물 매도({val_spot_sell}), 현물 초기 잔고({val_initial_spot}))] = {net_spot_flow:,.2f}
                
                시작 자산 보정액 = 외부 입금({val_external_dep}) + Max(0, Net Spot Flow({net_spot_flow:,.2f}))
                              = {val_external_dep} + {max(0, net_spot_flow):,.2f}
                              = {principal_adj:,.2f}
                """)
        else:
            # Binance Breakdown
            st.markdown("#### 1. 입출금 집계 (Binance Futures)")
            st.code(f"""
            총 입금액 (Transfer In) = {total_deposit:,.2f}
            총 출금액 (Transfer Out) = {total_withdrawal:,.2f}
            (현물 거래 내역은 선물 ROI 계산에 포함되지 않음)
            """)
            
            st.markdown("#### 2. 시작 자산 보정")
            st.code(f"""
            시작 자산 보정액 = 외부 입금 (Transfer In)
                          = {principal_adj:,.2f}
            """)

        st.markdown("#### 3. 조정 순이익")
        st.code(f"""
        조정 순이익 = 현재자산({val_end_bal}) - 시작자산({val_start_bal}) - 총 입금액({total_deposit:,.2f}) + 총 출금액({total_withdrawal:,.2f})
                = {adjusted_pnl:,.2f}
        """)
        
        st.markdown("#### 4. 최종 ROI (%)")
        st.code(f"""
        ROI (%) = 조정 순이익({adjusted_pnl:,.2f}) / (시작 자산({val_start_bal:,.2f}) + 시작 자산 보정액({principal_adj:,.2f})) * 100
            = {roi:.2f}%
        """)
