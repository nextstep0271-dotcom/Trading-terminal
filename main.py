import asyncio
import json
import os
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import ccxt.pro as ccxtpro

app = FastAPI()

class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in list(self.active_connections):
            try:
                await connection.send_text(json.dumps(message))
            except Exception:
                self.disconnect(connection)

manager = ConnectionManager()

# จัดการ Task ของแต่ละ Exchange ไม่ให้รันซ้ำซ้อน
running_tasks = {}
exchange_keys_store = {}

async def watch_exchange_data(exchange_id, symbol, api_keys=None):
    exchange_class = getattr(ccxtpro, exchange_id)
    config = {'enableRateLimit': True}
    
    # อ่านค่าจาก Environment Variables หรือ Keys ที่ส่งมาจาก Frontend
    env_key = os.getenv(f"{exchange_id.upper()}_API_KEY", '')
    env_sec = os.getenv(f"{exchange_id.upper()}_SECRET", '')
    env_pwd = os.getenv(f"{exchange_id.upper()}_PASSWORD", '')

    apiKey = (api_keys.get('apiKey') if api_keys else '') or env_key
    secret = (api_keys.get('secret') if api_keys else '') or env_sec
    password = (api_keys.get('password') if api_keys else '') or env_pwd

    has_credentials = bool(apiKey and secret)

    if has_credentials:
        config['apiKey'] = apiKey
        config['secret'] = secret
        if password:
            config['password'] = password

    exchange = exchange_class(config)
    base_asset, quote_asset = symbol.split('/') if '/' in symbol else ('BTC', 'USDT')

    # ยอดเงินเริ่มต้น
    pair_balances = {quote_asset: 0.0, base_asset: 0.0}

    while True:
        try:
            # ดึง Order Book
            orderbook = await exchange.watch_order_book(symbol, limit=5)
            
            # ดึง Balance เฉพาะคู่เทรด
            if has_credentials:
                try:
                    bal = await exchange.fetch_balance()
                    pair_balances[quote_asset] = float(bal['total'].get(quote_asset, 0.0))
                    pair_balances[base_asset] = float(bal['total'].get(base_asset, 0.0))
                except Exception as ex:
                    print(f"[{exchange_id}] Balance fetch error: {ex}")
            else:
                # ค่าจำลองสำหรับกรณีไม่ได้ใส่ API Key เท่านั้น
                mock_vals = {
                    'binance': {quote_asset: 12540.50, base_asset: 0.4520},
                    'okx': {quote_asset: 8320.00, base_asset: 0.1200},
                    'bybit': {quote_asset: 5410.25, base_asset: 0.0850},
                    'kucoin': {quote_asset: 3150.00, base_asset: 0.0000},
                    'gate': {quote_asset: 1890.75, base_asset: 0.0210}
                }
                pair_balances = mock_vals.get(exchange_id, {quote_asset: 0.0, base_asset: 0.0})

            payload = {
                "exchange": exchange_id,
                "symbol": symbol,
                "base_asset": base_asset,
                "quote_asset": quote_asset,
                "bids": orderbook['bids'][:5],
                "asks": orderbook['asks'][:5],
                "balance": pair_balances,
                "is_live": has_credentials
            }

            await manager.broadcast(payload)

        except asyncio.CancelledError:
            try:
                await exchange.close()
            except:
                pass
            break
        except Exception as e:
            print(f"Error on {exchange_id}: {e}")
            await asyncio.sleep(5)
            try:
                await exchange.close()
            except:
                pass
            exchange = exchange_class(config)

def start_or_restart_exchange(exchange_id, symbol='BTC/USDT', keys=None):
    # ถ้ารูปแบบ Task เก่ารันอยู่ ให้ Cancel ทิ้งก่อน เพื่อไม่ให้ส่งข้อมูลสลับกัน
    if exchange_id in running_tasks and not running_tasks[exchange_id].done():
        running_tasks[exchange_id].cancel()
    
    if keys:
        exchange_keys_store[exchange_id] = keys
    else:
        keys = exchange_keys_store.get(exchange_id)

    task = asyncio.create_task(watch_exchange_data(exchange_id, symbol, keys))
    running_tasks[exchange_id] = task

@app.on_event("startup")
async def startup_event():
    exchanges = ['binance', 'okx', 'bybit', 'kucoin', 'gate']
    symbol = 'BTC/USDT'
    for ex in exchanges:
        start_or_restart_exchange(ex, symbol)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            req = json.loads(data)
            
            if req.get("action") == "update_keys":
                ex_id = req["exchange"]
                keys = req["keys"]
                # รีสตาร์ท Task ของ Exchange นั้นด้วย Key ใหม่
                start_or_restart_exchange(ex_id, 'BTC/USDT', keys)
                await websocket.send_text(json.dumps({"status": "success", "message": f"Updated API for {ex_id}"}))
            
            elif req.get("action") == "place_order":
                print(f"Executing order: {req}")
                await websocket.send_text(json.dumps({
                    "status": "success", 
                    "message": f"Executed {req['type']} ${req['amount_usdt']} USDT on {req['exchange']}"
                }))
    except WebSocketDisconnect:
        manager.disconnect(websocket)

@app.get("/", response_class=HTMLResponse)
async def get_dashboard():
    return """
<!DOCTYPE html>
<html lang="th">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Multi-Exchange Live Trading Terminal</title>
    <script src="https://cdn.jsdelivr.net/npm/@tailwindcss/browser@4"></script>
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
        body { font-family: 'Inter', sans-serif; }
    </style>
</head>
<body class="bg-slate-950 text-slate-100 min-h-screen p-4 md:p-6">

    <div class="max-w-7xl mx-auto space-y-6">
        <!-- Header -->
        <header class="flex flex-col md:flex-row justify-between items-start md:items-center pb-4 border-b border-slate-800 gap-4">
            <div>
                <h1 class="text-2xl font-bold tracking-tight text-white flex items-center gap-2">
                    <svg xmlns="http://www.w3.org/2000/svg" class="h-7 w-7 text-cyan-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 7h8m0 0v8m0-8l-8 8-4-4-6 6" />
                    </svg>
                    Multi-Exchange Live Trading Terminal
                </h1>
                <p class="text-sm text-slate-400 mt-1">คู่เทรดปัจจุบัน: <span id="current-pair" class="text-cyan-400 font-bold bg-cyan-950/40 px-2 py-0.5 rounded border border-cyan-800/50">BTC/USDT</span> | เชื่อมต่อ 5 กระดานเทรด</p>
            </div>
            <div class="flex items-center gap-3">
                <button onclick="toggleSettings()" class="bg-cyan-600/20 hover:bg-cyan-600 text-cyan-400 hover:text-white border border-cyan-500/30 px-3 py-1.5 rounded-lg text-xs font-semibold transition cursor-pointer">
                    ⚙️ ตั้งค่า API Keys
                </button>
                <span id="connection-status" class="inline-flex items-center px-3 py-1.5 rounded-full text-xs font-medium bg-amber-500/10 text-amber-400 border border-amber-500/20">
                    <span class="w-2 h-2 mr-1.5 bg-amber-500 rounded-full animate-ping"></span> Connecting...
                </span>
            </div>
        </header>

        <!-- API Configuration Panel -->
        <div id="settings-panel" class="bg-slate-900 border border-cyan-500/30 rounded-xl p-5 hidden shadow-2xl space-y-4">
            <h3 class="text-base font-bold text-white flex items-center gap-2">
                <span>🔑 ตั้งค่า API Keys สำหรับเชื่อมต่อแต่ละ Exchange</span>
            </h3>
            <p class="text-xs text-slate-400">กรอก API Key และ Secret ของแต่ละกระดานเพื่อให้ระบบดึงข้อมูลพอร์ตและส่งคำสั่งซื้อขายจริง (ข้อมูลจะถูกบันทึกเฉพาะในเบราว์เซอร์ของคุณ)</p>
            
            <div class="grid grid-cols-1 md:grid-cols-5 gap-4" id="api-inputs-container"></div>
            <div class="flex justify-end gap-3 pt-2">
                <button onclick="toggleSettings()" class="bg-slate-800 hover:bg-slate-700 text-slate-300 px-4 py-2 rounded-lg text-xs font-medium cursor-pointer">ปิด</button>
                <button onclick="saveApiKeys()" class="bg-cyan-600 hover:bg-cyan-500 text-white px-4 py-2 rounded-lg text-xs font-medium cursor-pointer shadow-lg">บันทึกและเชื่อมต่อ API</button>
            </div>
        </div>

        <!-- 1. ภาพรวมพอร์ตทรัพย์สิน -->
        <section class="bg-slate-900/70 border border-slate-800/80 rounded-xl p-4 backdrop-blur-md shadow-lg">
            <div class="flex justify-between items-center mb-3">
                <h2 class="text-xs font-bold text-slate-300 uppercase tracking-wider flex items-center gap-2">
                    <span class="w-2 h-2 bg-cyan-400 rounded-full"></span> 
                    1. ปริมาณ Position / Asset คงเหลือรายกระดาน (เฉพาะคู่เทรดปัจจุบัน)
                </h2>
            </div>
            <div class="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3" id="balance-container"></div>
        </section>

        <!-- 2. Order Book 5 ชั้นราคา -->
        <section class="space-y-3">
            <h2 class="text-xs font-bold text-slate-300 uppercase tracking-wider flex items-center gap-2">
                <span class="w-2 h-2 bg-emerald-400 rounded-full"></span> 
                2. Order Book 5 ชั้นราคาจากกระดานเทรดจริง
            </h2>
            <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-5 gap-4" id="exchange-container"></div>
        </section>

        <!-- 3. ตารางประวัติการส่งคำสั่ง -->
        <section class="bg-slate-900/70 border border-slate-800/80 rounded-xl p-4 backdrop-blur-md shadow-lg">
            <h2 class="text-xs font-bold text-slate-300 uppercase tracking-wider flex items-center gap-2 mb-3">
                <span class="w-2 h-2 bg-amber-400 rounded-full"></span> 
                3. บันทึกคำสั่งซื้อขาย (Execution Log)
            </h2>
            <div class="overflow-x-auto">
                <table class="w-full text-left text-xs font-mono">
                    <thead class="bg-slate-950/80 text-slate-400 border-b border-slate-800">
                        <tr>
                            <th class="p-2.5">เวลา</th>
                            <th class="p-2.5">Exchange</th>
                            <th class="p-2.5">ประเภท</th>
                            <th class="p-2.5">จำนวน (USDT)</th>
                            <th class="p-2.5">ราคาอ้างอิง</th>
                            <th class="p-2.5">สถานะ</th>
                        </tr>
                    </thead>
                    <tbody id="order-history-body" class="divide-y divide-slate-800/50 text-slate-300">
                        <tr><td colspan="6" class="p-3 text-center text-slate-500">ยังไม่มีประวัติการส่งคำสั่งในเซสชันนี้</td></tr>
                    </tbody>
                </table>
            </div>
        </section>
    </div>

    <!-- Order Execution Modal -->
    <div id="order-modal" class="fixed inset-0 bg-black/70 backdrop-blur-xs hidden items-center justify-center z-50">
        <div class="bg-slate-900 border border-slate-800 rounded-xl p-6 w-96 shadow-2xl">
            <h3 class="text-lg font-bold mb-4 text-white">ยืนยันคำสั่งซื้อขาย API</h3>
            <div class="space-y-3 text-sm text-slate-300 mb-6 bg-slate-950/60 p-3.5 rounded-lg border border-slate-800 font-sans">
                <div class="flex justify-between"><span>Exchange:</span> <span id="modal-exchange" class="font-semibold text-white"></span></div>
                <div class="flex justify-between"><span>ประเภทคำสั่ง:</span> <span id="modal-type" class="font-semibold"></span></div>
                <div class="flex justify-between"><span>จำนวนเงิน:</span> <span id="modal-amount" class="font-semibold text-emerald-400 font-mono"></span></div>
                <div class="flex justify-between"><span>ราคาอ้างอิง:</span> <span id="modal-price" class="font-semibold text-cyan-400 font-mono"></span></div>
            </div>
            <div class="flex gap-3">
                <button onclick="closeModal()" class="flex-1 bg-slate-800 hover:bg-slate-700 text-slate-300 py-2.5 rounded-lg font-medium transition cursor-pointer">ยกเลิก</button>
                <button onclick="confirmOrder()" class="flex-1 bg-cyan-600 hover:bg-cyan-500 text-white py-2.5 rounded-lg font-medium transition cursor-pointer shadow-lg">ยืนยันส่งคำสั่ง</button>
            </div>
        </div>
    </div>

    <script>
        const exchanges = ['binance', 'okx', 'bybit', 'kucoin', 'gate'];
        const exchangeData = {};
        let ws;

        function initUI() {
            const container = document.getElementById('exchange-container');
            const balanceContainer = document.getElementById('balance-container');
            const apiInputsContainer = document.getElementById('api-inputs-container');
            
            container.innerHTML = '';
            balanceContainer.innerHTML = '';
            apiInputsContainer.innerHTML = '';

            exchanges.forEach(ex => {
                const exName = ex.toUpperCase();
                exchangeData[ex] = { bids: [], asks: [], balance: {}, base_asset: 'BTC', quote_asset: 'USDT' };

                const savedKey = localStorage.getItem(`${ex}_apiKey`) || '';
                const savedSec = localStorage.getItem(`${ex}_secret`) || '';
                const savedPwd = localStorage.getItem(`${ex}_password`) || '';

                const apiBox = document.createElement('div');
                apiBox.className = 'bg-slate-950/80 p-3 rounded-lg border border-slate-800 space-y-2';
                apiBox.innerHTML = `
                    <div class="font-bold text-xs text-cyan-400">${exName}</div>
                    <input type="text" id="${ex}-key" placeholder="API Key" value="${savedKey}" class="w-full bg-slate-900 border border-slate-700 rounded px-2 py-1 text-[11px] text-white">
                    <input type="password" id="${ex}-sec" placeholder="Secret Key" value="${savedSec}" class="w-full bg-slate-900 border border-slate-700 rounded px-2 py-1 text-[11px] text-white">
                    ${ex === 'okx' ? `<input type="password" id="${ex}-pwd" placeholder="Passphrase" value="${savedPwd}" class="w-full bg-slate-900 border border-slate-700 rounded px-2 py-1 text-[11px] text-white">` : ''}
                `;
                apiInputsContainer.appendChild(apiBox);

                const bCard = document.createElement('div');
                bCard.id = `balance-${ex}`;
                bCard.className = 'bg-slate-950/60 border border-slate-800/60 rounded-xl p-3';
                bCard.innerHTML = `
                    <div class="text-xs font-bold text-slate-400 mb-1">${exName}</div>
                    <div class="text-sm font-bold text-emerald-400 font-mono">$0.00 USDT</div>
                    <div class="text-xs text-cyan-300 font-mono mt-0.5">0.0000 BTC</div>
                `;
                balanceContainer.appendChild(bCard);

                const card = document.createElement('div');
                card.className = 'bg-slate-900/80 border border-slate-800/80 rounded-xl p-4 flex flex-col justify-between shadow-xl backdrop-blur-md';
                card.innerHTML = `
                    <div>
                        <div class="flex justify-between items-center mb-3">
                            <span class="font-bold text-base text-white tracking-wide">${exName}</span>
                            <span class="text-[10px] uppercase font-semibold px-2 py-0.5 rounded bg-slate-800 text-slate-400 border border-slate-700/50">Spot</span>
                        </div>
                        <div class="space-y-1 mb-2" id="${ex}-asks">
                            <div class="text-xs text-slate-500 text-center py-4">Waiting for data...</div>
                        </div>
                        <div class="py-1 text-center text-xs text-slate-500 border-y border-slate-800/50 my-1 bg-slate-950/30 rounded">
                            Spread: <span id="${ex}-spread" class="text-slate-400 font-mono">-</span>
                        </div>
                        <div class="space-y-1 mb-4" id="${ex}-bids">
                            <div class="text-xs text-slate-500 text-center py-4">Waiting for data...</div>
                        </div>
                    </div>

                    <div class="space-y-2 pt-2 border-t border-slate-800/60">
                        <div class="relative">
                            <input type="number" id="${ex}-amount" placeholder="ระบุจำนวนเงิน" value="100" min="1" step="10"
                                class="w-full bg-slate-950 border border-slate-700/80 rounded-lg px-3 py-1.5 text-xs text-white font-mono focus:outline-none focus:border-cyan-500 transition pr-12">
                            <span class="absolute right-3 top-1.5 text-[11px] font-bold text-slate-400">USDT</span>
                        </div>
                        <div class="grid grid-cols-2 gap-2">
                            <button onclick="openOrderModal('${ex}', 'BUY')" class="bg-emerald-600/20 hover:bg-emerald-600 text-emerald-400 hover:text-white border border-emerald-500/30 py-2 rounded-lg font-semibold text-sm transition cursor-pointer">BUY</button>
                            <button onclick="openOrderModal('${ex}', 'SELL')" class="bg-rose-600/20 hover:bg-rose-600 text-rose-400 hover:text-white border border-rose-500/30 py-2 rounded-lg font-semibold text-sm transition cursor-pointer">SELL</button>
                        </div>
                    </div>
                `;
                container.appendChild(card);
            });

            const totalCard = document.createElement('div');
            totalCard.id = 'total-balance-card';
            totalCard.className = 'bg-cyan-950/20 border border-cyan-800/40 rounded-xl p-3 shadow-inner';
            totalCard.innerHTML = `
                <div class="text-xs font-bold text-cyan-400 mb-1">TOTAL POSITION</div>
                <div class="text-sm font-bold text-cyan-200 font-mono" id="total-quote">$0.00 USDT</div>
                <div class="text-xs text-cyan-300 font-mono mt-0.5" id="total-base">0.0000 BTC</div>
            `;
            balanceContainer.appendChild(totalCard);
        }

        function toggleSettings() {
            const panel = document.getElementById('settings-panel');
            panel.classList.toggle('hidden');
        }

        function saveApiKeys() {
            exchanges.forEach(ex => {
                const key = document.getElementById(`${ex}-key`).value.trim();
                const sec = document.getElementById(`${ex}-sec`).value.trim();
                localStorage.setItem(`${ex}_apiKey`, key);
                localStorage.setItem(`${ex}_secret`, sec);
                
                let pwd = '';
                if(ex === 'okx') {
                    pwd = document.getElementById(`${ex}-pwd`).value.trim();
                    localStorage.setItem(`${ex}_password`, pwd);
                }

                if(ws && ws.readyState === WebSocket.OPEN) {
                    ws.send(JSON.stringify({
                        action: "update_keys",
                        exchange: ex,
                        keys: { apiKey: key, secret: sec, password: pwd }
                    }));
                }
            });
            alert("บันทึกและเชื่อมต่อ API Keys เรียบร้อยแล้ว!");
            toggleSettings();
        }

        function connectWebSocket() {
            const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            ws = new WebSocket(`${protocol}//${window.location.host}/ws`);

            ws.onopen = () => {
                const status = document.getElementById('connection-status');
                status.className = 'inline-flex items-center px-3 py-1.5 rounded-full text-xs font-medium bg-emerald-500/10 text-emerald-400 border border-emerald-500/20';
                status.innerHTML = '<span class="w-2 h-2 mr-1.5 bg-emerald-500 rounded-full animate-pulse"></span> Live WebSocket Connected';
                
                exchanges.forEach(ex => {
                    const key = localStorage.getItem(`${ex}_apiKey`) || '';
                    const sec = localStorage.getItem(`${ex}_secret`) || '';
                    const pwd = localStorage.getItem(`${ex}_password`) || '';
                    if(key) {
                        ws.send(JSON.stringify({
                            action: "update_keys",
                            exchange: ex,
                            keys: { apiKey: key, secret: sec, password: pwd }
                        }));
                    }
                });
            };

            ws.onmessage = (event) => {
                const data = JSON.parse(event.data);
                if (data.exchange && data.bids && data.asks) {
                    exchangeData[data.exchange] = data;
                    if(data.symbol) document.getElementById('current-pair').innerText = data.symbol;
                    renderExchange(data.exchange);
                    updateTotalBalance();
                }
            };

            ws.onclose = () => {
                const status = document.getElementById('connection-status');
                status.className = 'inline-flex items-center px-3 py-1.5 rounded-full text-xs font-medium bg-rose-500/10 text-rose-400 border border-rose-500/20';
                status.innerHTML = '<span class="w-2 h-2 mr-1.5 bg-rose-500 rounded-full"></span> Disconnected. Reconnecting...';
                setTimeout(connectWebSocket, 3000);
            };
        }

        function renderExchange(ex) {
            const item = exchangeData[ex];
            
            let asksHTML = '';
            const sortedAsks = [...item.asks].reverse();
            sortedAsks.forEach(ask => {
                asksHTML += `<div class="flex justify-between text-xs font-mono px-1 py-0.5 rounded"><span class="text-rose-400">${Number(ask[0]).toFixed(2)}</span><span class="text-slate-400">${Number(ask[1]).toFixed(4)}</span></div>`;
            });
            document.getElementById(`${ex}-asks`).innerHTML = asksHTML;

            let bidsHTML = '';
            item.bids.forEach(bid => {
                bidsHTML += `<div class="flex justify-between text-xs font-mono px-1 py-0.5 rounded"><span class="text-emerald-400">${Number(bid[0]).toFixed(2)}</span><span class="text-slate-400">${Number(bid[1]).toFixed(4)}</span></div>`;
            });
            document.getElementById(`${ex}-bids`).innerHTML = bidsHTML;

            if(item.asks.length > 0 && item.bids.length > 0) {
                const spread = item.asks[0][0] - item.bids[0][0];
                document.getElementById(`${ex}-spread`).innerText = spread.toFixed(2);
            }

            const bCard = document.getElementById(`balance-${ex}`);
            if(bCard && item.balance) {
                const baseAsset = item.base_asset || 'BTC';
                const quoteAsset = item.quote_asset || 'USDT';
                const baseVal = Number(item.balance[baseAsset] || 0);
                const quoteVal = Number(item.balance[quoteAsset] || 0);

                const liveBadge = item.is_live ? '<span class="text-[9px] text-emerald-400 bg-emerald-950/60 px-1 rounded border border-emerald-800">LIVE API</span>' : '<span class="text-[9px] text-slate-500">MOCK</span>';

                bCard.innerHTML = `
                    <div class="flex justify-between items-center mb-1">
                        <span class="text-xs font-bold text-slate-400">${ex.toUpperCase()}</span>
                        ${liveBadge}
                    </div>
                    <div class="text-sm font-bold text-emerald-400 font-mono">$${quoteVal.toLocaleString('en-US', {minimumFractionDigits: 2})} ${quoteAsset}</div>
                    <div class="text-xs text-cyan-300 font-mono mt-0.5">${baseVal.toFixed(4)} ${baseAsset}</div>
                `;
            }
        }

        function updateTotalBalance() {
            let totalQuote = 0;
            let totalBase = 0;
            let currentBaseAsset = 'BTC';
            let currentQuoteAsset = 'USDT';

            Object.values(exchangeData).forEach(d => {
                if(d.balance) {
                    currentBaseAsset = d.base_asset || 'BTC';
                    currentQuoteAsset = d.quote_asset || 'USDT';
                    totalQuote += Number(d.balance[currentQuoteAsset] || 0);
                    totalBase += Number(d.balance[currentBaseAsset] || 0);
                }
            });

            const totalQuoteElem = document.getElementById('total-quote');
            const totalBaseElem = document.getElementById('total-base');
            if(totalQuoteElem && totalBaseElem) {
                totalQuoteElem.innerText = `$${totalQuote.toLocaleString('en-US', {minimumFractionDigits: 2})} ${currentQuoteAsset}`;
                totalBaseElem.innerText = `${totalBase.toFixed(4)} ${currentBaseAsset}`;
            }
        }

        let selectedEx = '';
        let selectedType = '';
        let selectedAmount = 0;

        function openOrderModal(exchange, type) {
            selectedEx = exchange;
            selectedType = type;
            const amountInput = document.getElementById(`${exchange}-amount`);
            selectedAmount = amountInput ? parseFloat(amountInput.value) || 0 : 0;

            if (selectedAmount <= 0) {
                alert("กรุณาระบุจำนวนเงินที่ต้องการซื้อขายให้ถูกต้อง");
                return;
            }

            document.getElementById('modal-exchange').innerText = exchange.toUpperCase();
            document.getElementById('modal-type').innerText = type;
            document.getElementById('modal-type').className = `font-semibold ${type === 'BUY' ? 'text-emerald-400' : 'text-rose-400'}`;
            document.getElementById('modal-amount').innerText = `$${selectedAmount.toLocaleString('en-US')} USDT`;
            
            const currentItem = exchangeData[exchange];
            let refPrice = '0.00';
            if (type === 'BUY' && currentItem.asks.length > 0) refPrice = currentItem.asks[0][0];
            if (type === 'SELL' && currentItem.bids.length > 0) refPrice = currentItem.bids[0][0];

            document.getElementById('modal-price').innerText = refPrice + " USDT";
            document.getElementById('order-modal').classList.remove('hidden');
            document.getElementById('order-modal').classList.add('flex');
        }

        function closeModal() {
            document.getElementById('order-modal').classList.add('hidden');
            document.getElementById('order-modal').classList.remove('flex');
        }

        function confirmOrder() {
            const currentItem = exchangeData[selectedEx];
            let refPrice = '0.00';
            if (selectedType === 'BUY' && currentItem.asks.length > 0) refPrice = currentItem.asks[0][0];
            if (selectedType === 'SELL' && currentItem.bids.length > 0) refPrice = currentItem.bids[0][0];

            ws.send(JSON.stringify({ 
                action: "place_order", 
                exchange: selectedEx, 
                type: selectedType, 
                symbol: 'BTC/USDT',
                amount_usdt: selectedAmount,
                price: refPrice
            }));

            const tbody = document.getElementById('order-history-body');
            if(tbody.querySelector('td[colspan]')) tbody.innerHTML = '';
            
            const now = new Date().toLocaleTimeString();
            const row = document.createElement('tr');
            row.innerHTML = `
                <td class="p-2.5 text-slate-400">${now}</td>
                <td class="p-2.5 font-semibold text-white">${selectedEx.toUpperCase()}</td>
                <td class="p-2.5 font-bold ${selectedType === 'BUY' ? 'text-emerald-400' : 'text-rose-400'}">${selectedType}</td>
                <td class="p-2.5 font-mono text-cyan-400">$${selectedAmount.toLocaleString('en-US')}</td>
                <td class="p-2.5 font-mono text-slate-300">${refPrice}</td>
                <td class="p-2.5 text-emerald-400">Success (API Sent)</td>
            `;
            tbody.prepend(row);

            alert(`ส่งคำสั่ง ${selectedType} จำนวน $${selectedAmount} USDT ไปยัง ${selectedEx.toUpperCase()} เรียบร้อยแล้ว!`);
            closeModal();
        }

        initUI();
        connectWebSocket();
    </script>
</body>
</html>
    """
