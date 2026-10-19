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

running_tasks = {}
exchange_keys_store = {}
current_symbol_store = "BTC/USDT"

async def watch_exchange_data(exchange_id, symbol, api_keys=None):
    exchange_class = getattr(ccxtpro, exchange_id)
    config = {'enableRateLimit': True}
    
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

    while True:
        try:
            orderbook = {'bids': [], 'asks': []}
            try:
                orderbook_data = await exchange.watch_order_book(symbol, limit=5)
                orderbook['bids'] = orderbook_data.get('bids', [])[:5]
                orderbook['asks'] = orderbook_data.get('asks', [])[:5]
            except Exception as ex_ob:
                print(f"[{exchange_id}] Orderbook fetch error: {ex_ob}")

            pair_balances = None
            if has_credentials:
                try:
                    bal = await exchange.fetch_balance()
                    pair_balances = {
                        quote_asset: float(bal['total'].get(quote_asset, 0.0)),
                        base_asset: float(bal['total'].get(base_asset, 0.0))
                    }
                except Exception as ex:
                    print(f"[{exchange_id}] Balance fetch error: {ex}")
                    pair_balances = {quote_asset: 0.0, base_asset: 0.0}

            payload = {
                "type": "market_data",
                "exchange": exchange_id,
                "symbol": symbol,
                "base_asset": base_asset,
                "quote_asset": quote_asset,
                "bids": orderbook['bids'],
                "asks": orderbook['asks'],
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
            print(f"Error on {exchange_id} ({symbol}): {e}")
            await asyncio.sleep(5)
            try:
                await exchange.close()
            except:
                pass
            exchange = exchange_class(config)

def start_or_restart_exchange(exchange_id, symbol="BTC/USDT", keys=None):
    if exchange_id in running_tasks and not running_tasks[exchange_id].done():
        running_tasks[exchange_id].cancel()
    
    if keys is not None:
        exchange_keys_store[exchange_id] = keys
    else:
        keys = exchange_keys_store.get(exchange_id)

    task = asyncio.create_task(watch_exchange_data(exchange_id, symbol, keys))
    running_tasks[exchange_id] = task

def change_all_exchanges_symbol(symbol):
    global current_symbol_store
    current_symbol_store = symbol
    exchanges = ['binance', 'okx', 'bybit', 'kucoin', 'gate']
    for ex in exchanges:
        start_or_restart_exchange(ex, symbol)

async def execute_live_order(exchange_id, side_type, order_category, symbol, amount_usdt, price):
    """
    side_type: 'BUY' หรือ 'SELL'
    order_category: 'market' หรือ 'limit'
    """
    keys = exchange_keys_store.get(exchange_id) or {}
    env_key = os.getenv(f"{exchange_id.upper()}_API_KEY", '')
    env_sec = os.getenv(f"{exchange_id.upper()}_SECRET", '')
    env_pwd = os.getenv(f"{exchange_id.upper()}_PASSWORD", '')

    apiKey = keys.get('apiKey') or env_key
    secret = keys.get('secret') or env_sec
    password = keys.get('password') or env_pwd

    if not (apiKey and secret):
        return {
            "status": "error",
            "message": f"ไม่พบ API Key ของ {exchange_id.upper()} กรุณาตั้งค่าก่อนส่งคำสั่ง"
        }

    try:
        exchange_class = getattr(ccxtpro, exchange_id)
        config = {
            'apiKey': apiKey,
            'secret': secret,
            'enableRateLimit': True
        }
        if password:
            config['password'] = password

        exchange = exchange_class(config)
        
        target_price = float(price) if price and float(price) > 0 else 1.0
        amount_coin = float(amount_usdt) / target_price
        side = side_type.lower() # 'buy' หรือ 'sell'
        ord_type = order_category.lower() # 'market' หรือ 'limit'

        # ส่งคำสั่งซื้อขายไปยัง Exchange จริง
        if ord_type == 'limit':
            order_res = await exchange.create_order(
                symbol=symbol,
                type='limit',
                side=side,
                amount=amount_coin,
                price=target_price
            )
        else:
            order_res = await exchange.create_order(
                symbol=symbol,
                type='market',
                side=side,
                amount=amount_coin
            )

        await exchange.close()

        return {
            "status": "success",
            "order_id": order_res.get('id', 'N/A'),
            "message": f"ส่งคำสั่ง {side_type} ({ord_type.upper()}) คู่ {symbol} จำนวน ${amount_usdt} USDT สำเร็จ!",
            "details": order_res
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"การส่งคำสั่งล้มเหลว ({exchange_id.upper()}): {str(e)}"
        }

@app.on_event("startup")
async def startup_event():
    change_all_exchanges_symbol("BTC/USDT")

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
                start_or_restart_exchange(ex_id, current_symbol_store, keys)
                await websocket.send_text(json.dumps({
                    "type": "system_msg",
                    "status": "success", 
                    "message": f"Updated API for {ex_id}"
                }))
            
            elif req.get("action") == "change_symbol":
                new_symbol = req.get("symbol", "BTC/USDT")
                change_all_exchanges_symbol(new_symbol)
                await websocket.send_text(json.dumps({
                    "type": "system_msg",
                    "status": "success", 
                    "message": f"Switched symbol to {new_symbol}"
                }))

            elif req.get("action") == "place_order":
                ex_id = req["exchange"]
                side_type = req["type"] # BUY / SELL
                order_category = req.get("order_category", "market") # market / limit
                symbol = req["symbol"]
                amount_usdt = req["amount_usdt"]
                price = req["price"]

                result = await execute_live_order(ex_id, side_type, order_category, symbol, amount_usdt, price)
                
                await websocket.send_text(json.dumps({
                    "type": "order_result",
                    "exchange": ex_id,
                    "side_type": side_type,
                    "order_category": order_category,
                    "symbol": symbol,
                    "amount_usdt": amount_usdt,
                    "price": price,
                    "result": result
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

    <!-- Toast Notification Overlay -->
    <div id="toast-container" class="fixed top-5 right-5 z-50 space-y-2 w-80"></div>

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
                <div class="flex items-center gap-2 mt-2">
                    <span class="text-xs text-slate-400">เลือกคู่เทรด:</span>
                    <select id="symbol-select" onchange="onSymbolChange()" class="bg-slate-900 border border-cyan-500/50 text-cyan-300 font-bold text-xs rounded-lg px-3 py-1.5 focus:outline-none focus:border-cyan-400 cursor-pointer">
                        <option value="BTC/USDT" selected>BTC / USDT</option>
                        <option value="ETH/USDT">ETH / USDT</option>
                        <option value="VELO/USDT">VELO / USDT</option>
                    </select>
                </div>
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
                    1. สรุปมูลค่าทรัพย์สินแยกรายกระดานและพอร์ตรวม (<span id="balance-pair-title" class="text-cyan-400 font-bold">BTC/USDT</span>)
                </h2>
            </div>
            <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-6 gap-3" id="balance-container"></div>
        </section>

        <!-- 2. Order Book 5 ชั้นราคา พร้อมเมนูสั่งซื้อขาย (Market / Limit) -->
        <section class="space-y-3">
            <h2 class="text-xs font-bold text-slate-300 uppercase tracking-wider flex items-center gap-2">
                <span class="w-2 h-2 bg-emerald-400 rounded-full"></span> 
                2. Order Book 5 ชั้นราคาและพาเนลส่งคำสั่ง
            </h2>
            <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-5 gap-4" id="exchange-container"></div>
        </section>

        <!-- 3. ตารางประวัติการส่งคำสั่ง -->
        <section class="bg-slate-900/70 border border-slate-800/80 rounded-xl p-4 backdrop-blur-md shadow-lg">
            <h2 class="text-xs font-bold text-slate-300 uppercase tracking-wider flex items-center gap-2 mb-3">
                <span class="w-2 h-2 bg-amber-400 rounded-full"></span> 
                3. บันทึกผลลัพธ์การส่งคำสั่งซื้อขายจริง (Execution Log & Status Feedback)
            </h2>
            <div class="overflow-x-auto">
                <table class="w-full text-left text-xs font-mono">
                    <thead class="bg-slate-950/80 text-slate-400 border-b border-slate-800">
                        <tr>
                            <th class="p-2.5">เวลา</th>
                            <th class="p-2.5">Exchange</th>
                            <th class="p-2.5">คู่เทรด</th>
                            <th class="p-2.5">ประเภท</th>
                            <th class="p-2.5">โหมด</th>
                            <th class="p-2.5">จำนวน (USDT)</th>
                            <th class="p-2.5">ราคา</th>
                            <th class="p-2.5">สถานะจาก Exchange</th>
                        </tr>
                    </thead>
                    <tbody id="order-history-body" class="divide-y divide-slate-800/50 text-slate-300">
                        <tr id="empty-history-row"><td colspan="8" class="p-3 text-center text-slate-500">ยังไม่มีประวัติการส่งคำสั่งในเซสชันนี้</td></tr>
                    </tbody>
                </table>
            </div>
        </section>
    </div>

    <!-- Order Execution Modal -->
    <div id="order-modal" class="fixed inset-0 bg-black/70 backdrop-blur-xs hidden items-center justify-center z-50">
        <div class="bg-slate-900 border border-slate-800 rounded-xl p-6 w-96 shadow-2xl">
            <h3 class="text-lg font-bold mb-4 text-white">ยืนยันคำสั่งซื้อขาย API จริง</h3>
            <div class="space-y-3 text-sm text-slate-300 mb-6 bg-slate-950/60 p-3.5 rounded-lg border border-slate-800 font-sans">
                <div class="flex justify-between"><span>Exchange:</span> <span id="modal-exchange" class="font-semibold text-white"></span></div>
                <div class="flex justify-between"><span>คู่เทรด:</span> <span id="modal-symbol" class="font-semibold text-cyan-400"></span></div>
                <div class="flex justify-between"><span>ประเภทคำสั่ง:</span> <span id="modal-type" class="font-semibold"></span></div>
                <div class="flex justify-between"><span>โหมดคำสั่ง:</span> <span id="modal-category" class="font-bold text-amber-400"></span></div>
                <div class="flex justify-between"><span>จำนวนเงิน:</span> <span id="modal-amount" class="font-semibold text-emerald-400 font-mono"></span></div>
                <div class="flex justify-between"><span>ราคาเสนอเทรด:</span> <span id="modal-price" class="font-semibold text-cyan-400 font-mono"></span></div>
            </div>
            <div class="flex gap-3">
                <button onclick="closeModal()" class="flex-1 bg-slate-800 hover:bg-slate-700 text-slate-300 py-2.5 rounded-lg font-medium transition cursor-pointer">ยกเลิก</button>
                <button id="modal-confirm-btn" onclick="confirmOrder()" class="flex-1 bg-cyan-600 hover:bg-cyan-500 text-white py-2.5 rounded-lg font-medium transition cursor-pointer shadow-lg">ยืนยันส่งคำสั่ง</button>
            </div>
        </div>
    </div>

    <script>
        const exchanges = ['binance', 'okx', 'bybit', 'kucoin', 'gate'];
        const exchangeData = {};
        let ws;
        let currentSelectedSymbol = "BTC/USDT";

        function showToast(message, type = 'success') {
            const container = document.getElementById('toast-container');
            const toast = document.createElement('div');
            const isSuccess = type === 'success';
            
            toast.className = `p-3 rounded-lg border shadow-xl text-xs font-semibold flex items-center justify-between transition-all duration-300 transform translate-x-full ${
                isSuccess 
                ? 'bg-emerald-950/90 border-emerald-500/50 text-emerald-200' 
                : 'bg-rose-950/90 border-rose-500/50 text-rose-200'
            }`;
            
            toast.innerHTML = `
                <div class="flex items-center gap-2">
                    <span>${isSuccess ? '✅' : '❌'}</span>
                    <span>${message}</span>
                </div>
                <button onclick="this.parentElement.remove()" class="text-slate-400 hover:text-white ml-2">✕</button>
            `;
            
            container.appendChild(toast);
            setTimeout(() => toast.classList.remove('translate-x-full'), 10);
            setTimeout(() => {
                toast.classList.add('translate-x-full');
                setTimeout(() => toast.remove(), 300);
            }, 5000);
        }

        function toggleOrderType(ex) {
            const typeSelect = document.getElementById(`${ex}-ord-type`);
            const priceContainer = document.getElementById(`${ex}-price-container`);
            if (typeSelect && priceContainer) {
                if (typeSelect.value === 'limit') {
                    priceContainer.classList.remove('hidden');
                } else {
                    priceContainer.classList.add('hidden');
                }
            }
        }

        function initUI() {
            const container = document.getElementById('exchange-container');
            const balanceContainer = document.getElementById('balance-container');
            const apiInputsContainer = document.getElementById('api-inputs-container');
            
            container.innerHTML = '';
            balanceContainer.innerHTML = '';
            apiInputsContainer.innerHTML = '';

            exchanges.forEach(ex => {
                const exName = ex.toUpperCase();
                exchangeData[ex] = { bids: [], asks: [], balance: null, base_asset: 'BTC', quote_asset: 'USDT', is_live: false };

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
                bCard.className = 'bg-slate-950/60 border border-slate-800/60 rounded-xl p-3 flex flex-col justify-between';
                bCard.innerHTML = `
                    <div class="flex justify-between items-center mb-1">
                        <span class="text-xs font-bold text-slate-400">${exName}</span>
                        <span class="text-[9px] text-slate-500 font-semibold bg-slate-900 px-1 py-0.5 rounded border border-slate-800">NOT CONNECTED</span>
                    </div>
                    <div class="text-xs text-slate-500 py-3">กรุณาตั้งค่า API Key เพื่อแสดงยอดพอร์ต</div>
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
                            <div class="text-xs text-slate-500 text-center py-4">Waiting for connection...</div>
                        </div>
                        <div class="py-1 text-center text-xs text-slate-500 border-y border-slate-800/50 my-1 bg-slate-950/30 rounded">
                            Spread: <span id="${ex}-spread" class="text-slate-400 font-mono">-</span>
                        </div>
                        <div class="space-y-1 mb-4" id="${ex}-bids">
                            <div class="text-xs text-slate-500 text-center py-4">Waiting for connection...</div>
                        </div>
                    </div>

                    <!-- Action Control Panel -->
                    <div class="space-y-2 pt-2 border-t border-slate-800/60">
                        <div class="flex gap-2">
                            <select id="${ex}-ord-type" onchange="toggleOrderType('${ex}')" class="bg-slate-950 border border-slate-700 rounded-lg px-2 py-1 text-xs text-amber-400 font-semibold w-full focus:outline-none focus:border-cyan-500 cursor-pointer">
                                <option value="market" selected>Market Order</option>
                                <option value="limit">Limit Order</option>
                            </select>
                        </div>
                        
                        <div class="relative">
                            <input type="number" id="${ex}-amount" placeholder="จำนวนเงิน" value="100" min="1" step="10"
                                class="w-full bg-slate-950 border border-slate-700/80 rounded-lg px-3 py-1.5 text-xs text-white font-mono focus:outline-none focus:border-cyan-500 transition pr-12">
                            <span class="absolute right-3 top-1.5 text-[11px] font-bold text-slate-400">USDT</span>
                        </div>

                        <div id="${ex}-price-container" class="relative hidden">
                            <input type="number" id="${ex}-limit-price" placeholder="ระบุราคา Limit" step="0.0001"
                                class="w-full bg-slate-950 border border-amber-500/50 rounded-lg px-3 py-1.5 text-xs text-amber-300 font-mono focus:outline-none focus:border-amber-400 transition pr-12">
                            <span class="absolute right-3 top-1.5 text-[11px] font-bold text-slate-400">Price</span>
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
            totalCard.className = 'bg-cyan-950/30 border border-cyan-700/50 rounded-xl p-3 flex flex-col justify-between shadow-inner';
            totalCard.innerHTML = `
                <div>
                    <div class="text-xs font-bold text-cyan-400 mb-1 flex justify-between items-center">
                        <span>TOTAL PORTFOLIO</span>
                        <span class="text-[9px] bg-cyan-900/60 text-cyan-300 px-1.5 py-0.5 rounded border border-cyan-700/60">ALL EXCHANGES</span>
                    </div>
                    <div class="text-xs text-slate-400 mt-1">CASH: <span id="total-quote" class="text-emerald-400 font-bold font-mono">$0.00 USDT</span></div>
                    <div class="text-xs text-slate-400 mt-0.5">ASSET: <span id="total-base" class="text-cyan-300 font-bold font-mono">0.0000 BTC</span></div>
                </div>
                <div class="pt-2 mt-2 border-t border-cyan-800/60 text-right">
                    <span class="text-[10px] text-slate-400 block uppercase tracking-wider">Net Portfolio Value</span>
                    <span id="total-net-val" class="text-sm font-extrabold text-cyan-200 font-mono">$0.00 USDT</span>
                </div>
            `;
            balanceContainer.appendChild(totalCard);
        }

        function onSymbolChange() {
            const selectElem = document.getElementById('symbol-select');
            currentSelectedSymbol = selectElem.value;
            document.getElementById('balance-pair-title').innerText = currentSelectedSymbol;

            if (ws && ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({
                    action: "change_symbol",
                    symbol: currentSelectedSymbol
                }));
            }
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
            showToast("บันทึกและส่ง API Keys ไปยังเซิร์ฟเวอร์แล้ว", "success");
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
                if (data.type === "order_result") {
                    handleOrderFeedback(data);
                } else if (data.exchange) {
                    exchangeData[data.exchange] = data;
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

        function handleOrderFeedback(data) {
            const res = data.result;
            const isSuccess = res.status === "success";
            
            showToast(res.message, isSuccess ? "success" : "error");

            const emptyRow = document.getElementById('empty-history-row');
            if (emptyRow) emptyRow.remove();

            const tbody = document.getElementById('order-history-body');
            const now = new Date().toLocaleTimeString();
            const pDecimals = data.symbol.includes('VELO') ? 4 : 2;
            
            const row = document.createElement('tr');
            row.className = isSuccess ? "bg-emerald-950/20" : "bg-rose-950/20";
            
            row.innerHTML = `
                <td class="p-2.5 text-slate-400">${now}</td>
                <td class="p-2.5 font-semibold text-white">${data.exchange.toUpperCase()}</td>
                <td class="p-2.5 font-bold text-cyan-400">${data.symbol}</td>
                <td class="p-2.5 font-bold ${data.side_type === 'BUY' ? 'text-emerald-400' : 'text-rose-400'}">${data.side_type}</td>
                <td class="p-2.5 font-bold uppercase text-amber-400">${data.order_category}</td>
                <td class="p-2.5 font-mono text-cyan-400">$${Number(data.amount_usdt).toLocaleString('en-US')}</td>
                <td class="p-2.5 font-mono text-slate-300">${Number(data.price).toFixed(pDecimals)}</td>
                <td class="p-2.5 font-semibold ${isSuccess ? 'text-emerald-400' : 'text-rose-400'}">
                    ${isSuccess ? `✅ EXECUTED (${res.order_id || 'SUCCESS'})` : `❌ FAILED: ${res.message}`}
                </td>
            `;
            tbody.prepend(row);
        }

        function renderExchange(ex) {
            const item = exchangeData[ex];
            
            if (item.asks && item.asks.length > 0) {
                let asksHTML = '';
                const sortedAsks = [...item.asks].reverse();
                sortedAsks.forEach(ask => {
                    const pDecimals = currentSelectedSymbol.includes('VELO') ? 4 : 2;
                    asksHTML += `<div class="flex justify-between text-xs font-mono px-1 py-0.5 rounded"><span class="text-rose-400">${Number(ask[0]).toFixed(pDecimals)}</span><span class="text-slate-400">${Number(ask[1]).toFixed(2)}</span></div>`;
                });
                document.getElementById(`${ex}-asks`).innerHTML = asksHTML;
            } else {
                document.getElementById(`${ex}-asks`).innerHTML = `<div class="text-xs text-slate-500 text-center py-4">No data</div>`;
            }

            if (item.bids && item.bids.length > 0) {
                let bidsHTML = '';
                item.bids.forEach(bid => {
                    const pDecimals = currentSelectedSymbol.includes('VELO') ? 4 : 2;
                    bidsHTML += `<div class="flex justify-between text-xs font-mono px-1 py-0.5 rounded"><span class="text-emerald-400">${Number(bid[0]).toFixed(pDecimals)}</span><span class="text-slate-400">${Number(bid[1]).toFixed(2)}</span></div>`;
                });
                document.getElementById(`${ex}-bids`).innerHTML = bidsHTML;
            } else {
                document.getElementById(`${ex}-bids`).innerHTML = `<div class="text-xs text-slate-500 text-center py-4">No data</div>`;
            }

            if(item.asks && item.asks.length > 0 && item.bids && item.bids.length > 0) {
                const spread = item.asks[0][0] - item.bids[0][0];
                const pDecimals = currentSelectedSymbol.includes('VELO') ? 4 : 2;
                document.getElementById(`${ex}-spread`).innerText = spread.toFixed(pDecimals);
            }

            const bCard = document.getElementById(`balance-${ex}`);
            if (bCard) {
                if (item.is_live && item.balance) {
                    const baseAsset = item.base_asset || 'BTC';
                    const quoteAsset = item.quote_asset || 'USDT';
                    const baseVal = Number(item.balance[baseAsset] || 0);
                    const quoteVal = Number(item.balance[quoteAsset] || 0);

                    let currentPrice = 0;
                    if (item.asks && item.asks.length > 0) {
                        currentPrice = Number(item.asks[0][0]);
                    }
                    const baseValueInUsdt = baseVal * currentPrice;
                    const exTotalNetUsdt = quoteVal + baseValueInUsdt;
                    const bDecimals = baseAsset === 'VELO' ? 2 : 4;

                    bCard.innerHTML = `
                        <div>
                            <div class="flex justify-between items-center mb-1.5 pb-1 border-b border-slate-800/80">
                                <span class="text-xs font-bold text-white tracking-wide">${ex.toUpperCase()}</span>
                                <span class="text-[9px] text-emerald-400 bg-emerald-950/60 px-1.5 py-0.5 rounded border border-emerald-800/80 font-semibold">LIVE API</span>
                            </div>
                            <div class="space-y-1 text-xs">
                                <div class="flex justify-between">
                                    <span class="text-slate-400">${quoteAsset}:</span>
                                    <span class="font-mono font-bold text-emerald-400">$${quoteVal.toLocaleString('en-US', {minimumFractionDigits: 2})}</span>
                                </div>
                                <div class="flex justify-between">
                                    <span class="text-slate-400">${baseAsset}:</span>
                                    <span class="font-mono text-cyan-300 font-semibold">${baseVal.toLocaleString('en-US', {minimumFractionDigits: bDecimals, maximumFractionDigits: bDecimals})}</span>
                                </div>
                                <div class="text-[10px] text-slate-500 text-right">
                                    (≈ $${baseValueInUsdt.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2})} USDT)
                                </div>
                            </div>
                        </div>
                        <div class="pt-2 mt-2 border-t border-slate-800/80 flex justify-between items-center">
                            <span class="text-[10px] font-bold text-slate-400">TOTAL:</span>
                            <span class="text-xs font-bold font-mono text-cyan-400">$${exTotalNetUsdt.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2})} USDT</span>
                        </div>
                    `;
                } else {
                    bCard.innerHTML = `
                        <div class="flex justify-between items-center mb-1">
                            <span class="text-xs font-bold text-slate-400">${ex.toUpperCase()}</span>
                            <span class="text-[9px] text-slate-500 font-semibold bg-slate-900 px-1 py-0.5 rounded border border-slate-800">NOT CONNECTED</span>
                        </div>
                        <div class="text-xs text-slate-500 py-3">กรุณาตั้งค่า API Key เพื่อแสดงยอดพอร์ต</div>
                    `;
                }
            }
        }

        function updateTotalBalance() {
            let totalQuote = 0;
            let totalBase = 0;
            let totalBaseInUsdt = 0;
            let currentBaseAsset = currentSelectedSymbol.split('/')[0];
            let currentQuoteAsset = 'USDT';
            let liveCount = 0;

            Object.values(exchangeData).forEach(d => {
                if(d.is_live && d.balance) {
                    liveCount++;
                    currentBaseAsset = d.base_asset || currentBaseAsset;
                    currentQuoteAsset = d.quote_asset || 'USDT';
                    const baseAmt = Number(d.balance[currentBaseAsset] || 0);
                    totalQuote += Number(d.balance[currentQuoteAsset] || 0);
                    totalBase += baseAmt;

                    let px = 0;
                    if(d.asks && d.asks.length > 0) px = Number(d.asks[0][0]);
                    totalBaseInUsdt += (baseAmt * px);
                }
            });

            const totalQuoteElem = document.getElementById('total-quote');
            const totalBaseElem = document.getElementById('total-base');
            const totalNetElem = document.getElementById('total-net-val');
            const bDecimals = currentBaseAsset === 'VELO' ? 2 : 4;

            if(totalQuoteElem && totalBaseElem && totalNetElem) {
                if (liveCount > 0) {
                    const grandTotalNet = totalQuote + totalBaseInUsdt;
                    totalQuoteElem.innerText = `$${totalQuote.toLocaleString('en-US', {minimumFractionDigits: 2})} ${currentQuoteAsset}`;
                    totalBaseElem.innerHTML = `${totalBase.toLocaleString('en-US', {minimumFractionDigits: bDecimals, maximumFractionDigits: bDecimals})} ${currentBaseAsset} <span class="text-slate-400 font-normal">(≈ $${totalBaseInUsdt.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2})})</span>`;
                    totalNetElem.innerText = `$${grandTotalNet.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2})} USDT`;
                } else {
                    totalQuoteElem.innerText = "$0.00 USDT";
                    totalBaseElem.innerHTML = `0.0000 ${currentBaseAsset}`;
                    totalNetElem.innerText = "$0.00 USDT (ยังไม่มี API)";
                }
            }
        }

        let selectedEx = '';
        let selectedType = '';
        let selectedCategory = 'market';
        let selectedAmount = 0;
        let selectedPrice = 0;

        function openOrderModal(exchange, type) {
            const currentItem = exchangeData[exchange];
            if (!currentItem || !currentItem.is_live) {
                showToast(`กรุณาใส่ API Key ของ ${exchange.toUpperCase()} ก่อนส่งคำสั่งซื้อขาย`, 'error');
                return;
            }

            selectedEx = exchange;
            selectedType = type;
            
            const categoryElem = document.getElementById(`${exchange}-ord-type`);
            selectedCategory = categoryElem ? categoryElem.value : 'market';

            const amountInput = document.getElementById(`${exchange}-amount`);
            selectedAmount = amountInput ? parseFloat(amountInput.value) || 0 : 0;

            if (selectedAmount <= 0) {
                showToast("กรุณาระบุจำนวนเงินที่ต้องการซื้อขายให้ถูกต้อง", "error");
                return;
            }

            // กำหนดราคาอ้างอิง
            let refPrice = '0.00';
            if (currentItem && currentItem.asks && currentItem.bids) {
                if (type === 'BUY' && currentItem.asks.length > 0) refPrice = currentItem.asks[0][0];
                if (type === 'SELL' && currentItem.bids.length > 0) refPrice = currentItem.bids[0][0];
            }

            if (selectedCategory === 'limit') {
                const limitInput = document.getElementById(`${exchange}-limit-price`);
                selectedPrice = (limitInput && parseFloat(limitInput.value) > 0) ? parseFloat(limitInput.value) : parseFloat(refPrice);
            } else {
                selectedPrice = parseFloat(refPrice);
            }

            const pDecimals = currentSelectedSymbol.includes('VELO') ? 4 : 2;

            document.getElementById('modal-exchange').innerText = exchange.toUpperCase();
            document.getElementById('modal-symbol').innerText = currentSelectedSymbol;
            document.getElementById('modal-type').innerText = type;
            document.getElementById('modal-type').className = `font-semibold ${type === 'BUY' ? 'text-emerald-400' : 'text-rose-400'}`;
            document.getElementById('modal-category').innerText = selectedCategory.toUpperCase();
            document.getElementById('modal-amount').innerText = `$${selectedAmount.toLocaleString('en-US')} USDT`;
            document.getElementById('modal-price').innerText = Number(selectedPrice).toFixed(pDecimals) + " USDT";
            
            document.getElementById('order-modal').classList.remove('hidden');
            document.getElementById('order-modal').classList.add('flex');
        }

        function closeModal() {
            document.getElementById('order-modal').classList.add('hidden');
            document.getElementById('order-modal').classList.remove('flex');
        }

        function confirmOrder() {
            ws.send(JSON.stringify({ 
                action: "place_order", 
                exchange: selectedEx, 
                type: selectedType, 
                order_category: selectedCategory,
                symbol: currentSelectedSymbol,
                amount_usdt: selectedAmount,
                price: selectedPrice
            }));

            showToast(`กำลังส่งคำสั่ง ${selectedCategory.toUpperCase()} ไปยัง ${selectedEx.toUpperCase()}...`, 'success');
            closeModal();
        }

        initUI();
        connectWebSocket();
    </script>
</body>
</html>
    """
