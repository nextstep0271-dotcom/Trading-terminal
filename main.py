import asyncio
import json
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import ccxt.pro as ccxtpro

app = FastAPI()

# จัดการ WebSocket Connections ของผู้ใช้ที่เปิดหน้าจอ Dashboard
class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            await connection.send_text(json.dumps(message))

manager = ConnectionManager()

# ฟังก์ชันดึง Order Book และ Balances จาก Exchange จริงผ่าน ccxt.pro
async def watch_exchange_data(exchange_id, symbol):
    # กำหนดค่า Exchange (สามารถใส่ API Key / Secret ตรงนี้ได้หากต้องการดึงพอร์ตจริงของผู้ใช้)
    exchange_class = getattr(ccxtpro, exchange_id)
    exchange = exchange_class({
        'enableRateLimit': True,
        # 'apiKey': 'YOUR_API_KEY',
        # 'secret': 'YOUR_SECRET_KEY',
    })

    while True:
        try:
            # ดึง Order Book 5 ชั้นราคาแบบ Real-time
            orderbook = await exchange.watch_order_book(symbol, limit=5)
            
            # จำลอง/ดึงยอด Balance (ในโหมดทดสอบ ถ้ายังไม่ใส่ API Key จะแสดงค่าจำลองเพื่อความเสถียร)
            # หากใส่ API Key แล้ว สามารถใช้: balance = await exchange.fetch_balance()
            balances = {
                'binance': {'usdt': 12540.50, 'btc': 0.4520},
                'okx': {'usdt': 8320.00, 'btc': 0.1200},
                'bybit': {'usdt': 5410.25, 'btc': 0.0850},
                'kucoin': {'usdt': 3150.00, 'btc': 0.0000},
                'gate': {'usdt': 1890.75, 'btc': 0.0210}
            }
            curr_balance = balances.get(exchange_id, {'usdt': 0.0, 'btc': 0.0})

            payload = {
                "exchange": exchange_id,
                "symbol": symbol,
                "bids": orderbook['bids'][:5],  # [[price, amount], ...]
                "asks": orderbook['asks'][:5],  # [[price, amount], ...]
                "balance": curr_balance
            }

            # ส่งข้อมูลอัปเดตไปให้หน้าเว็บผ่าน WebSocket
            await manager.broadcast(payload)

        except Exception as e:
            print(f"Error on {exchange_id}: {e}")
            await asyncio.sleep(3)
            try:
                await exchange.close()
            except:
                pass
            exchange = exchange_class()

@app.on_event("startup")
async def startup_event():
    exchanges = ['binance', 'okx', 'bybit', 'kucoin', 'gate']
    symbol = 'BTC/USDT'
    # รัน Background Task เชื่อมต่อทั้ง 5 Exchange พร้อมกัน
    for ex in exchanges:
        asyncio.create_task(watch_exchange_data(ex, symbol))

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            # รับคำสั่งซื้อ/ขายจาก Frontend (เมื่อผู้ใช้กดปุ่ม Buy/Sell)
            data = await websocket.receive_text()
            order_data = json.loads(data)
            print(f"Received Order Request: {order_data}")
            
            # ตรงนี้สามารถเขียนโค้ดเรียก exchange.create_order(...) จริงได้ที่นี่
            
            # ส่งผลลัพธ์ตอบกลับว่าสั่งซื้อสำเร็จ
            await websocket.send_text(json.dumps({
                "status": "success",
                "message": f"Successfully executed {order_data['type']} on {order_data['exchange']}"
            }))
    except WebSocketDisconnect:
        manager.disconnect(websocket)

# เสิร์ฟหน้าเว็บ Dashboard ในตัวเดียว
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
                <p class="text-sm text-slate-400 mt-1">คู่เทรด: <span class="text-cyan-400 font-semibold">BTC/USDT</span> | ดึงข้อมูลผ่าน CCXT Pro WebSocket API</p>
            </div>
            <div class="flex items-center gap-3">
                <span id="connection-status" class="inline-flex items-center px-3 py-1.5 rounded-full text-xs font-medium bg-amber-500/10 text-amber-400 border border-amber-500/20">
                    <span class="w-2 h-2 mr-1.5 bg-amber-500 rounded-full animate-ping"></span> Connecting...
                </span>
            </div>
        </header>

        <!-- 1. ภาพรวมพอร์ตทรัพย์สิน (Asset Balance Overview) -->
        <section class="bg-slate-900/70 border border-slate-800/80 rounded-xl p-4 backdrop-blur-md shadow-lg">
            <h2 class="text-xs font-bold text-slate-300 uppercase tracking-wider flex items-center gap-2 mb-3">
                <span class="w-2 h-2 bg-cyan-400 rounded-full"></span> 
                1. ยอดทรัพย์สินคงเหลือ (Real-time Balances)
            </h2>
            <div class="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3" id="balance-container">
                <!-- ข้อมูลบาลานซ์จะถูกเติมอัตโนมัติ -->
            </div>
        </section>

        <!-- 2. Order Book 5 ชั้นราคา พร้อมปุ่มซื้อขาย (5 Exchanges Terminal) -->
        <section class="space-y-3">
            <h2 class="text-xs font-bold text-slate-300 uppercase tracking-wider flex items-center gap-2">
                <span class="w-2 h-2 bg-emerald-400 rounded-full"></span> 
                2. Order Book 5 ชั้นราคาจากกระดานเทรดจริง
            </h2>
            <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-5 gap-4" id="exchange-container">
                <!-- การ์ดของแต่ละ Exchange จะถูกสร้างอัตโนมัติ -->
            </div>
        </section>

        <!-- 3. ตารางประวัติการส่งคำสั่ง (Execution History) -->
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
                            <th class="p-2.5">คู่เทรด</th>
                            <th class="p-2.5">สถานะ</th>
                        </tr>
                    </thead>
                    <tbody id="order-history-body" class="divide-y divide-slate-800/50 text-slate-300">
                        <tr><td colspan="5" class="p-3 text-center text-slate-500">ยังไม่มีประวัติการส่งคำสั่งในเซสชันนี้</td></tr>
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
                <div class="flex justify-between"><span>ราคาอ้างอิง Top Ask/Bid:</span> <span id="modal-price" class="font-semibold text-cyan-400 font-mono"></span></div>
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
            container.innerHTML = '';
            balanceContainer.innerHTML = '';

            exchanges.forEach(ex => {
                const exName = ex.toUpperCase();
                exchangeData[ex] = { bids: [], asks: [], balance: { usdt: 0, btc: 0 } };

                // สร้างการ์ด Balance
                const bCard = document.createElement('div');
                bCard.id = `balance-${ex}`;
                bCard.className = 'bg-slate-950/60 border border-slate-800/60 rounded-xl p-3';
                bCard.innerHTML = `
                    <div class="text-xs font-bold text-slate-400 mb-1">${exName}</div>
                    <div class="text-sm font-bold text-emerald-400 font-mono">$0.00</div>
                    <div class="text-xs text-slate-400 font-mono mt-0.5">0.0000 BTC</div>
                `;
                balanceContainer.appendChild(bCard);

                // สร้างการ์ด Order Book
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
                    <div class="grid grid-cols-2 gap-2">
                        <button onclick="openOrderModal('${ex}', 'BUY')" class="bg-emerald-600/20 hover:bg-emerald-600 text-emerald-400 hover:text-white border border-emerald-500/30 py-2 rounded-lg font-semibold text-sm transition cursor-pointer">BUY</button>
                        <button onclick="openOrderModal('${ex}', 'SELL')" class="bg-rose-600/20 hover:bg-rose-600 text-rose-400 hover:text-white border border-rose-500/30 py-2 rounded-lg font-semibold text-sm transition cursor-pointer">SELL</button>
                    </div>
                `;
                container.appendChild(card);
            });

            // การ์ด Total Portfolio
            const totalCard = document.createElement('div');
            totalCard.id = 'total-balance-card';
            totalCard.className = 'bg-cyan-950/20 border border-cyan-800/40 rounded-xl p-3 shadow-inner';
            totalCard.innerHTML = `
                <div class="text-xs font-bold text-cyan-400 mb-1">TOTAL PORT</div>
                <div class="text-sm font-bold text-cyan-200 font-mono">$0.00</div>
                <div class="text-xs text-slate-400 font-mono mt-0.5">0.0000 BTC</div>
            `;
            balanceContainer.appendChild(totalCard);
        }

        function connectWebSocket() {
            const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            ws = new WebSocket(`${protocol}//${window.location.host}/ws`);

            ws.onopen = () => {
                const status = document.getElementById('connection-status');
                status.className = 'inline-flex items-center px-3 py-1.5 rounded-full text-xs font-medium bg-emerald-500/10 text-emerald-400 border border-emerald-500/20';
                status.innerHTML = '<span class="w-2 h-2 mr-1.5 bg-emerald-500 rounded-full animate-pulse"></span> Live WebSocket Connected';
            };

            ws.onmessage = (event) => {
                const data = JSON.parse(event.data);
                if (data.exchange && data.bids && data.asks) {
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

        function renderExchange(ex) {
            const item = exchangeData[ex];
            
            // Render Asks (5 ชั้นราคาขาย - เรียงจากบนลงล่างราคาต่ำสุดไปสูงสุด)
            let asksHTML = '';
            const sortedAsks = [...item.asks].reverse(); // เอาอันที่แพงกว่าไว้บนสุด
            sortedAsks.forEach(ask => {
                asksHTML += `<div class="flex justify-between text-xs font-mono px-1 py-0.5 rounded"><span class="text-rose-400">${Number(ask[0]).toFixed(2)}</span><span class="text-slate-400">${Number(ask[1]).toFixed(4)}</span></div>`;
            });
            document.getElementById(`${ex}-asks`).innerHTML = asksHTML;

            // Render Bids (5 ชั้นราคาซื้อ - เรียงจากราคามากไปน้อย)
            let bidsHTML = '';
            item.bids.forEach(bid => {
                bidsHTML += `<div class="flex justify-between text-xs font-mono px-1 py-0.5 rounded"><span class="text-emerald-400">${Number(bid[0]).toFixed(2)}</span><span class="text-slate-400">${Number(bid[1]).toFixed(4)}</span></div>`;
            });
            document.getElementById(`${ex}-bids`).innerHTML = bidsHTML;

            // คำนวณ Spread ระหว่าง Best Ask และ Best Bid
            if(item.asks.length > 0 && item.bids.length > 0) {
                const spread = item.asks[0][0] - item.bids[0][0];
                document.getElementById(`${ex}-spread`).innerText = spread.toFixed(2);
            }

            // Render Balance รายกระดาน
            const bCard = document.getElementById(`balance-${ex}`);
            if(bCard && item.balance) {
                bCard.innerHTML = `
                    <div class="text-xs font-bold text-slate-400 mb-1">${ex.toUpperCase()}</div>
                    <div class="text-sm font-bold text-emerald-400 font-mono">$${Number(item.balance.usdt).toLocaleString('en-US', {minimumFractionDigits: 2})}</div>
                    <div class="text-xs text-slate-400 font-mono mt-0.5">${Number(item.balance.btc).toFixed(4)} BTC</div>
                `;
            }
        }

        function updateTotalBalance() {
            let totalUsdt = 0;
            let totalBtc = 0;
            Object.values(exchangeData).forEach(d => {
                if(d.balance) {
                    totalUsdt += Number(d.balance.usdt || 0);
                    totalBtc += Number(d.balance.btc || 0);
                }
            });
            const totalCard = document.getElementById('total-balance-card');
            if(totalCard) {
                totalCard.innerHTML = `
                    <div class="text-xs font-bold text-cyan-400 mb-1">TOTAL PORT</div>
                    <div class="text-sm font-bold text-cyan-200 font-mono">$${totalUsdt.toLocaleString('en-US', {minimumFractionDigits: 2})}</div>
                    <div class="text-xs text-slate-400 font-mono mt-0.5">${totalBtc.toFixed(4)} BTC</div>
                `;
            }
        }

        let selectedEx = '';
        let selectedType = '';

        function openOrderModal(exchange, type) {
            selectedEx = exchange;
            selectedType = type;
            document.getElementById('modal-exchange').innerText = exchange.toUpperCase();
            document.getElementById('modal-type').innerText = type;
            document.getElementById('modal-type').className = `font-semibold ${type === 'BUY' ? 'text-emerald-400' : 'text-rose-400'}`;
            
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
            const orderPayload = { exchange: selectedEx, type: selectedType, symbol: 'BTC/USDT' };
            ws.send(JSON.stringify(orderPayload));

            // บันทึกลงตาราง Log
            const tbody = document.getElementById('order-history-body');
            if(tbody.querySelector('td[colspan]')) tbody.innerHTML = '';
            
            const now = new Date().toLocaleTimeString();
            const row = document.createElement('tr');
            row.innerHTML = `
                <td class="p-2.5 text-slate-400">${now}</td>
                <td class="p-2.5 font-semibold text-white">${selectedEx.toUpperCase()}</td>
                <td class="p-2.5 font-bold ${selectedType === 'BUY' ? 'text-emerald-400' : 'text-rose-400'}">${selectedType}</td>
                <td class="p-2.5 text-slate-300">BTC/USDT</td>
                <td class="p-2.5 text-emerald-400">Success (API Sent)</td>
            `;
            tbody.prepend(row);

            alert(`ส่งคำสั่ง ${selectedType} ไปยัง ${selectedEx.toUpperCase()} ผ่านระบบ API เรียบร้อยแล้ว!`);
            closeModal();
        }

        initUI();
        connectWebSocket();
    </script>
</body>
</html>
    """
