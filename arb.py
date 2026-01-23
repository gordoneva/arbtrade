# main_arb.py
import asyncio
import logging
import ccxt.async_support as ccxt
import time
import os
import sys
import collections
import numpy as np
from datetime import datetime

import config_lighter as cfg
from lighter_api import LighterExchange

# ==========================================================
# 🔧 WINDOWS UTF-8 FIX
# ==========================================================
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

# ==========================================================
# 📝 LOGGER SETUP
# ==========================================================
if not os.path.exists("logs"): os.makedirs("logs")

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    handlers=[
        logging.FileHandler(cfg.LOG_FILE, encoding='utf-8'), 
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("LighterSniper")

# ==========================================================
# 💾 DATA STORAGE
# ==========================================================
TICKS = collections.defaultdict(list)   # Тики с OKX
LAST_TRADE = {}                         # Время последней сделки
ACTIVE_POSITIONS = {}                   # Открытые сделки

# ==========================================================
# 🧮 UTILITIES
# ==========================================================
def calculate_btc_correlation(symbol, btc_symbol, ticks_dict, window=10):
    """Считает корреляцию движения монеты относительно BTC"""
    if symbol not in ticks_dict or btc_symbol not in ticks_dict: return 0.0
    
    p_coin = [t['price'] for t in ticks_dict[symbol][-window:]]
    p_btc = [t['price'] for t in ticks_dict[btc_symbol][-window:]]
    
    if len(p_coin) < window or len(p_btc) < window: return 0.0
    
    coin_norm = np.diff(p_coin)
    btc_norm = np.diff(p_btc)
    
    if len(coin_norm) == 0: return 0.0
    if np.std(coin_norm) == 0 or np.std(btc_norm) == 0: return 0.0
    return np.corrcoef(coin_norm, btc_norm)[0, 1]

# ==========================================================
# 🛡 POSITION MANAGER
# ==========================================================
async def position_manager(lighter: LighterExchange):
    logger.info("🛡 Position Manager Started")
    while True:
        try:
            if not ACTIVE_POSITIONS:
                await asyncio.sleep(1)
                continue
                
            for l_symbol, pos in list(ACTIVE_POSITIONS.items()):
                bid, ask = await lighter.get_orderbook_price(l_symbol)
                if not bid: continue
                
                # SL LOGIC
                if bid <= pos['sl']:
                    logger.warning(f"📉 SL TRIGGER: {l_symbol} | Price {bid} < {pos['sl']}")
                    await lighter.create_market_sell(l_symbol, pos['amount'])
                    del ACTIVE_POSITIONS[l_symbol]
        except Exception as e:
            logger.error(f"Pos Manager Error: {e}")
            
        await asyncio.sleep(1)

# ==========================================================
# 🚀 MAIN LOOP
# ==========================================================
async def main():
    okx = None
    try:
        # 1. Инициализация
        lighter = LighterExchange()
        await lighter.initialize()
        
        okx = ccxt.okx({'enableRateLimit': True})
        await okx.load_markets()
        
        # 2. Whitelist
        all_okx = list(okx.markets.keys())
        pairs_map = lighter.get_common_pairs(all_okx)
        
        if not pairs_map:
            logger.error("No common pairs found!")
            return
            
        logger.info(f"Target Pairs: {[p['lighter'] for p in pairs_map]}")
        target_okx_symbols = [p['okx'] for p in pairs_map]
        btc_symbol = 'BTC/USDT'
        
        asyncio.create_task(position_manager(lighter))

        logger.info("🔥 Sniper Started. Waiting for impulses...")

        # Переменная для таймера логов
        last_log_time = 0
        LOG_INTERVAL = 5 # Логировать статус каждые 5 секунд

        while True:
            try:
                # Получаем свежие цены с OKX
                tickers = await okx.fetch_tickers(target_okx_symbols + [btc_symbol])
                now = time.time()
                
                # 1. Обновляем историю цен (буфер 50 секунд)
                for s, t in tickers.items():
                    TICKS[s].append({'price': t['last'], 'time': now})
                    if len(TICKS[s]) > 50: TICKS[s].pop(0)

                # Нужно ли выводить логи сейчас? (Heartbeat)
                should_log = (now - last_log_time) > LOG_INTERVAL
                if should_log:
                    print("-" * 60) # Разделитель

                for pair in pairs_map:
                    okx_s = pair['okx']
                    lighter_s = pair['lighter']
                    
                    if okx_s not in tickers: continue
                    
                    price = tickers[okx_s]['last']
                    
                    # 2. МАТЕМАТИКА (ИСПРАВЛЕННАЯ)
                    # Считаем корреляцию с битком
                    corr = calculate_btc_correlation(okx_s, btc_symbol, TICKS)
                    
                    # Считаем изменение цены за ~50 секунд
                    change_1m = 0.0
                    # Проверяем, что накопили хотя бы 15 тиков, чтобы считать тренд
                    if len(TICKS[okx_s]) > 15:
                        # БЕРЕМ САМЫЙ СТАРЫЙ ТИК (индекс 0), а не предыдущий
                        start_p = TICKS[okx_s][0]['price'] 
                        if start_p > 0:
                            change_1m = (price - start_p) / start_p * 100
                    
                    # === ЛОГИРОВАНИЕ СТАТУСА ===
                    if should_log:
                        # Выводим инфо, чтобы видеть, что бот живой
                        logger.info(f"👀 SCAN: {okx_s:<10} | Price: {price:<8} | Chg: {change_1m:+.3f}% | Corr: {corr:+.2f}")

                    # === 3. ЛОГИКА СИГНАЛА ===
                    signal = None
                    
                    # ВАЖНО: Для теста поставьте здесь 0.01. 
                    # Для реальной торговли используйте cfg.PRICE_CHANGE_TRIGGER (обычно 0.3 - 0.5)
                    TRIGGER_PERCENT = 0.01 # <--- ТЕСТОВЫЙ РЕЖИМ (0.01%)
                    
                    if change_1m > TRIGGER_PERCENT: 
                        # Фильтр: входим только если это НЕ движение за битком (раскорреляция)
                        # Если corr низкая (< 0.8), значит монета пампится сама по себе -> ХОРОШО
                        if corr < cfg.BTC_LAG_THRESHOLD:
                            signal = 'BUY'
                            
                    # Проверка кулдауна (чтобы не купить одну и ту же монету 2 раза подряд)
                    if signal == 'BUY':
                        last_t = LAST_TRADE.get(lighter_s, 0)
                        if (now - last_t) < cfg.COOLDOWN:
                            signal = None
                    
                    # === 4. ИСПОЛНЕНИЕ ===
                    if signal == 'BUY':
                        logger.info(f"🚀 SIGNAL DETECTED: {okx_s} | Change: {change_1m:.3f}% | Corr: {corr:.2f}")
                        
                        # Запрашиваем стакан Lighter
                        bid_l, ask_l = await lighter.get_orderbook_price(lighter_s)
                        
                        if ask_l:
                            # Проверяем разницу цен (Arbitrage Check)
                            diff = (ask_l - price) / price
                            logger.info(f"🔎 Price Check: OKX={price} vs Lighter={ask_l} (Diff: {diff*100:.2f}%)")
                            
                            # Если цена на Lighter не сильно хуже OKX
                            if diff <= cfg.MAX_PRICE_DIFF:
                                # ПОКУПКА
                                res = await lighter.create_market_buy(lighter_s, cfg.ORDER_SIZE_USDC)
                                
                                if res:
                                    logger.info(f"✅ BOUGHT {lighter_s}")
                                    LAST_TRADE[lighter_s] = now
                                    
                                    entry_p = float(res['price'])
                                    amt = float(res['amount'])
                                    
                                    # Выставляем Take Profit (Limit Order)
                                    tp_price = entry_p * (1 + cfg.TAKE_PROFIT_PCT)
                                    await lighter.create_limit_sell(lighter_s, amt, tp_price)
                                    
                                    # Запоминаем для Stop Loss (программный)
                                    sl_price = entry_p * (1 - cfg.STOP_LOSS_PCT)
                                    ACTIVE_POSITIONS[lighter_s] = {
                                        'entry': entry_p,
                                        'amount': amt,
                                        'sl': sl_price
                                    }
                            else:
                                logger.warning(f"🚫 SKIP: Price on Lighter too high (+{diff*100:.2f}%)")
                        else:
                            logger.warning(f"🚫 SKIP: No Liquidity on Lighter for {lighter_s}")

                if should_log:
                    last_log_time = now

                await asyncio.sleep(1) # Пауза 1 секунда между сканированиями

            except Exception as e:
                logger.error(f"Main Loop Error: {e}")
                await asyncio.sleep(5)

    except KeyboardInterrupt:
        logger.info("Bot Stopped")
    finally:
        if okx:
            await okx.close()

if __name__ == "__main__":
    asyncio.run(main())
