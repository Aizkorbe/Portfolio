# ─────────────────────────────────────────────
# Imports estándar y de terceros
# ─────────────────────────────────────────────
import csv        # lectura/escritura de archivos CSV para exportar e importar operaciones
import io         # StringIO: genera el CSV en memoria sin escribir en disco
import sqlite3    # base de datos local donde se guardan todas las operaciones
import time       # timestamps para los cachés de precios

from datetime import datetime  # formateo de fechas legibles en la UI

import pandas as pd                          # manipulación de series de precios históricos
import plotly.graph_objects as go            # construcción de gráficos interactivos
import plotly.io as pio                      # exportar gráficos a HTML embebible
import yfinance as yf                        # descarga de precios desde Yahoo Finance
from flask import Flask, render_template, request, redirect, url_for, Response, flash

# ─────────────────────────────────────────────
# Inicialización de Flask
# ─────────────────────────────────────────────
app = Flask(__name__)
app.secret_key = "portfolio_dev_secret"   # necesario para que flash() funcione
DB_NAME = "portfolio.db"                  # archivo SQLite; se crea automáticamente si no existe

# Una onza troy = 31.1034768 g. Se usa para convertir el precio spot del oro/plata
# (que yfinance devuelve en USD/oz) a EUR/g o EUR/oz según el ticker elegido.
TROY_OUNCE_IN_GRAMS = 31.1034768

# ─────────────────────────────────────────────
# Caché de precios de mercado (tiempo real)
# ─────────────────────────────────────────────
# Se guarda en memoria para no llamar a yfinance en cada petición HTTP.
# TTL de 3 minutos: suficientemente fresco sin saturar la API.
# tickers_key guarda qué conjunto de tickers se cacheó; si cambia (nuevo activo),
# se invalida aunque el TTL no haya expirado.
PRICE_CACHE = {"timestamp": 0, "data": {}, "tickers_key": None}
PRICE_TTL_SECONDS = 180

# ─────────────────────────────────────────────
# Caché de precios históricos (gráficos)
# ─────────────────────────────────────────────
# Las series históricas son mucho más grandes; se cachean 1 hora para no
# redescargar al navegar entre páginas.
HIST_CACHE = {}
HIST_TTL_SECONDS = 3600

# ─────────────────────────────────────────────
# Mapeo de tickers internos → símbolos de yfinance
# ─────────────────────────────────────────────
# BTC y ETH ya cotizan en EUR en yfinance. Los metales (GC=F, SI=F) cotizan
# en USD/oz, por lo que necesitan conversión con EURUSD=X.
TICKER_TO_YFSYMBOL = {
    "BTC": "BTC-EUR",
    "ETH": "ETH-EUR",
    "ORO": "GC=F",    # oro en USD/oz → se convierte a EUR/g
    "XAU": "GC=F",    # oro en USD/oz → se convierte a EUR/oz
    "PLATA": "SI=F",  # plata en USD/oz → se convierte a EUR/g
    "XAG": "SI=F",    # plata en USD/oz → se convierte a EUR/oz
}

# METAL_TICKERS: estos tickers tienen precio en USD y necesitan la tasa EUR/USD.
METAL_TICKERS = {"ORO", "XAU", "PLATA", "XAG"}

# GRAM_TICKERS: el usuario registró sus compras en gramos, no en onzas,
# así que el precio final se divide entre TROY_OUNCE_IN_GRAMS.
GRAM_TICKERS = {"ORO", "PLATA"}

# Sufijos de bolsas europeas que cotizan directamente en EUR (o GBP en .L).
# Si un ticker genérico termina en uno de estos sufijos, no necesita conversión USD→EUR.
EUR_EXCHANGE_SUFFIXES = frozenset({
    ".PA", ".MC", ".DE", ".L", ".AS", ".BR", ".MI", ".SW",
    ".HE", ".OL", ".ST", ".CO", ".LS", ".WA", ".PR", ".AT",
    ".F", ".BE", ".MU", ".DU", ".HA", ".SG",
})

# Opciones de benchmark para el gráfico de evolución.
# La clave se pasa como parámetro de URL (?benchmark=BTC); el valor es
# el símbolo que se descarga directamente de yfinance.
BENCHMARK_OPTIONS = {
    "BTC": "BTC-EUR",
    "ETH": "ETH-EUR",
    "SP500": "^GSPC",  # S&P 500 en USD; se normaliza, no se convierte a EUR
    "none": None,
}

# Operaciones por página en el listado de historial
OPS_PER_PAGE = 25


# ─────────────────────────────────────────────
# Base de datos
# ─────────────────────────────────────────────

def get_db_connection():
    """Abre y devuelve una conexión SQLite con row_factory para acceder a columnas por nombre."""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row  # permite hacer row["campo"] en vez de row[0]
    return conn


def init_db():
    """
    Crea la tabla 'operaciones' si no existe.
    También añade la columna 'ticker' si falta (migraciones de versiones anteriores).
    """
    conn = get_db_connection()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS operaciones (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fecha TEXT NOT NULL,
            ticker TEXT,
            nombre TEXT NOT NULL,
            tipo TEXT NOT NULL,         -- 'Compra' o 'Venta'
            cantidad REAL NOT NULL,
            precio_eur REAL NOT NULL,   -- precio unitario en el momento de la operación
            comision_eur REAL NOT NULL DEFAULT 0,
            estrategia TEXT,
            notas TEXT
        )
    """)
    # Migración: versiones antiguas de la app no tenían columna 'ticker'
    columns = [row["name"] for row in conn.execute("PRAGMA table_info(operaciones)").fetchall()]
    if "ticker" not in columns:
        conn.execute("ALTER TABLE operaciones ADD COLUMN ticker TEXT")
    conn.commit()
    conn.close()


# ─────────────────────────────────────────────
# Filtros de formato para las plantillas Jinja
# ─────────────────────────────────────────────

def format_decimal(value, max_decimals=10, use_grouping=False):
    """
    Formatea un número eliminando ceros finales innecesarios.
    Ejemplo: 0.07500000 → '0.075', 1234.50 → '1,234.5' (con grouping).
    El caso '-0' se normaliza a '0' para evitar mostrarlo en la UI.
    """
    try:
        num = float(value)
        text = f"{num:,.{max_decimals}f}" if use_grouping else f"{num:.{max_decimals}f}"
        text = text.rstrip("0").rstrip(".")
        return "0" if text == "-0" else text
    except (TypeError, ValueError):
        return "0"


@app.template_filter("num")
def num(value):
    """Filtro Jinja: número con hasta 10 decimales, sin separador de miles. Ej: {{ 0.075 | num }} → '0.075'"""
    return format_decimal(value, 10, False)


@app.template_filter("eur")
def eur(value):
    """Filtro Jinja: importe en euros con separador de miles y 2 decimales. Ej: {{ 1234.5 | eur }} → '€1,234.50'"""
    return f"€{format_decimal(value, 2, True)}"


# ─────────────────────────────────────────────
# Resolución de tickers a símbolos de yfinance
# ─────────────────────────────────────────────

def get_yf_info(ticker):
    """
    Dado un ticker interno (BTC, AAPL, SAN.MC…) devuelve:
      - yf_symbol: el símbolo que se usará en yfinance.download()
      - needs_usd_to_eur: True si el precio de yfinance está en USD y hay que convertir
      - is_per_gram: True si el precio está en USD/oz pero el usuario opera en gramos

    Lógica de detección:
      1. Si está en TICKER_TO_YFSYMBOL, se usa el mapeo explícito.
      2. Si termina en '-EUR' o con sufijo de bolsa europea, se asume que yfinance
         ya devuelve el precio en EUR.
      3. En cualquier otro caso se asume acción cotizada en USD (bolsas US).
    """
    if ticker in TICKER_TO_YFSYMBOL:
        return TICKER_TO_YFSYMBOL[ticker], ticker in METAL_TICKERS, ticker in GRAM_TICKERS
    t = ticker.upper()
    if t.endswith("-EUR") or any(t.endswith(s.upper()) for s in EUR_EXCHANGE_SUFFIXES):
        return t, False, False
    return t, True, False  # acción en USD por defecto


# ─────────────────────────────────────────────
# Descarga de precios de mercado (recientes)
# ─────────────────────────────────────────────

def get_last_close_from_download(symbols):
    """
    Descarga los últimos 5 días de precios diarios para la lista de símbolos
    y devuelve el cierre más reciente de cada uno.

    Se piden 5 días en vez de 1 para cubrir fines de semana y festivos,
    donde yfinance no tiene datos del día actual.

    yfinance tiene un comportamiento diferente según el número de símbolos:
      - 1 símbolo: las columnas del DataFrame son planas (df["Close"])
      - N símbolos: las columnas tienen doble nivel (df[symbol]["Close"])
    Por eso se trata cada caso por separado.
    """
    try:
        data = yf.download(
            tickers=symbols,
            period="5d",
            interval="1d",
            group_by="ticker",
            auto_adjust=False,
            progress=False,
            threads=True
        )
        results = {}
        if data is None or data.empty:
            return {symbol: 0.0 for symbol in symbols}
        if len(symbols) == 1:
            symbol = symbols[0]
            try:
                close_series = data["Close"].dropna()
                results[symbol] = float(close_series.iloc[-1]) if not close_series.empty else 0.0
            except Exception:
                results[symbol] = 0.0
            return results
        for symbol in symbols:
            try:
                close_series = data[symbol]["Close"].dropna()
                results[symbol] = float(close_series.iloc[-1]) if not close_series.empty else 0.0
            except Exception:
                results[symbol] = 0.0
        return results
    except Exception:
        return {symbol: 0.0 for symbol in symbols}


def get_cached_market_prices(force_refresh=False, portfolio_tickers=None):
    """
    Devuelve un diccionario {ticker: precio_en_eur} para todos los activos
    del portfolio, usando caché de 3 minutos para no sobrecargar yfinance.

    portfolio_tickers: lista de tickers internos actualmente en cartera.
    Si cambia (se añade un activo nuevo), tickers_key es distinto y se invalida
    el caché aunque el TTL no haya expirado.

    Siempre se incluye EURUSD=X en la descarga para poder convertir metales y
    acciones en USD a EUR en el mismo lote de peticiones.
    """
    now = time.time()
    ptickers_key = frozenset(portfolio_tickers) if portfolio_tickers else None

    # Devolver caché si sigue vigente y cubre los mismos tickers
    if (not force_refresh
            and now - PRICE_CACHE["timestamp"] < PRICE_TTL_SECONDS
            and PRICE_CACHE["data"]
            and PRICE_CACHE.get("tickers_key") == ptickers_key):
        return PRICE_CACHE["data"], PRICE_CACHE["timestamp"]

    # Construir el conjunto de símbolos yfinance necesarios
    tickers_to_price = list(portfolio_tickers) if portfolio_tickers else list(TICKER_TO_YFSYMBOL.keys())
    symbols_needed = {"EURUSD=X"}
    yf_map = {}
    for ticker in tickers_to_price:
        yf_sym, is_usd, is_gram = get_yf_info(ticker)
        yf_map[ticker] = (yf_sym, is_usd, is_gram)
        symbols_needed.add(yf_sym)

    raw = get_last_close_from_download(list(symbols_needed))
    eurusd = raw.get("EURUSD=X", 1.0) or 1.0  # fallback a 1.0 si la descarga falla

    # Convertir precios brutos a EUR/unidad según el tipo de activo
    prices = {}
    for ticker, (yf_sym, is_usd, is_gram) in yf_map.items():
        raw_price = raw.get(yf_sym, 0.0)
        if raw_price <= 0:
            prices[ticker] = 0.0
        elif is_usd:
            price_eur = raw_price / eurusd
            prices[ticker] = price_eur / TROY_OUNCE_IN_GRAMS if is_gram else price_eur
        else:
            prices[ticker] = raw_price / TROY_OUNCE_IN_GRAMS if is_gram else raw_price

    PRICE_CACHE["timestamp"] = now
    PRICE_CACHE["data"] = prices
    PRICE_CACHE["tickers_key"] = ptickers_key
    return prices, now


def format_cache_time(ts):
    """Convierte un timestamp UNIX a cadena legible para mostrar en la UI."""
    if not ts:
        return "Sin datos"
    return datetime.fromtimestamp(ts).strftime("%d/%m/%Y %H:%M:%S")


# ─────────────────────────────────────────────
# Análisis de operaciones (FIFO)
# ─────────────────────────────────────────────

def analyze_operations(rows):
    """
    Procesa todas las operaciones en orden cronológico y calcula:
      - holdings: posiciones actualmente abiertas con cantidad y coste medio
      - sale_history: detalle de cada venta con su resultado realizado
      - summary: totales globales (invertido, vendido, comisiones, P/L realizado)
      - ticker_commissions: total de comisiones pagadas por ticker
      - first_buy_dates: fecha de la primera compra de cada ticker

    Método de precio medio (coste medio ponderado):
      Al comprar, el coste total se acumula incluyendo la comisión.
      Al vender, se descuenta del coste total la proporción vendida
      según el precio medio vigente antes de esa venta.
      Esto es equivalente al método FIFO en precio promedio.
    """
    holdings = {}       # estado actual de cada posición abierta
    sale_history = []   # registro de ventas para el historial
    total_invertido = 0.0
    total_vendido = 0.0
    resultado_realizado = 0.0
    comisiones_compra = 0.0
    comisiones_venta = 0.0
    ticker_commissions = {}   # comisiones totales por ticker
    first_buy_dates = {}      # primera compra de cada ticker (para máx/mín histórico)

    # Ordenar por fecha y luego por id garantiza el orden cronológico correcto
    # incluso si hay dos operaciones el mismo día
    sorted_rows = sorted(
        rows,
        key=lambda r: (r["fecha"], r["id"] if "id" in r.keys() else 0)
    )

    for row in sorted_rows:
        ticker = (row["ticker"] or "").upper()
        nombre = row["nombre"]
        tipo = (row["tipo"] or "").strip().upper()
        cantidad = float(row["cantidad"] or 0)
        precio_eur = float(row["precio_eur"] or 0)
        comision_eur = float(row["comision_eur"] or 0)
        fecha = row["fecha"]

        # Si no hay ticker explícito, se usa el nombre en mayúsculas como identificador
        if not ticker:
            ticker = nombre.upper()

        # Inicializar el holding si es la primera operación de este ticker
        if ticker not in holdings:
            holdings[ticker] = {
                "ticker": ticker,
                "nombre": nombre,
                "cantidad": 0.0,
                "coste_total": 0.0,
                "precio_medio": 0.0
            }

        h = holdings[ticker]
        ticker_commissions.setdefault(ticker, 0.0)

        if tipo == "COMPRA":
            # El coste de compra incluye la comisión: así el precio medio refleja
            # el coste real de adquisición, no solo el precio de mercado
            coste_compra = (cantidad * precio_eur) + comision_eur
            total_invertido += coste_compra
            comisiones_compra += comision_eur
            ticker_commissions[ticker] += comision_eur
            h["cantidad"] += cantidad
            h["coste_total"] += coste_compra
            h["precio_medio"] = h["coste_total"] / h["cantidad"] if h["cantidad"] > 0 else 0.0
            # Guardar la primera compra para calcular máx/mín desde ese momento
            if ticker not in first_buy_dates:
                first_buy_dates[ticker] = fecha

        elif tipo == "VENTA":
            if h["cantidad"] <= 0:
                continue  # ignorar ventas sin posición abierta (datos inconsistentes)
            ticker_commissions[ticker] += comision_eur
            # Si se vende más de lo que hay, se limita a la posición disponible
            cantidad_vendida = min(cantidad, h["cantidad"])
            precio_medio_actual = h["coste_total"] / h["cantidad"] if h["cantidad"] > 0 else 0.0
            importe_bruto_venta = cantidad_vendida * precio_eur
            importe_neto_venta = importe_bruto_venta - comision_eur
            # Coste asignado: la parte proporcional del coste total que corresponde a
            # las unidades vendidas según el precio medio actual
            coste_asignado_venta = cantidad_vendida * precio_medio_actual
            # Resultado = lo que se cobró (neto) menos lo que costó (precio medio)
            resultado_venta = importe_neto_venta - coste_asignado_venta

            total_vendido += importe_neto_venta
            comisiones_venta += comision_eur
            resultado_realizado += resultado_venta

            sale_history.append({
                "fecha": fecha,
                "ticker": ticker,
                "nombre": nombre,
                "cantidad_vendida": cantidad_vendida,
                "precio_venta_eur": precio_eur,
                "importe_bruto_venta_eur": importe_bruto_venta,
                "comision_venta_eur": comision_eur,
                "importe_neto_venta_eur": importe_neto_venta,
                "coste_asignado_eur": coste_asignado_venta,
                "resultado_realizado_eur": resultado_venta
            })

            # Actualizar la posición restante tras la venta
            h["cantidad"] -= cantidad_vendida
            h["coste_total"] -= coste_asignado_venta
            if h["cantidad"] > 0:
                h["precio_medio"] = h["coste_total"] / h["cantidad"]
            else:
                # Posición cerrada: resetear para evitar valores residuales
                h["cantidad"] = 0.0
                h["coste_total"] = 0.0
                h["precio_medio"] = 0.0

    # Solo se devuelven posiciones con cantidad > 0 (abiertas)
    holdings_abiertas = {t: hh for t, hh in holdings.items() if hh["cantidad"] > 0}

    return {
        "holdings": holdings_abiertas,
        "sale_history": list(reversed(sale_history)),  # más recientes primero
        "summary": {
            "total_invertido_eur": total_invertido,
            "total_vendido_eur": total_vendido,
            "resultado_realizado_eur": resultado_realizado,
            "comisiones_compra_eur": comisiones_compra,
            "comisiones_venta_eur": comisiones_venta,
        },
        "ticker_commissions": ticker_commissions,
        "first_buy_dates": first_buy_dates,
    }


# ─────────────────────────────────────────────
# Resumen fiscal
# ─────────────────────────────────────────────

def compute_fiscal_summary(sale_history):
    """
    Agrupa el resultado realizado por año fiscal.
    Devuelve una lista de (año, {resultado_eur, comisiones_eur, num_ventas})
    ordenada del año más reciente al más antiguo.

    El resultado neto (resultado_eur - comisiones_eur) es el que habitualmente
    se declara como ganancia/pérdida patrimonial en el IRPF.
    """
    by_year = {}
    for sale in sale_history:
        year = sale["fecha"][:4]  # los primeros 4 caracteres de 'YYYY-MM-DD'
        entry = by_year.setdefault(year, {"resultado_eur": 0.0, "comisiones_eur": 0.0, "num_ventas": 0})
        entry["resultado_eur"] += sale["resultado_realizado_eur"]
        entry["comisiones_eur"] += sale["comision_venta_eur"]
        entry["num_ventas"] += 1
    return sorted(by_year.items(), key=lambda x: x[0], reverse=True)


# ─────────────────────────────────────────────
# Descarga y caché de precios históricos
# ─────────────────────────────────────────────

def _fetch_and_cache_hist(symbols_needed, start_date, end_date):
    """
    Descarga precios diarios de cierre (Close) para los símbolos indicados
    entre start_date y end_date, cacheando el resultado 1 hora.

    La clave de caché incluye la fecha de inicio y los símbolos ordenados
    para reutilizar la misma descarga entre el gráfico de evolución y
    el cálculo de máx/mín de precios cuando coinciden los símbolos.

    EURUSD=X se devuelve por separado (hist_eurusd) para que los llamantes
    puedan aplicar la conversión con la tasa del día exacto.
    """
    cache_key = f"{start_date.date()}_{','.join(sorted(symbols_needed))}"
    now = time.time()
    if cache_key in HIST_CACHE and now - HIST_CACHE[cache_key]["ts"] < HIST_TTL_SECONDS:
        c = HIST_CACHE[cache_key]
        return c["prices"], c["eurusd"]

    hist_prices = {}
    hist_eurusd = None
    syms = list(symbols_needed)
    try:
        dl = yf.download(
            tickers=syms,
            start=start_date,
            end=end_date,
            interval="1d",
            group_by="ticker",
            auto_adjust=False,
            progress=False
        )
        if dl is not None and not dl.empty:
            # yfinance devuelve columnas planas cuando se descarga 1 solo símbolo;
            # con múltiples símbolos usa un índice jerárquico: dl[symbol]["Close"]
            if len(syms) == 1:
                hist_prices[syms[0]] = dl["Close"].dropna()
            else:
                for sym in syms:
                    try:
                        hist_prices[sym] = dl[sym]["Close"].dropna()
                    except Exception:
                        hist_prices[sym] = pd.Series(dtype=float)
            # Separar EURUSD del resto para usarlo como conversor de divisa
            if "EURUSD=X" in hist_prices:
                hist_eurusd = hist_prices.pop("EURUSD=X")
    except Exception:
        pass  # si la descarga falla, se devuelven dicts vacíos; la UI mostrará '—'

    HIST_CACHE[cache_key] = {"ts": now, "prices": hist_prices, "eurusd": hist_eurusd}
    return hist_prices, hist_eurusd


def _get_hist_price_at_date(ticker, date, hist_prices, hist_eurusd):
    """
    Devuelve el precio en EUR de un ticker en una fecha concreta (o la más cercana
    anterior disponible, para cubrir festivos y fines de semana).

    Se usa al construir el gráfico de evolución: para cada operación se necesita
    valorar la cartera completa a precio de mercado en esa fecha exacta.
    """
    yf_sym, is_usd, is_gram = get_yf_info(ticker)

    if yf_sym not in hist_prices:
        return 0.0
    series = hist_prices[yf_sym]
    if series is None or series.empty:
        return 0.0

    # Buscar el último precio disponible hasta 'date' inclusive
    available = series.index[series.index <= date]
    if available.empty:
        return 0.0

    price_raw = float(series.loc[available[-1]])

    if is_usd:
        # Usar la tasa EUR/USD del mismo día (o la más reciente anterior)
        if hist_eurusd is None or hist_eurusd.empty:
            return 0.0
        eurusd_avail = hist_eurusd.index[hist_eurusd.index <= date]
        if eurusd_avail.empty:
            return 0.0
        eurusd = float(hist_eurusd.loc[eurusd_avail[-1]])
        if eurusd <= 0:
            return 0.0
        price_eur = price_raw / eurusd
        return price_eur / TROY_OUNCE_IN_GRAMS if is_gram else price_eur

    return price_raw / TROY_OUNCE_IN_GRAMS if is_gram else price_raw


# ─────────────────────────────────────────────
# Gráfico de evolución de cartera
# ─────────────────────────────────────────────

def build_portfolio_evolution_chart(rows, benchmark=None):
    """
    Construye un gráfico de líneas con la evolución histórica de la cartera:
      - Línea azul: coste acumulado (dinero realmente invertido)
      - Línea roja: valor estimado a precio de mercado en cada fecha de operación
      - Línea amarilla (opcional): benchmark normalizado

    El benchmark se normaliza para que empiece en el mismo valor que el coste
    acumulado en la primera operación. Así la comparación es visual (rendimiento
    relativo), no absoluta. No se ajusta por EUR/USD en ^GSPC; para una cartera
    en EUR es una aproximación aceptable.

    El gráfico solo tiene puntos en fechas donde hubo una operación; no muestra
    la evolución diaria continua (eso requeriría descargar precios para cada día,
    lo cual sería muy lento).
    """
    if not rows:
        return None

    sorted_rows = sorted(
        rows,
        key=lambda r: (r["fecha"], r["id"] if "id" in r.keys() else 0)
    )

    # Recopilar todos los tickers del portfolio para saber qué símbolos descargar
    all_tickers = set()
    for row in sorted_rows:
        t = (row["ticker"] or "").upper() or row["nombre"].upper()
        all_tickers.add(t)

    symbols_needed = set()
    needs_eurusd = False
    for t in all_tickers:
        yf_sym, is_usd, _ = get_yf_info(t)
        symbols_needed.add(yf_sym)
        if is_usd:
            needs_eurusd = True

    if not symbols_needed:
        return None

    if needs_eurusd:
        symbols_needed.add("EURUSD=X")

    if benchmark:
        symbols_needed.add(benchmark)
        # El S&P 500 cotiza en USD; aunque no convertimos la línea benchmark a EUR,
        # sí necesitamos EURUSD para valorar los activos del portfolio en EUR
        if benchmark == "^GSPC":
            symbols_needed.add("EURUSD=X")

    # La descarga comienza en la fecha de la primera operación
    start_date = pd.to_datetime(sorted_rows[0]["fecha"]).normalize()
    end_date = pd.Timestamp.now().normalize() + pd.Timedelta(days=2)  # +2 días para incluir hoy

    hist_prices, hist_eurusd = _fetch_and_cache_hist(symbols_needed, start_date, end_date)

    # Simular la cartera avanzando operación a operación
    holdings = {}
    points = []

    for row in sorted_rows:
        ticker = (row["ticker"] or "").upper() or row["nombre"].upper()
        tipo = (row["tipo"] or "").strip().upper()
        cantidad = float(row["cantidad"] or 0)
        precio_eur = float(row["precio_eur"] or 0)
        comision_eur = float(row["comision_eur"] or 0)
        fecha = pd.to_datetime(row["fecha"]).normalize()

        if ticker not in holdings:
            holdings[ticker] = {"cantidad": 0.0, "coste_total": 0.0}
        h = holdings[ticker]

        if tipo == "COMPRA":
            h["cantidad"] += cantidad
            h["coste_total"] += (cantidad * precio_eur) + comision_eur
        elif tipo == "VENTA" and h["cantidad"] > 0:
            cantidad_vendida = min(cantidad, h["cantidad"])
            precio_medio_actual = h["coste_total"] / h["cantidad"]
            h["cantidad"] -= cantidad_vendida
            h["coste_total"] -= cantidad_vendida * precio_medio_actual
            if h["cantidad"] <= 0:
                h["cantidad"] = 0.0
                h["coste_total"] = 0.0

        # Tras cada operación, calcular el valor de mercado de toda la cartera
        coste_acumulado = sum(pos["coste_total"] for pos in holdings.values() if pos["cantidad"] > 0)
        valor_hist = 0.0
        for t, pos in holdings.items():
            if pos["cantidad"] > 0:
                p = _get_hist_price_at_date(t, fecha, hist_prices, hist_eurusd)
                if p > 0:
                    valor_hist += pos["cantidad"] * p

        points.append({
            "fecha": fecha,
            "coste_acumulado": coste_acumulado,
            # Si no hay precio histórico disponible, se usa el coste como estimación
            "valor_actual": valor_hist if valor_hist > 0 else coste_acumulado
        })

    chart_df = pd.DataFrame(points)
    if chart_df.empty:
        return None

    # Si hay varias operaciones el mismo día, conservar solo el último estado del día
    chart_df = chart_df.groupby("fecha", as_index=False).last()
    chart_df["fecha_label"] = chart_df["fecha"].dt.strftime("%d/%m/%Y")

    # Con un único punto no tiene sentido trazar líneas; se muestran solo marcadores
    mode = "lines+markers" if len(chart_df) > 1 else "markers"
    title_text = "Evolución de cartera" if len(chart_df) > 1 else "Estado actual de cartera"

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=chart_df["fecha_label"],
        y=chart_df["valor_actual"],
        mode=mode,
        name="Valor estimado",
        line=dict(width=3, color="#e74c3c"),
        marker=dict(size=8)
    ))
    fig.add_trace(go.Scatter(
        x=chart_df["fecha_label"],
        y=chart_df["coste_acumulado"],
        mode=mode,
        name="Coste acumulado",
        line=dict(width=3, color="#4f6ef7"),
        marker=dict(size=8)
    ))

    # Benchmark normalizado: se escala para que en la primera fecha valga lo mismo
    # que el coste acumulado inicial, permitiendo comparar rendimientos en la misma escala
    if benchmark and benchmark in hist_prices:
        bench_series = hist_prices[benchmark]
        if bench_series is not None and not bench_series.empty:
            first_date = chart_df["fecha"].iloc[0]
            b_avail_start = bench_series.index[bench_series.index <= first_date]
            if not b_avail_start.empty:
                bench_start = float(bench_series.loc[b_avail_start[-1]])
                first_coste = chart_df["coste_acumulado"].iloc[0]
                if bench_start > 0 and first_coste > 0:
                    bench_norm = []
                    for fecha_val in chart_df["fecha"]:
                        b_avail = bench_series.index[bench_series.index <= fecha_val]
                        if not b_avail.empty:
                            b_price = float(bench_series.loc[b_avail[-1]])
                            # Normalización: first_coste * (precio_actual / precio_inicial)
                            bench_norm.append(first_coste * (b_price / bench_start))
                        else:
                            bench_norm.append(None)
                    bench_label = (benchmark
                                   .replace("^GSPC", "S&P 500")
                                   .replace("BTC-EUR", "BTC")
                                   .replace("ETH-EUR", "ETH"))
                    fig.add_trace(go.Scatter(
                        x=chart_df["fecha_label"],
                        y=bench_norm,
                        mode=mode,
                        name=f"Benchmark ({bench_label})",
                        line=dict(width=2, color="#f59e0b", dash="dash"),
                        marker=dict(size=6)
                    ))

    fig.update_layout(
        title=title_text,
        template="plotly_white",
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        margin=dict(l=20, r=20, t=60, b=20),
        xaxis=dict(type="category")  # eje X categórico: evita saltos en fines de semana
    )
    fig.update_xaxes(title_text="Fecha")
    fig.update_yaxes(title_text="EUR", tickformat=",.2f")

    # Se exporta como HTML parcial (sin <html>/<body>) para insertarlo en la plantilla
    return pio.to_html(fig, full_html=False, include_plotlyjs="cdn", config={"responsive": True})


# ─────────────────────────────────────────────
# Estadísticas de precio (máx/mín desde compra)
# ─────────────────────────────────────────────

def compute_price_stats(holdings, first_buy_dates):
    """
    Calcula el precio máximo y mínimo alcanzado por cada activo desde su primera
    compra hasta hoy, usando precios de cierre diarios (Close).

    Nota: se usa el precio de cierre, no el High/Low intradiario, lo que puede
    subestimar ligeramente los extremos reales. Para un tracker personal es
    una aproximación suficiente y evita descargar columnas adicionales.

    La conversión USD→EUR del máx/mín usa la tasa EUR/USD más reciente disponible
    (no la tasa histórica del día exacto del extremo). Esto introduce un pequeño
    error si el tipo de cambio ha variado mucho, pero simplifica el cálculo.
    """
    if not holdings or not first_buy_dates:
        return {}

    known = {t: get_yf_info(t) for t in holdings if t in first_buy_dates}
    if not known:
        return {}

    # Descarga desde la primera compra más antigua de todos los activos
    earliest_str = min(first_buy_dates[t] for t in known)
    start_date = pd.to_datetime(earliest_str).normalize()
    end_date = pd.Timestamp.now().normalize() + pd.Timedelta(days=2)

    symbols_needed = set()
    needs_eurusd = False
    for yf_sym, is_usd, _ in known.values():
        symbols_needed.add(yf_sym)
        if is_usd:
            needs_eurusd = True
    if needs_eurusd:
        symbols_needed.add("EURUSD=X")

    # Reutiliza el caché si el gráfico de evolución ya descargó los mismos símbolos
    hist_prices, hist_eurusd = _fetch_and_cache_hist(symbols_needed, start_date, end_date)

    # Tasa actual EUR/USD para la conversión de máx/mín
    eurusd_current = 1.0
    if hist_eurusd is not None and not hist_eurusd.empty:
        eurusd_current = float(hist_eurusd.iloc[-1]) or 1.0

    stats = {}
    for ticker, (yf_sym, is_usd, is_gram) in known.items():
        buy_date = pd.to_datetime(first_buy_dates[ticker]).normalize()
        series = hist_prices.get(yf_sym)
        if series is None or series.empty:
            stats[ticker] = {"max_eur": 0.0, "min_eur": 0.0}
            continue

        # Filtrar solo el período desde la primera compra
        period = series[series.index >= buy_date]
        if period.empty:
            stats[ticker] = {"max_eur": 0.0, "min_eur": 0.0}
            continue

        def to_eur(raw):
            """Convierte precio bruto a EUR/unidad según el tipo de activo."""
            if raw <= 0:
                return 0.0
            val = (raw / eurusd_current) if is_usd else raw
            return val / TROY_OUNCE_IN_GRAMS if is_gram else val

        stats[ticker] = {"max_eur": to_eur(float(period.max())), "min_eur": to_eur(float(period.min()))}

    return stats


# ─────────────────────────────────────────────
# Gráficos de cartera
# ─────────────────────────────────────────────

def build_asset_performance_chart(cartera):
    """
    Gráfico de barras horizontales con el P/L % no realizado de cada activo.
    Solo se incluyen activos con precio de mercado disponible (pnl_pct no None).
    Las barras se ordenan de menor a mayor rendimiento para facilitar la lectura.
    La línea vertical en x=0 actúa como referencia de breakeven.
    """
    items = [c for c in cartera if c["pnl_pct"] is not None]
    if not items:
        return None

    items_sorted = sorted(items, key=lambda c: c["pnl_pct"])
    labels = [c["ticker"] for c in items_sorted]
    values = [c["pnl_pct"] for c in items_sorted]
    colors = ["#e74c3c" if v < 0 else "#27ae60" for v in values]  # rojo: pérdida, verde: ganancia

    fig = go.Figure(go.Bar(
        x=values,
        y=labels,
        orientation="h",
        marker_color=colors,
        text=[f"{v:.2f}%" for v in values],
        textposition="outside",
        hovertemplate="%{y}<br>P/L: %{x:.2f}%<extra></extra>"
    ))
    fig.update_layout(
        title="Rendimiento por activo (P/L %)",
        template="plotly_white",
        margin=dict(l=20, r=80, t=60, b=20),
        xaxis=dict(title="P/L %", ticksuffix="%"),
        yaxis=dict(title=""),
        height=max(200, 60 * len(items_sorted) + 80)  # altura dinámica según nº de activos
    )
    fig.add_vline(x=0, line_width=1, line_color="#94a3b8")  # línea de breakeven
    return pio.to_html(fig, full_html=False, include_plotlyjs="cdn", config={"responsive": True})


def build_portfolio_pie_chart(cartera):
    """
    Donut chart con la distribución porcentual de la cartera por valor actual.
    Solo se incluyen activos con precio de mercado disponible (valor > 0).
    """
    items = [c for c in cartera if c["valor_actual_eur"] > 0]
    if not items:
        return None

    labels = [f"{c['ticker']} – {c['nombre']}" for c in items]
    values = [c["valor_actual_eur"] for c in items]

    fig = go.Figure(go.Pie(
        labels=labels,
        values=values,
        hole=0.45,  # hueco central para convertirlo en donut
        textinfo="label+percent",
        hovertemplate="%{label}<br>%{value:,.2f} €<br>%{percent}<extra></extra>"
    ))
    fig.update_layout(
        title="Distribución de cartera",
        template="plotly_white",
        margin=dict(l=20, r=20, t=60, b=20),
        legend=dict(orientation="v", yanchor="middle", y=0.5, xanchor="left", x=1.02)
    )
    return pio.to_html(fig, full_html=False, include_plotlyjs="cdn", config={"responsive": True})


# ─────────────────────────────────────────────
# Parseo del formulario de operaciones
# ─────────────────────────────────────────────

def parse_operation_form(form):
    """
    Extrae y valida los campos del formulario HTML de nueva/editar operación.
    El ticker se fuerza a mayúsculas para garantizar consistencia en la DB.
    comision_eur es opcional; si se deja vacío se trata como 0.
    Lanza KeyError o ValueError si faltan campos requeridos o hay tipos incorrectos.
    """
    fecha = form["fecha"]
    ticker = form.get("ticker", "").strip().upper()
    nombre = form["nombre"].strip()
    tipo = form["tipo"].strip()
    cantidad = float(form["cantidad"])
    precio_eur = float(form["precio_eur"])
    comision_eur = float(form.get("comision_eur", 0) or 0)
    estrategia = form.get("estrategia", "").strip()
    notas = form.get("notas", "").strip()
    return fecha, ticker, nombre, tipo, cantidad, precio_eur, comision_eur, estrategia, notas


# ─────────────────────────────────────────────
# Rutas Flask
# ─────────────────────────────────────────────

@app.route("/")
def index():
    """
    Página de inicio: muestra KPIs globales, las 5 últimas operaciones
    y el gráfico de evolución de cartera con benchmark seleccionable.
    """
    conn = get_db_connection()
    operaciones = conn.execute(
        "SELECT * FROM operaciones ORDER BY fecha DESC, id DESC LIMIT 5"
    ).fetchall()
    all_rows = conn.execute(
        "SELECT * FROM operaciones ORDER BY fecha ASC, id ASC"
    ).fetchall()
    total_operaciones = conn.execute(
        "SELECT COUNT(*) AS total FROM operaciones"
    ).fetchone()["total"]
    conn.close()

    analysis = analyze_operations(all_rows)
    holdings = analysis["holdings"]

    # Pasar los tickers actuales para que el caché de precios sea exacto
    portfolio_tickers = list(holdings.keys()) if holdings else None
    market_prices, cache_timestamp = get_cached_market_prices(portfolio_tickers=portfolio_tickers)
    last_update_text = format_cache_time(cache_timestamp)

    activos_en_cartera = len(holdings)
    coste_total_cartera = 0.0
    valor_actual_total = 0.0

    for ticker, h in holdings.items():
        coste_total_cartera += h["coste_total"]
        precio_actual = market_prices.get(ticker, 0.0)
        if precio_actual > 0:
            valor_actual_total += h["cantidad"] * precio_actual

    pnl_total = valor_actual_total - coste_total_cartera
    pnl_pct = (pnl_total / coste_total_cartera * 100) if coste_total_cartera > 0 else 0.0

    # El benchmark se pasa como clave URL (?benchmark=BTC) y se resuelve al símbolo yfinance
    benchmark_key = request.args.get("benchmark", "BTC")
    benchmark_sym = BENCHMARK_OPTIONS.get(benchmark_key, "BTC-EUR")
    portfolio_chart = build_portfolio_evolution_chart(all_rows, benchmark=benchmark_sym)

    return render_template(
        "index.html",
        operaciones=operaciones,
        total_operaciones=total_operaciones,
        activos_en_cartera=activos_en_cartera,
        coste_total_cartera=coste_total_cartera,
        valor_actual_total=valor_actual_total,
        pnl_total=pnl_total,
        pnl_pct=pnl_pct,
        last_update_text=last_update_text,
        portfolio_chart=portfolio_chart,
        benchmark_key=benchmark_key,
    )


@app.route("/operaciones")
def operaciones():
    """
    Lista paginada de operaciones con filtros por texto (nombre/ticker) y tipo.
    La paginación usa LIMIT/OFFSET en SQL para no cargar toda la tabla en memoria.
    """
    today = datetime.now().strftime("%Y-%m-%d")  # pre-rellena la fecha en el formulario
    q = request.args.get("q", "").strip()
    tipo = request.args.get("tipo", "").strip()
    try:
        page = max(1, int(request.args.get("page", 1) or 1))
    except ValueError:
        page = 1

    conn = get_db_connection()

    # Construcción dinámica de la cláusula WHERE para evitar SQL injection con parámetros
    sql_base = "FROM operaciones WHERE 1=1"
    params = []

    if q:
        sql_base += " AND (nombre LIKE ? OR ticker LIKE ?)"
        like_q = f"%{q}%"
        params.extend([like_q, like_q])

    if tipo in ["Compra", "Venta"]:
        sql_base += " AND tipo = ?"
        params.append(tipo)

    total_count = conn.execute(f"SELECT COUNT(*) AS n {sql_base}", params).fetchone()["n"]
    total_pages = max(1, (total_count + OPS_PER_PAGE - 1) // OPS_PER_PAGE)
    page = min(page, total_pages)  # corregir si la página pedida supera el máximo
    offset = (page - 1) * OPS_PER_PAGE

    ops = conn.execute(
        f"SELECT * {sql_base} ORDER BY fecha DESC, id DESC LIMIT ? OFFSET ?",
        params + [OPS_PER_PAGE, offset]
    ).fetchall()

    # Lista de tickers únicos para el autocompletado del formulario de nueva operación
    tickers_existentes = [
        row["ticker"] for row in
        conn.execute(
            "SELECT DISTINCT ticker FROM operaciones WHERE ticker IS NOT NULL AND ticker != '' ORDER BY ticker"
        ).fetchall()
    ]

    conn.close()

    return render_template(
        "operaciones.html",
        operaciones=ops,
        q=q,
        tipo=tipo,
        today=today,
        page=page,
        total_pages=total_pages,
        total_count=total_count,
        tickers_existentes=tickers_existentes
    )


@app.route("/agregar-operacion", methods=["POST"])
def agregar_operacion():
    """Inserta una nueva operación en la base de datos."""
    try:
        fecha, ticker, nombre, tipo, cantidad, precio_eur, comision_eur, estrategia, notas = parse_operation_form(request.form)
    except (KeyError, ValueError):
        flash("Datos inválidos. Revisa los campos del formulario.", "error")
        return redirect(url_for("operaciones"))

    conn = get_db_connection()
    conn.execute("""
        INSERT INTO operaciones (fecha, ticker, nombre, tipo, cantidad, precio_eur, comision_eur, estrategia, notas)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (fecha, ticker, nombre, tipo, cantidad, precio_eur, comision_eur, estrategia, notas))
    conn.commit()
    conn.close()
    return redirect(url_for("operaciones"))


@app.route("/editar-operacion/<int:id>")
def editar_operacion(id):
    """Muestra el formulario de edición con los datos actuales de la operación."""
    conn = get_db_connection()
    op = conn.execute("SELECT * FROM operaciones WHERE id = ?", (id,)).fetchone()
    conn.close()
    if op is None:
        return redirect(url_for("operaciones"))
    return render_template("editar_operacion.html", op=op)


@app.route("/actualizar-operacion/<int:id>", methods=["POST"])
def actualizar_operacion(id):
    """Actualiza los campos de una operación existente."""
    try:
        fecha, ticker, nombre, tipo, cantidad, precio_eur, comision_eur, estrategia, notas = parse_operation_form(request.form)
    except (KeyError, ValueError):
        flash("Datos inválidos. Revisa los campos del formulario.", "error")
        return redirect(url_for("operaciones"))

    conn = get_db_connection()
    conn.execute("""
        UPDATE operaciones
        SET fecha=?, ticker=?, nombre=?, tipo=?, cantidad=?, precio_eur=?,
            comision_eur=?, estrategia=?, notas=?
        WHERE id=?
    """, (fecha, ticker, nombre, tipo, cantidad, precio_eur, comision_eur, estrategia, notas, id))
    conn.commit()
    conn.close()
    return redirect(url_for("operaciones"))


@app.route("/borrar-operacion/<int:id>", methods=["POST"])
def borrar_operacion(id):
    """Elimina una operación por su ID. La confirmación se hace en el cliente (JS confirm)."""
    conn = get_db_connection()
    conn.execute("DELETE FROM operaciones WHERE id = ?", (id,))
    conn.commit()
    conn.close()
    return redirect(url_for("operaciones"))


@app.route("/cartera")
def cartera():
    """
    Vista de cartera: posiciones abiertas, P/L, gráficos de distribución y rendimiento,
    historial de ventas y resumen fiscal por año.

    El flujo ?refresh=1 fuerza la actualización del caché de precios sin recalcular
    los demás datos; después redirige a la misma página sin el parámetro.
    """
    force_refresh = request.args.get("refresh") == "1"

    conn = get_db_connection()
    all_rows = conn.execute(
        "SELECT * FROM operaciones ORDER BY fecha ASC, id ASC"
    ).fetchall()
    conn.close()

    analysis = analyze_operations(all_rows)
    holdings = analysis["holdings"]
    sale_history = analysis["sale_history"]
    summary = analysis["summary"]
    ticker_commissions = analysis["ticker_commissions"]
    first_buy_dates = analysis["first_buy_dates"]

    portfolio_tickers = list(holdings.keys()) if holdings else None

    if force_refresh:
        # Actualizar el caché de precios y redirigir para limpiar la URL
        get_cached_market_prices(force_refresh=True, portfolio_tickers=portfolio_tickers)
        return redirect(url_for("cartera"))

    market_prices, cache_timestamp = get_cached_market_prices(portfolio_tickers=portfolio_tickers)
    last_update_text = format_cache_time(cache_timestamp)

    # Descargar histórico para calcular máx/mín desde primera compra
    price_stats = compute_price_stats(holdings, first_buy_dates) if holdings else {}

    cartera_list = []
    coste_total_abierto = 0.0
    valor_actual_total = 0.0

    for ticker, h in sorted(holdings.items(), key=lambda x: x[1]["nombre"].lower()):
        posicion = h["cantidad"]
        coste_total = h["coste_total"]
        precio_medio = h["precio_medio"]
        precio_actual = market_prices.get(ticker, 0.0)
        # Si no hay precio de mercado, valor y P/L quedan en 0 para no mostrar datos falsos
        valor_actual = posicion * precio_actual if precio_actual > 0 else 0.0
        pnl = valor_actual - coste_total if precio_actual > 0 else 0.0

        coste_total_abierto += coste_total
        valor_actual_total += valor_actual

        # Unidad de precio que se mostrará junto al precio en la tabla
        if ticker in ("ORO", "PLATA"):
            unidad_precio = "EUR/g"
        elif ticker in ("XAU", "XAG"):
            unidad_precio = "EUR/oz"
        else:
            unidad_precio = "EUR"

        # pnl_pct es None cuando no hay precio de mercado, para distinguirlo de un 0%
        pnl_pct = (pnl / coste_total * 100) if coste_total > 0 and precio_actual > 0 else None
        stats = price_stats.get(ticker, {"max_eur": 0.0, "min_eur": 0.0})

        cartera_list.append({
            "ticker": ticker,
            "nombre": h["nombre"],
            "posicion_actual": posicion,
            "precio_medio_compra_eur": precio_medio,
            "coste_total_eur": coste_total,
            "precio_actual_eur": precio_actual,
            "valor_actual_eur": valor_actual,
            "pnl_eur": pnl,
            "pnl_pct": pnl_pct,
            "unidad_precio": unidad_precio,
            "max_precio_eur": stats["max_eur"],   # precio máximo de cierre desde la primera compra
            "min_precio_eur": stats["min_eur"],   # precio mínimo de cierre desde la primera compra
            "comisiones_eur": ticker_commissions.get(ticker, 0.0),  # suma de todas las comisiones del ticker
        })

    resultado_no_realizado = valor_actual_total - coste_total_abierto
    # Resultado total = lo ya materializado en ventas + la ganancia latente de lo que queda abierto
    resultado_total = summary["resultado_realizado_eur"] + resultado_no_realizado

    resumen_cartera = {
        "total_invertido_eur": summary["total_invertido_eur"],
        "total_vendido_eur": summary["total_vendido_eur"],
        "resultado_realizado_eur": summary["resultado_realizado_eur"],
        "resultado_no_realizado_eur": resultado_no_realizado,
        "resultado_total_eur": resultado_total,
        "coste_abierto_eur": coste_total_abierto,
        "valor_actual_total_eur": valor_actual_total,
        "comisiones_compra_eur": summary["comisiones_compra_eur"],
        "comisiones_venta_eur": summary["comisiones_venta_eur"],
    }

    pie_chart = build_portfolio_pie_chart(cartera_list)
    performance_chart = build_asset_performance_chart(cartera_list)
    fiscal_summary = compute_fiscal_summary(sale_history)

    return render_template(
        "cartera.html",
        cartera=cartera_list,
        resumen_cartera=resumen_cartera,
        historial_ventas=sale_history,
        last_update_text=last_update_text,
        pie_chart=pie_chart,
        performance_chart=performance_chart,
        fiscal_summary=fiscal_summary,
    )


@app.route("/exportar-csv")
def exportar_csv():
    """
    Exporta las operaciones visibles (con los mismos filtros que la vista de historial)
    como archivo CSV descargable, generado en memoria con StringIO.
    """
    q = request.args.get("q", "").strip()
    tipo = request.args.get("tipo", "").strip()

    conn = get_db_connection()
    sql = "SELECT * FROM operaciones WHERE 1=1"
    params = []
    if q:
        sql += " AND (nombre LIKE ? OR ticker LIKE ?)"
        like_q = f"%{q}%"
        params.extend([like_q, like_q])
    if tipo in ["Compra", "Venta"]:
        sql += " AND tipo = ?"
        params.append(tipo)
    sql += " ORDER BY fecha DESC, id DESC"
    ops = conn.execute(sql, params).fetchall()
    conn.close()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["id", "fecha", "ticker", "nombre", "tipo", "cantidad", "precio_eur", "comision_eur", "estrategia", "notas"])
    for op in ops:
        writer.writerow([
            op["id"], op["fecha"], op["ticker"] or "",
            op["nombre"], op["tipo"], op["cantidad"],
            op["precio_eur"], op["comision_eur"],
            op["estrategia"] or "", op["notas"] or ""
        ])

    fecha_hoy = datetime.now().strftime("%Y-%m-%d")
    filename = f"operaciones_{fecha_hoy}.csv"
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


@app.route("/importar-csv", methods=["POST"])
def importar_csv():
    """
    Importa operaciones desde un CSV subido por el usuario.
    Se decodifica con utf-8-sig para manejar el BOM que añade Excel al exportar.
    Las filas con datos inválidos se cuentan y se informa al usuario, pero no
    abortan la importación del resto.
    """
    file = request.files.get("archivo_csv")
    if not file or not file.filename.lower().endswith(".csv"):
        flash("Selecciona un archivo CSV válido.", "error")
        return redirect(url_for("operaciones"))

    try:
        stream = io.StringIO(file.stream.read().decode("utf-8-sig"))
        reader = csv.DictReader(stream)
        required = {"fecha", "nombre", "tipo", "cantidad", "precio_eur"}
        fieldnames = set(reader.fieldnames or [])
        if not required.issubset(fieldnames):
            missing = required - fieldnames
            flash(f"El CSV no tiene las columnas requeridas: {', '.join(sorted(missing))}.", "error")
            return redirect(url_for("operaciones"))

        inserted = 0
        errors = 0
        conn = get_db_connection()
        for row in reader:
            try:
                conn.execute("""
                    INSERT INTO operaciones
                        (fecha, ticker, nombre, tipo, cantidad, precio_eur, comision_eur, estrategia, notas)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    row["fecha"],
                    row.get("ticker", "").strip().upper(),
                    row["nombre"].strip(),
                    row["tipo"].strip(),
                    float(row["cantidad"]),
                    float(row["precio_eur"]),
                    float(row.get("comision_eur", 0) or 0),
                    row.get("estrategia", "").strip(),
                    row.get("notas", "").strip()
                ))
                inserted += 1
            except (ValueError, KeyError):
                errors += 1
        conn.commit()
        conn.close()

        msg = f"Importadas {inserted} operaciones correctamente."
        if errors:
            msg += f" {errors} filas ignoradas por datos inválidos."
        flash(msg, "success")
    except Exception as e:
        flash(f"Error al procesar el archivo: {e}", "error")

    return redirect(url_for("operaciones"))


# ─────────────────────────────────────────────
# Arranque
# ─────────────────────────────────────────────

# Crear la tabla al importar el módulo (tanto en producción como en tests)
init_db()

if __name__ == "__main__":
    app.run(debug=True)
