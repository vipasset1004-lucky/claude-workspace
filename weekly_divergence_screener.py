"""
주봉 상승 다이버전스 스크리너 (Weekly Bullish Divergence Screener)
- 주가는 하락/횡보하는데 기술적 지표가 상승하는 종목을 탐지
- 지표: Stochastic %K, RSI, MACD Histogram, OBV
"""

import yfinance as yf
import pandas as pd
import numpy as np
from ta.momentum import StochRSIIndicator, RSIIndicator, StochasticOscillator
from ta.trend import MACD
from ta.volume import OnBalanceVolumeIndicator
from datetime import datetime, timedelta
import json
import sys
import os
import warnings
import time
import requests

warnings.filterwarnings("ignore")


# ── 설정 ──────────────────────────────────────────────
LOOKBACK_WEEKS = 12       # 다이버전스 탐지 기간 (주)
MIN_DIVERGENCE_WEEKS = 3  # 최소 다이버전스 지속 기간
PRICE_THRESHOLD = -0.02   # 주가 변화율 기준 (횡보/하락 = -2% 이하)


def get_krx_tickers(market="ALL", top_n=300):
    """한국 주식 종목 코드 수집 (KRX 공시 + yfinance 시가총액 정렬)"""
    try:
        # 1단계: KRX에서 전체 종목 리스트 가져오기
        print("  KRX 종목 리스트 다운로드 중...")
        url = "http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13"
        tables = pd.read_html(url, encoding="euc-kr")
        df = tables[0]

        # 종목코드 6자리 포맷 + 순수 숫자만 (채권/펀드 제외)
        df["종목코드"] = df["종목코드"].astype(str).str.zfill(6)
        df = df[df["종목코드"].str.match(r"^\d{6}$")]
        df = df[~df["종목코드"].str.contains("[A-Za-z]")]

        print(f"  전체 종목: {len(df)}개")

        # 2단계: yfinance로 시가총액 기준 상위 종목 추출
        print("  시가총액 기준 정렬 중...")
        cap_data = []
        checked = 0

        for _, row in df.iterrows():
            ticker = row["종목코드"]
            name = row["회사명"]

            # yfinance에서 시가총액 확인
            for suffix in [".KS", ".KQ"]:
                try:
                    info = yf.Ticker(f"{ticker}{suffix}")
                    hist = info.history(period="5d")
                    if hist.empty:
                        continue

                    # 시가총액 추정 (최근 종가 * 발행주식수는 비용이 크므로, 거래대금으로 대체)
                    avg_volume = hist["Volume"].mean()
                    last_price = hist["Close"].iloc[-1]
                    trade_value = avg_volume * last_price  # 일평균 거래대금

                    mkt = "KOSPI" if suffix == ".KS" else "KOSDAQ"
                    cap_data.append({
                        "ticker": ticker,
                        "name": name,
                        "suffix": suffix,
                        "market": mkt,
                        "trade_value": trade_value,
                        "price": last_price
                    })
                    break
                except:
                    continue

            checked += 1
            if checked % 50 == 0:
                print(f"  확인: {checked}개... (유효: {len(cap_data)}개)")

            # 충분한 후보를 모으면 중단 (top_n의 2배)
            if len(cap_data) >= top_n * 2:
                break

            time.sleep(0.15)

        # 거래대금 기준 내림차순 정렬 (활발한 종목 우선)
        cap_data.sort(key=lambda x: x["trade_value"], reverse=True)

        # 시장 필터
        if market == "KOSPI":
            cap_data = [x for x in cap_data if x["market"] == "KOSPI"]
        elif market == "KOSDAQ":
            cap_data = [x for x in cap_data if x["market"] == "KOSDAQ"]

        result = [{"ticker": x["ticker"], "name": x["name"]} for x in cap_data[:top_n]]
        print(f"  → 최종 {len(result)}개 종목 선정 완료")
        return result

    except Exception as e:
        print(f"[ERROR] KRX 종목 수집 실패: {e}")
        print("  → 주요 종목 기본 리스트로 대체합니다.")
        return get_fallback_tickers(top_n)


def get_fallback_tickers(top_n=100):
    """KRX 수집 실패 시 주요 종목 기본 리스트"""
    major = [
        ("005930", "삼성전자"), ("000660", "SK하이닉스"), ("373220", "LG에너지솔루션"),
        ("035420", "NAVER"), ("005380", "현대차"), ("000270", "기아"),
        ("006400", "삼성SDI"), ("035720", "카카오"), ("051910", "LG화학"),
        ("028260", "삼성물산"), ("105560", "KB금융"), ("055550", "신한지주"),
        ("066570", "LG전자"), ("012330", "현대모비스"), ("003670", "포스코퓨처엠"),
        ("096770", "SK이노베이션"), ("034730", "SK"), ("003550", "LG"),
        ("015760", "한국전력"), ("032830", "삼성생명"), ("009150", "삼성전기"),
        ("086790", "하나금융지주"), ("010130", "고려아연"), ("011200", "HMM"),
        ("033780", "KT&G"), ("009540", "한국조선해양"), ("000810", "삼성화재"),
        ("034020", "두산에너빌리티"), ("003490", "대한항공"), ("018260", "삼성에스디에스"),
        ("259960", "크래프톤"), ("352820", "하이브"), ("293490", "카카오게임즈"),
        ("263750", "펄어비스"), ("112040", "위메이드"), ("036570", "엔씨소프트"),
        ("251270", "넷마블"), ("377300", "카카오페이"), ("403870", "토스"),
        ("247540", "에코프로비엠"), ("086520", "에코프로"), ("006280", "녹십자"),
        ("068270", "셀트리온"), ("207940", "삼성바이오로직스"), ("091990", "셀트리온헬스케어"),
        ("196170", "알테오젠"), ("326030", "SK바이오팜"), ("145020", "휴젤"),
        ("005490", "POSCO홀딩스"), ("010950", "S-Oil"), ("036460", "한국가스공사"),
        ("017670", "SK텔레콤"), ("030200", "KT"), ("316140", "우리금융지주"),
        ("138930", "BNK금융지주"), ("024110", "기업은행"), ("021240", "코웨이"),
        ("180640", "한진칼"), ("161390", "한국타이어앤테크놀로지"), ("004020", "현대제철"),
        ("011170", "롯데케미칼"), ("010140", "삼성중공업"), ("009830", "한화솔루션"),
        ("042700", "한미반도체"), ("041510", "에스엠"), ("122870", "와이지엔터테인먼트"),
        ("047810", "한국항공우주"), ("012450", "한화에어로스페이스"), ("298050", "효성첨단소재"),
        ("302440", "SK바이오사이언스"), ("323410", "카카오뱅크"), ("000100", "유한양행"),
        ("128940", "한미약품"), ("004170", "신세계"), ("023530", "롯데쇼핑"),
        ("030000", "제일기획"), ("002790", "아모레퍼시픽"), ("090430", "아모레G"),
        ("097950", "CJ제일제당"), ("051900", "LG생활건강"),
    ]
    return [{"ticker": t, "name": n} for t, n in major[:top_n]]


def fetch_weekly_data(ticker_code, is_korean=True, weeks=52):
    """주봉 데이터 수집"""
    try:
        if is_korean:
            symbol = f"{ticker_code}.KS"
            data = yf.download(symbol, period=f"{weeks}wk", interval="1wk",
                               progress=False, timeout=10)
            if data.empty:
                symbol = f"{ticker_code}.KQ"
                data = yf.download(symbol, period=f"{weeks}wk", interval="1wk",
                                   progress=False, timeout=10)
        else:
            data = yf.download(ticker_code, period=f"{weeks}wk", interval="1wk",
                               progress=False, timeout=10)

        if data.empty:
            return None

        # MultiIndex columns 처리
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)

        return data
    except:
        return None


def calculate_indicators(df):
    """기술적 지표 계산"""
    if df is None or len(df) < 20:
        return None

    result = df.copy()

    # Stochastic Oscillator (%K, %D)
    stoch = StochasticOscillator(
        high=result["High"], low=result["Low"], close=result["Close"],
        window=14, smooth_window=3
    )
    result["Stoch_K"] = stoch.stoch()
    result["Stoch_D"] = stoch.stoch_signal()

    # RSI (14)
    rsi = RSIIndicator(close=result["Close"], window=14)
    result["RSI"] = rsi.rsi()

    # MACD
    macd = MACD(close=result["Close"])
    result["MACD"] = macd.macd()
    result["MACD_Signal"] = macd.macd_signal()
    result["MACD_Hist"] = macd.macd_diff()

    # OBV
    obv = OnBalanceVolumeIndicator(close=result["Close"], volume=result["Volume"])
    result["OBV"] = obv.on_balance_volume()

    return result.dropna()


def find_local_lows(series, window=3):
    """로컬 저점 찾기"""
    lows = []
    values = series.values
    for i in range(window, len(values) - window):
        if values[i] == min(values[i - window:i + window + 1]):
            lows.append((i, values[i]))
    return lows


def detect_bullish_divergence(df, lookback=LOOKBACK_WEEKS):
    """상승 다이버전스 탐지 - 여러 지표 종합"""
    if df is None or len(df) < lookback:
        return None

    recent = df.tail(lookback).copy()
    recent = recent.reset_index(drop=True)

    divergences = {}
    score = 0

    # 주가 저점 찾기
    price_lows = find_local_lows(recent["Close"], window=2)

    if len(price_lows) < 2:
        return None

    last_two_price_lows = price_lows[-2:]
    p1_idx, p1_val = last_two_price_lows[0]
    p2_idx, p2_val = last_two_price_lows[1]

    # 주가가 하락 또는 횡보 (두 번째 저점 <= 첫 번째 저점)
    price_declining = p2_val <= p1_val * (1 + PRICE_THRESHOLD)

    if not price_declining:
        return None

    price_change_pct = ((p2_val - p1_val) / p1_val) * 100

    # 각 지표별 다이버전스 확인
    indicators = {
        "Stochastic %K": "Stoch_K",
        "RSI": "RSI",
        "MACD Histogram": "MACD_Hist",
        "OBV": "OBV"
    }

    for name, col in indicators.items():
        if col not in recent.columns:
            continue

        # 지표의 저점 비교 (주가 저점과 동일 인덱스 근처)
        range_size = 2
        ind_val1 = recent[col].iloc[max(0, p1_idx - range_size):p1_idx + range_size + 1].min()
        ind_val2 = recent[col].iloc[max(0, p2_idx - range_size):p2_idx + range_size + 1].min()

        # 지표는 상승 (두 번째 저점 > 첫 번째 저점)
        if ind_val2 > ind_val1:
            divergences[name] = {
                "prev_low": round(float(ind_val1), 2),
                "curr_low": round(float(ind_val2), 2),
                "change": round(float(ind_val2 - ind_val1), 2)
            }
            score += 1

    if score == 0:
        return None

    # 추가 점수: 현재 위치 분석
    last_row = df.iloc[-1]
    bonus_signals = []

    # RSI 과매도 근처에서 반등
    if "RSI" in recent.columns and last_row["RSI"] < 40:
        bonus_signals.append("RSI 과매도 근처")
        score += 0.5

    # Stochastic 과매도 반등
    if "Stoch_K" in recent.columns and last_row["Stoch_K"] < 30:
        bonus_signals.append("Stochastic 과매도")
        score += 0.5

    # MACD 히스토그램 반전
    if "MACD_Hist" in recent.columns:
        hist_vals = recent["MACD_Hist"].tail(3).values
        if len(hist_vals) == 3 and hist_vals[-1] > hist_vals[-2]:
            bonus_signals.append("MACD 히스토그램 반전")
            score += 0.5

    # 거래량 증가 추세
    if "Volume" in recent.columns:
        vol_recent = recent["Volume"].tail(3).mean()
        vol_prev = recent["Volume"].tail(8).head(5).mean()
        if vol_prev > 0 and vol_recent > vol_prev * 1.2:
            bonus_signals.append("거래량 증가")
            score += 0.5

    return {
        "divergence_count": len(divergences),
        "divergences": divergences,
        "price_change_pct": round(price_change_pct, 2),
        "score": round(score, 1),
        "bonus_signals": bonus_signals,
        "current_price": round(float(last_row["Close"]), 0),
        "current_rsi": round(float(last_row.get("RSI", 0)), 1),
        "current_stoch": round(float(last_row.get("Stoch_K", 0)), 1),
        "current_macd_hist": round(float(last_row.get("MACD_Hist", 0)), 2),
    }


def screen_korean_stocks(market="ALL", top_n=200):
    """한국 주식 스크리닝"""
    print(f"\n{'='*60}")
    print(f"  주봉 상승 다이버전스 스크리너")
    print(f"  시장: {market} | 스캔 대상: 상위 {top_n}개 종목")
    print(f"  날짜: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}\n")

    # 종목 수집
    print("[1/3] 종목 리스트 수집 중...")
    tickers = get_krx_tickers(market, top_n)
    if not tickers:
        print("[ERROR] 종목 수집 실패")
        return []

    print(f"  → {len(tickers)}개 종목 로드 완료\n")

    # 스크리닝
    print(f"[2/3] 주봉 데이터 분석 중...")
    results = []
    total = len(tickers)

    for i, t in enumerate(tickers):
        ticker = t["ticker"]
        name = t["name"]

        if (i + 1) % 20 == 0 or i == 0:
            print(f"  진행: {i+1}/{total} ({name}...)")

        # 데이터 수집
        df = fetch_weekly_data(ticker, is_korean=True)
        if df is None:
            continue

        # 지표 계산
        df = calculate_indicators(df)
        if df is None:
            continue

        # 다이버전스 탐지
        div = detect_bullish_divergence(df)
        if div and div["score"] >= 1.5:
            results.append({
                "ticker": ticker,
                "name": name,
                **div
            })

        # API 부하 방지
        time.sleep(0.3)

    # 점수순 정렬
    results.sort(key=lambda x: x["score"], reverse=True)

    # 결과 출력
    print(f"\n[3/3] 결과 출력")
    print(f"{'='*60}")
    print(f"  발견된 다이버전스 종목: {len(results)}개")
    print(f"{'='*60}\n")

    for rank, r in enumerate(results, 1):
        stars = "★" * int(r["score"]) + "☆" * (4 - int(r["score"]))
        print(f"  [{rank}] {r['name']} ({r['ticker']})  신호강도: {stars} ({r['score']}점)")
        print(f"      현재가: {r['current_price']:,.0f}원 | 주가변화: {r['price_change_pct']:+.1f}%")
        print(f"      RSI: {r['current_rsi']} | Stoch: {r['current_stoch']} | MACD Hist: {r['current_macd_hist']}")
        print(f"      다이버전스: ", end="")
        for d_name in r["divergences"]:
            print(f"[{d_name}] ", end="")
        print()
        if r["bonus_signals"]:
            print(f"      추가신호: {', '.join(r['bonus_signals'])}")
        print()

    return results


def screen_us_stocks(watchlist=None):
    """미국 주식 스크리닝 (워치리스트 기반)"""
    if watchlist is None:
        # 기본 워치리스트: S&P500 주요 종목 + 성장주
        watchlist = [
            "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA",
            "AMD", "PLTR", "SOFI", "COIN", "MARA", "RIOT", "SQ", "SHOP",
            "NET", "SNOW", "DDOG", "CRWD", "ZS", "PANW", "ABNB",
            "UBER", "RBLX", "U", "HOOD", "AFRM", "UPST", "IONQ",
            "SMCI", "ARM", "AVGO", "MU", "MRVL", "QCOM", "INTC",
        ]

    print(f"\n{'='*60}")
    print(f"  주봉 상승 다이버전스 스크리너 (US)")
    print(f"  스캔 대상: {len(watchlist)}개 종목")
    print(f"  날짜: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}\n")

    results = []
    total = len(watchlist)

    for i, ticker in enumerate(watchlist):
        if (i + 1) % 10 == 0 or i == 0:
            print(f"  진행: {i+1}/{total} ({ticker}...)")

        df = fetch_weekly_data(ticker, is_korean=False)
        if df is None:
            continue

        df = calculate_indicators(df)
        if df is None:
            continue

        div = detect_bullish_divergence(df)
        if div and div["score"] >= 1.5:
            results.append({
                "ticker": ticker,
                "name": ticker,
                **div
            })

        time.sleep(0.3)

    results.sort(key=lambda x: x["score"], reverse=True)

    print(f"\n{'='*60}")
    print(f"  발견된 다이버전스 종목: {len(results)}개")
    print(f"{'='*60}\n")

    for rank, r in enumerate(results, 1):
        stars = "★" * int(r["score"]) + "☆" * (4 - int(r["score"]))
        print(f"  [{rank}] {r['name']}  신호강도: {stars} ({r['score']}점)")
        print(f"      현재가: ${r['current_price']:,.2f} | 주가변화: {r['price_change_pct']:+.1f}%")
        print(f"      RSI: {r['current_rsi']} | Stoch: {r['current_stoch']} | MACD Hist: {r['current_macd_hist']}")
        print(f"      다이버전스: ", end="")
        for d_name in r["divergences"]:
            print(f"[{d_name}] ", end="")
        print()
        if r["bonus_signals"]:
            print(f"      추가신호: {', '.join(r['bonus_signals'])}")
        print()

    return results


def export_json(results, filename="divergence_results.json"):
    """결과를 JSON으로 저장"""
    output = {
        "scan_date": datetime.now().isoformat(),
        "total_found": len(results),
        "results": results
    }
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"\n  → 결과 저장: {filename}")


def export_html(results, base_dir="D:/클로드 작업 공간"):
    """결과를 HTML 대시보드에 임베드하여 저장 (로컬 파일로 바로 열기 가능)"""
    template_path = os.path.join(base_dir, "divergence_dashboard.html")
    output_path = os.path.join(base_dir, "divergence_report.html")

    data = {
        "scan_date": datetime.now().isoformat(),
        "total_found": len(results),
        "results": results
    }
    json_str = json.dumps(data, ensure_ascii=False)

    with open(template_path, "r", encoding="utf-8") as f:
        html = f.read()

    # __EMBED_MARKER__ 라인을 실제 데이터로 교체
    html = html.replace(
        "const EMBEDDED_DATA = null; // __EMBED_MARKER__",
        f"const EMBEDDED_DATA = {json_str};"
    )

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"  → HTML 리포트 저장: {output_path}")
    print(f"     파일을 더블클릭하면 브라우저에서 바로 확인 가능!")


if __name__ == "__main__":

    market = sys.argv[1] if len(sys.argv) > 1 else "KR"
    top_n = int(sys.argv[2]) if len(sys.argv) > 2 else 100

    if market.upper() in ["KR", "KOSPI", "KOSDAQ", "ALL"]:
        m = "ALL" if market.upper() == "KR" else market.upper()
        results = screen_korean_stocks(market=m, top_n=top_n)
    elif market.upper() == "US":
        results = screen_us_stocks()
    else:
        print(f"사용법: python weekly_divergence_screener.py [KR|US|KOSPI|KOSDAQ] [종목수]")
        sys.exit(1)

    if results:
        base = "D:/클로드 작업 공간"
        export_json(results, os.path.join(base, "divergence_results.json"))
        export_html(results, base)

    print("\n  본 결과는 기술적 분석 참고용이며, 투자 판단은 본인 책임입니다.")
