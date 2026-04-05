"""
실시간 다이버전스 스크리너 웹 서버
- Flask + SSE로 브라우저에서 실시간 스캔
- /api/scan → 스캔 실행 + 실시간 결과 스트리밍
"""

from flask import Flask, Response, request, send_file, jsonify
from weekly_divergence_screener import (
    get_krx_tickers, get_fallback_tickers, fetch_weekly_data,
    calculate_indicators, detect_bullish_divergence
)
import json
import time
import threading
from datetime import datetime

app = Flask(__name__)

# 스캔 상태 관리
scan_state = {
    "running": False,
    "last_results": None,
    "last_scan_date": None,
}


@app.route("/")
def index():
    return send_file("divergence_dashboard.html")


@app.route("/api/results")
def get_results():
    if scan_state["last_results"]:
        return jsonify({
            "scan_date": scan_state["last_scan_date"],
            "total_found": len(scan_state["last_results"]),
            "results": scan_state["last_results"]
        })
    return jsonify({"error": "no data", "results": []})


@app.route("/api/scan")
def scan():
    market = request.args.get("market", "KR")
    top_n = int(request.args.get("top_n", "100"))
    min_score = float(request.args.get("min_score", "1.0"))

    if scan_state["running"]:
        def already_running():
            yield f"data: {json.dumps({'type': 'error', 'message': '이미 스캔이 진행 중입니다'})}\n\n"
        return Response(already_running(), mimetype="text/event-stream")

    def generate():
        scan_state["running"] = True
        results = []
        try:
            # 1단계: 종목 리스트 수집
            yield f"data: {json.dumps({'type': 'status', 'message': '종목 리스트 수집 중...', 'progress': 0}, ensure_ascii=False)}\n\n"

            if market.upper() == "US":
                tickers = [
                    {"ticker": t, "name": t} for t in [
                        "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA",
                        "AMD", "PLTR", "SOFI", "COIN", "MARA", "RIOT", "SHOP",
                        "NET", "SNOW", "DDOG", "CRWD", "ZS", "PANW", "ABNB",
                        "UBER", "RBLX", "U", "HOOD", "AFRM", "UPST", "IONQ",
                        "SMCI", "ARM", "AVGO", "MU", "MRVL", "QCOM", "INTC",
                    ]
                ]
                is_korean = False
            else:
                # fallback 리스트를 바로 사용 (KRX 수집은 너무 느림)
                tickers = get_fallback_tickers(top_n)
                is_korean = True

            total = len(tickers)
            yield f"data: {json.dumps({'type': 'status', 'message': f'{total}개 종목 분석 시작', 'progress': 5, 'total': total}, ensure_ascii=False)}\n\n"

            # 2단계: 종목별 스캔
            for i, t in enumerate(tickers):
                ticker = t["ticker"]
                name = t["name"]
                progress = int(5 + (i / total) * 90)

                # 진행 상황 전송
                if i % 5 == 0 or i == total - 1:
                    yield f"data: {json.dumps({'type': 'progress', 'current': name, 'index': i+1, 'total': total, 'progress': progress, 'found': len(results)}, ensure_ascii=False)}\n\n"

                # 데이터 수집 + 분석
                df = fetch_weekly_data(ticker, is_korean=is_korean)
                if df is None:
                    continue

                df = calculate_indicators(df)
                if df is None:
                    continue

                div = detect_bullish_divergence(df)
                if div and div["score"] >= min_score:
                    result = {"ticker": ticker, "name": name, **div}
                    results.append(result)

                    # 발견 즉시 전송
                    yield f"data: {json.dumps({'type': 'found', 'result': result, 'found_total': len(results)}, ensure_ascii=False)}\n\n"

                time.sleep(0.2)

            # 3단계: 완료
            results.sort(key=lambda x: x["score"], reverse=True)
            scan_state["last_results"] = results
            scan_state["last_scan_date"] = datetime.now().isoformat()

            yield f"data: {json.dumps({'type': 'complete', 'total_found': len(results), 'results': results, 'scan_date': scan_state['last_scan_date']}, ensure_ascii=False)}\n\n"

        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)}, ensure_ascii=False)}\n\n"
        finally:
            scan_state["running"] = False

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 8765))
    print("\n" + "=" * 50)
    print("  주봉 다이버전스 스크리너 서버")
    print(f"  http://localhost:{port}")
    print("=" * 50 + "\n")
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
