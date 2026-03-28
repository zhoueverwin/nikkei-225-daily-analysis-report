"""Beginner lesson concept library — tracks covered topics and provides pre-written lessons."""

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Pre-written beginner lesson library in Japanese
TOPIC_LIBRARY: list[dict[str, str]] = [
    {
        "id": "vix",
        "topic_ja": "VIX（恐怖指数）とは？",
        "explanation_ja": "VIXは「恐怖指数」とも呼ばれ、投資家がこの先の株式市場にどれくらい不安を感じているかを数値で表したものです。天気予報の「降水確率」のようなもので、数字が高いほど「荒れそうだ」と多くの人が思っていることを意味します。通常は15-20くらいが平穏な状態。30を超えると「かなり不安」、40超えは「パニック」水準です。極端に高くなった後は「売られすぎ」の反動で株価が戻ることもあるため、逆張りの指標としても使われます。",
        "keywords": ["vix", "恐怖指数", "volatility", "ボラティリティ"],
    },
    {
        "id": "rsi",
        "topic_ja": "RSI（相対力指数）とは？",
        "explanation_ja": "RSIは株価の「体温計」のような指標です。0から100の範囲で、株が買われすぎか売られすぎかを教えてくれます。例えるなら、マラソンランナーの心拍数のようなもの。70以上は「走りすぎて疲れている」＝買われすぎで下がるかも。30以下は「まだ余力がある」＝売られすぎで上がるかも。ただし、強い上昇トレンド中は70以上が続くこともあるので、これだけで判断するのは危険です。他の指標と組み合わせて使うのがコツです。",
        "keywords": ["rsi", "相対力指数", "overbought", "oversold", "買われすぎ", "売られすぎ"],
    },
    {
        "id": "macd",
        "topic_ja": "MACD（マックディー）とは？",
        "explanation_ja": "MACDは「トレンドの方向転換」を見つけるための指標です。料理に例えると、お湯が沸騰する前の「プツプツ」という泡立ちのようなもの。株価が上がり始める前兆や、下がり始める前兆をキャッチします。「ゴールデンクロス」（MACD線がシグナル線を上に抜ける）は買いサイン、「デッドクロス」は売りサイン。毎日のニュースで「MACDがゴールデンクロス」と聞いたら、上昇トレンドの始まりかもしれないという意味です。",
        "keywords": ["macd", "ゴールデンクロス", "デッドクロス", "golden cross", "death cross"],
    },
    {
        "id": "moving_average",
        "topic_ja": "移動平均線とは？",
        "explanation_ja": "移動平均線は株価の「なめらかな流れ」を見るためのツールです。毎日の体重を記録するとギザギザになりますが、1週間の平均を取ると「太り気味」「痩せ気味」の傾向が見えますよね。それと同じです。日本株では5日線（1週間）、25日線（約1ヶ月）、75日線（約3ヶ月）がよく使われます。株価が移動平均線より上にあれば「上昇トレンド」、下なら「下降トレンド」の目安になります。",
        "keywords": ["移動平均", "ma", "moving average", "5日線", "25日線", "75日線"],
    },
    {
        "id": "bollinger_bands",
        "topic_ja": "ボリンジャーバンドとは？",
        "explanation_ja": "ボリンジャーバンドは株価の「通常範囲」を示す帯です。道路に例えると、中央線が移動平均線、上下の白線がボリンジャーバンド。車（株価）が白線をはみ出すと「異常事態」です。統計的に株価の約95%はこの帯の中に収まります。上の帯を突破すると「買われすぎ」で反落注意、下の帯を突破すると「売られすぎ」で反発期待。帯の幅が狭くなると「そろそろ大きく動くかも」というサインです。",
        "keywords": ["ボリンジャーバンド", "bollinger", "バンド幅"],
    },
    {
        "id": "usdjpy",
        "topic_ja": "円安・円高が日本株に与える影響",
        "explanation_ja": "円安（ドル円が上がる）は日本の輸出企業にとって追い風です。トヨタが1台100万円の車をアメリカで売る場合、1ドル=100円なら1万ドル、1ドル=150円なら6,667ドルで売れます。つまり円安だと海外で「お買い得」になり売上が伸びます。逆に円高は輸出企業には逆風ですが、海外から原材料を買う企業や、海外旅行者にはプラス。日経225は輸出大企業が多いため、一般に円安→株高、円高→株安の傾向があります。",
        "keywords": ["円安", "円高", "usdjpy", "ドル円", "為替", "輸出"],
    },
    {
        "id": "yield_curve",
        "topic_ja": "国債利回りと株式市場の関係",
        "explanation_ja": "国債利回りは「お金を貸すときの利息」のようなものです。国にお金を貸して（国債を買って）もらえる利息が高くなると、リスクのある株より安全な国債の方が魅力的になり、株からお金が流出しやすくなります。特に米国10年国債の利回りは世界の金融市場の「基準金利」のような存在。これが急上昇すると株式市場が下がることが多いです。逆に利回りが下がると「お金は株に回した方がいい」となり、株高につながりやすいです。",
        "keywords": ["国債", "利回り", "yield", "10年債", "金利"],
    },
    {
        "id": "put_call_ratio",
        "topic_ja": "プットコールレシオとは？",
        "explanation_ja": "プットコールレシオは「弱気派と強気派の勢力図」を表す指標です。選挙の世論調査のようなもので、市場参加者が「上がる」と思っている人（コール=強気派）と「下がる」と思っている人（プット=弱気派）の比率を示します。この数値が1を超えると弱気派が多数で、市場は悲観的。逆に低いと楽観的。面白いのは、極端に弱気に傾くと「みんなが既に売った=もう売る人がいない」状態になり、そこから株が反発することがある点です。",
        "keywords": ["プットコール", "put call", "オプション"],
    },
    {
        "id": "oil_japan",
        "topic_ja": "原油価格と日本経済の関係",
        "explanation_ja": "日本はエネルギーの約90%を輸入に頼る資源小国です。原油価格が上がると、ガソリン代・電気代・物流費が上がり、企業のコスト増→利益減となりやすいです。スーパーの商品が値上がりするのも元をたどれば原油高が原因のことが多いです。ただし、商社（三菱商事など）は資源を扱うため原油高で恩恵を受けます。また、極端な原油高は世界全体の景気を冷やすリスクがあり、「原油100ドル超え」は市場にとって警戒水準とされます。",
        "keywords": ["原油", "oil", "wti", "エネルギー", "opec"],
    },
    {
        "id": "sector_rotation",
        "topic_ja": "セクターローテーションとは？",
        "explanation_ja": "セクターローテーションは「景気の波に合わせて有利な業種が入れ替わる」現象です。四季に例えると分かりやすいです。景気回復期（春）→IT・半導体が上昇。景気拡大期（夏）→銀行・不動産が上昇。景気後退の兆し（秋）→医薬品・電力など守りの業種に資金移動。景気後退期（冬）→現金や国債に退避。日経平均が同じ水準でも、中身を見ると「どの業種が買われているか」で今の景気段階が分かります。",
        "keywords": ["セクター", "ローテーション", "業種", "sector rotation", "板块"],
    },
    {
        "id": "golden_death_cross",
        "topic_ja": "ゴールデンクロスとデッドクロスとは？",
        "explanation_ja": "短期の移動平均線が長期の移動平均線を下から上に突き抜けることを「ゴールデンクロス」、上から下に突き抜けることを「デッドクロス」と呼びます。信号機に例えると、ゴールデンクロスは「青信号」、デッドクロスは「赤信号」です。ただし、信号が変わっても車がすぐ止まらないように、クロスが出ても株価がすぐ反応しないこともあります。また、横ばい相場ではダマシ（偽のシグナル）が多いため、他の指標も確認することが大切です。",
        "keywords": ["ゴールデンクロス", "デッドクロス", "golden cross", "death cross", "クロス"],
    },
    {
        "id": "support_resistance",
        "topic_ja": "サポートライン（支持線）とレジスタンスライン（抵抗線）",
        "explanation_ja": "サポートラインは株価の「床」、レジスタンスラインは「天井」のようなものです。ボールを想像してください。床に落ちたボールは跳ね返りますが、天井にぶつかると跳ね返って落ちます。株価も同じで、ある価格帯で何度も反発したり跳ね返されたりします。これは多くの投資家が「この価格なら買いたい」「この価格なら売りたい」と思っているからです。サポートが破られると大きな下落、レジスタンスを突破すると大きな上昇のきっかけになります。",
        "keywords": ["サポート", "レジスタンス", "support", "resistance", "支持線", "抵抗線"],
    },
    {
        "id": "risk_on_off",
        "topic_ja": "リスクオン・リスクオフとは？",
        "explanation_ja": "リスクオンは「投資家がやる気満々で、リスクの高い資産を積極的に買う状態」、リスクオフは「怖くなって安全な場所にお金を避難させる状態」です。遊園地に例えると、リスクオンは「ジェットコースターに乗りたい！」、リスクオフは「観覧車で安全に過ごそう…」。リスクオフでは株が売られ、金や国債、日本円やスイスフランなどの「安全通貨」が買われます。VIXが急上昇して金価格が上がっていたら、リスクオフのサインです。",
        "keywords": ["リスクオン", "リスクオフ", "risk on", "risk off", "安全資産"],
    },
    {
        "id": "boj_policy",
        "topic_ja": "日銀の金融政策とは？",
        "explanation_ja": "日銀（日本銀行）は日本のお金の「交通整理係」です。景気が悪い時は「金融緩和」（お金をたくさん流通させて企業が借りやすくする）、景気が過熱している時は「金融引き締め」（お金の流通を減らして冷ます）を行います。2024年に日銀はマイナス金利を解除して利上げに転じました。利上げは銀行株にプラス（金利で稼げる）ですが、不動産や成長株にはマイナス（借入コスト増）。日銀の政策変更は為替（ドル円）にも大きく影響します。",
        "keywords": ["日銀", "boj", "金融政策", "利上げ", "利下げ", "緩和", "引き締め"],
    },
    {
        "id": "pce_cpi_difference",
        "topic_ja": "CPIとPCEデフレーターの違い",
        "explanation_ja": "どちらも物価（インフレ）を測る指標ですが、計算方法が違います。CPIは「決まった買い物かごの中身の値段」を毎月チェック。牛肉が高くなっても牛肉を計算に入れ続けます。一方PCEは「実際に人々が買ったもの」で計算するので、牛肉が高くなって豚肉に切り替えた消費者の行動も反映します。FRB（米国の中央銀行）はPCEを重視します。ニュースでCPIが高くても、PCEが低ければ「FRBは利下げするかも」と市場は判断します。",
        "keywords": ["cpi", "pce", "インフレ", "消費者物価", "デフレーター"],
    },
]


class BeginnerTopicManager:
    """Manages beginner lesson topics to avoid repetition."""

    def __init__(self, history_path: str = "memory/beginner_history.json"):
        self.history_path = Path(history_path)
        self.library = {t["id"]: t for t in TOPIC_LIBRARY}
        self.history = self._load_history()

    def _load_history(self) -> dict[str, str]:
        """Load history of covered topics. Returns {topic_id: date_covered}."""
        if self.history_path.exists():
            try:
                with open(self.history_path) as f:
                    return json.load(f)
            except (json.JSONDecodeError, OSError):
                return {}
        return {}

    def _save_history(self) -> None:
        """Save covered topics history."""
        self.history_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.history_path, "w") as f:
            json.dump(self.history, f, indent=2)

    def select_topic(self, context_keywords: list[str], date: str) -> dict[str, str] | None:
        """Select the best uncovered topic matching today's analysis context.

        Args:
            context_keywords: Keywords from today's analysis (e.g., ["vix", "oil", "rsi"])
            date: Today's date string

        Returns:
            Selected topic dict, or None if all covered.
        """
        # Score each uncovered topic by keyword match
        candidates = []
        for topic_id, topic in self.library.items():
            if topic_id in self.history:
                continue  # Already covered
            # Score by keyword overlap
            score = 0
            for kw in context_keywords:
                kw_lower = kw.lower()
                if any(kw_lower in tk.lower() for tk in topic["keywords"]):
                    score += 2
                if kw_lower in topic["topic_ja"].lower() or kw_lower in topic["explanation_ja"].lower():
                    score += 1
            candidates.append((topic_id, topic, score))

        if not candidates:
            # All topics covered — reset and start over
            logger.info("All beginner topics covered. Resetting history.")
            self.history = {}
            self._save_history()
            return self.select_topic(context_keywords, date)

        # Sort by score (highest first), then by library order for consistency
        candidates.sort(key=lambda x: x[2], reverse=True)
        best_id, best_topic, _ = candidates[0]

        return {
            "topic_ja": best_topic["topic_ja"],
            "explanation_ja": best_topic["explanation_ja"],
        }

    def mark_covered(self, topic_ja: str, date: str) -> None:
        """Mark a topic as covered."""
        for topic_id, topic in self.library.items():
            if topic["topic_ja"] == topic_ja:
                self.history[topic_id] = date
                self._save_history()
                logger.info(f"Marked beginner topic as covered: {topic_id}")
                return

    def get_coverage_stats(self) -> dict[str, Any]:
        """Get stats on topic coverage."""
        total = len(self.library)
        covered = len(self.history)
        return {
            "total_topics": total,
            "covered": covered,
            "remaining": total - covered,
            "coverage_pct": round(covered / total * 100, 1) if total > 0 else 0,
        }
