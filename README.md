# 日経225 毎日分析エージェント

日経225指数の毎日の市場分析レポートを自動生成するAIエージェント。

## セットアップ

```bash
pip install -r requirements.txt
```

## 環境変数

```bash
export ANTHROPIC_API_KEY="your-key"
export FRED_API_KEY="your-key"
```

## 実行

```bash
python main.py
```

レポートは `docs/` ディレクトリに出力されます。
