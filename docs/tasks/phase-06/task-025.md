# Task-025: Makefile / ワンコマンド実行環境

## 概要
参加者が `make` 一発で環境構築・テスト実行できるようにする

## 依存タスク
- task-024

## 成果物
- `Makefile`

## 詳細

### ターゲット一覧
```makefile
install:        # 依存ライブラリインストール + pwscup CLIセットアップ
setup:          # install + サンプルデータ生成 + Dockerイメージビルド
test:           # pytest 実行
lint:           # ruff + mypy
run:            # サンプルデータに対してベースラインを評価する一連のデモ
docker-build:   # サンドボックスDockerイメージのビルド
generate-data:  # 合成データ生成
clean:          # 生成物のクリーンアップ
```

### `make run` の実行内容
```
1. サンプルデータの存在確認（なければ生成）
2. ベースライン匿名化の評価
3. ベースライン再識別の評価
4. リーダーボード表示
```

参加者が clone → `make setup` → `make run` の3ステップで動作確認できることが目標。

## 完了条件
- `make setup` で環境が構築できる
- `make run` でデモが最後まで動作する
- `make test` でテストが全件パスする
