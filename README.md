---
page_type: sample
description: "A Python application that uses Batch to process media files in parallel with the ffmpeg open-source tool."
languages:
- python
products:
- azure
---

# Batch Python File Processing with ffmpeg

A Python application that uses Batch to process media files in parallel with the [ffmpeg](http://ffmpeg.org/) open-source tool.

For details and explanation, see the accompanying article [Run a parallel workload with Azure Batch using the Python API](https://learn.microsoft.com/azure/batch/tutorial-parallel-python).

## 概要

このサンプルは、Azure Batch と ffmpeg を使って mp4 を mp3 に並列変換します。プール OS は Ubuntu 22.04 (Jammy) を使用し、SDK は azure-batch v14 系に更新済みです。

出典: 本リポジトリは、次の公式チュートリアルをベースにしています。
[Run a parallel workload with Azure Batch using the Python API](https://learn.microsoft.com/azure/batch/tutorial-parallel-python)

## 前提条件

- Azure サブスクリプション
- Azure Batch アカウント（クラシック/ユーザーサブスクリプションどちらでも可）
- 一般用途の Azure Storage アカウント（接続キー）
- Python 3.8+（動作確認は 3.13）と pip
- 任意: Azure CLI（AAD 認証時のログイン用）

## セットアップ

1. 依存関係のインストール（Windows/PowerShell）

    ```powershell
    python -m venv .venv
    ./.venv/Scripts/Activate.ps1
    pip install -r src/requirements.txt
    ```

1. 入力ファイルを配置

    - 変換したい mp4 を `src/InputFiles/` に配置します。

1. 設定ファイルの編集

    - 最初にサンプル設定をコピーしてから編集します（`config.py` は .gitignore 済みで公開されません）。
      - `src/config_sample.py` を `src/config.py` にコピー
      - `src/config.py` を開き、以下を環境に合わせて設定
        - `_STORAGE_ACCOUNT_NAME` / `_STORAGE_ACCOUNT_KEY`（AAD 利用時はキー不要）
        - `_BATCH_ACCOUNT_URL`（例: `https://<account>.<region>.batch.azure.com`）
        - `_AUTH_MODE` を `AAD` か `SharedKey` に設定
        - `_BATCH_ACCOUNT_NAME` / `_BATCH_ACCOUNT_KEY`（SharedKey の場合のみ）
        - `_SUBNET_ID`（ユーザーサブスクリプション＋VNet を使う場合のみ。未設定なら公開ネットワークで作成）
        - プール/ジョブ関連: `_POOL_ID`, `_POOL_VM_SIZE`, `_LOW_PRIORITY_POOL_NODE_COUNT` など
        - 実行オプション（既定は False）:
          - `_DELETE_EXISTING_POOL_AT_START`: 起動時に既存プールを削除して作り直すか（既定 False。通常は既存プールを再利用）
          - `_CLEAN_STORAGE_AT_START`: 起動時に `input`/`output` コンテナーを削除して作り直すか（既定 False）

## 認証方式

- AAD（推奨、ユーザーサブスクリプションに適合）
  - 事前に `az login` でログインし、Batch アカウント/サブスクリプションに十分な RBAC があること（例: Contributor）。
  - コードは `DefaultAzureCredential` でトークン取得 → `msrest.authentication.BasicTokenAuthentication` に包んで `BatchServiceClient` を初期化します。
- SharedKey
  - `config.py` の `_AUTH_MODE = 'SharedKey'` と `_BATCH_ACCOUNT_KEY` を設定します。

## プール OS イメージ（既定）

- Publisher: `Canonical`
- Offer: `0001-com-ubuntu-server-jammy`
- SKU: `22_04-lts`
- Node agent SKU: `batch.node.ubuntu 22.04`

リージョンによってはサポート状況が異なる場合があります。エラーになる場合は、対象リージョンのサポートイメージを一覧し、Offer/SKU と node agent を合わせてください（例: East US）。

## 実行方法

```powershell
cd src
python .\batch_python_tutorial_ffmpeg.py
```

- スクリプトは `input`/`output` コンテナーを作成し、入力をアップロード、プールとジョブを作成、タスクを投入します。
- 変換結果（mp3）は `output` コンテナーにアップロードされます。
- 実行後に `input` コンテナーは削除されます。ジョブ/プールは残します（対話による削除プロンプトはありません）。
  - 既定ではプールは再利用します（`_DELETE_EXISTING_POOL_AT_START = False`）。強制的に作り直す場合のみ True にしてください。
  - 既定ではストレージの初期削除は行いません（`_CLEAN_STORAGE_AT_START = False`）。完全クリーン開始したい場合のみ True にしてください。

## VNet（任意）

- ユーザーサブスクリプションモードで自前の VNet に参加させる場合、`config._SUBNET_ID` にサブネットのリソース ID を設定します。
- 未設定の場合は公開ネットワーク経由（Public IP）で動作します。

## トラブルシュート

- nodeAgentSKUId が無効/見つからない
  - "batch.node.ubuntu 22.04" を使用し、Offer/SKU に Jammy `22_04-lts` を設定してください。
- 画像（ImageReference）がサポートされない
  - 対象リージョンのサポートイメージを確認して合わせてください。
- AAD 認証で失敗する
  - `az login` 済み、適切な RBAC が付与されているか、Batch アカウントの AAD/ネットワーク設定が要件を満たすか確認してください。
- ffmpeg が見つからない
  - StartTask で `apt-get install -y ffmpeg` を実行しています。プール作成の失敗や OS イメージ不一致がないか確認してください。

## 使用ライブラリ

- azure-batch >= 14.0.0
- azure-identity >= 1.17.0（AAD のみ）
- azure-storage-blob >= 12.19.0（ユーザー委任 SAS を使用。Track 2 SDK）

## Resources

- [Azure Batch documentation](https://learn.microsoft.com/azure/batch/)
- [Azure Batch code samples](https://github.com/Azure/azure-batch-samples)
