# DVC を使用する方法

このチュートリアルでは、このリポジトリを例にして、DVC (Data Version Control) を使用する方法を説明します。以下の手順に従ってください。

## 1. DVC のインストール

まず、DVC をインストールします。

```bash
pip install dvc-gs
```
または、VSCodeのExtensionでダウンロードする。

## 2. DVC の初期化

リポジトリで DVC を初期化します。

```bash
dvc init
```

これにより、`.dvc` ディレクトリが作成され、DVC の設定ファイルが追加されます。

## 3. データの追跡

データを管理するために、追跡したいデータを指定します。たとえば、`data/` ディレクトリを追跡する場合:

```bash
dvc add data/
```

これにより、`data/` ディレクトリに対応する `.dvc` ファイルが作成されます。

## 4. Git に変更をコミット

DVC の設定ファイルと `.dvc` ファイルを Git にコミットします。

```bash
git add .dvc .gitignore data.dvc
git commit -m "Track data directory with DVC"
```

## 5. リモートストレージの設定

データを保存するリモートストレージを設定します。GCS (Google Cloud Storage) バケットを使用する場合、まず以下のコマンドでユーザー認証を行います:

```bash
gcloud auth application-default login
```

その後、リモートストレージを設定します。たとえば、`gs://graspp-25s-innovation` バケットを使用する場合:

```bash
dvc remote add -d myremote gs://graspp-25s-innovation
```

リモートストレージを設定したら、変更を Git にコミットします。

```bash
git add .dvc/config
git commit -m "Configure remote storage"
```

## 6. データのプッシュ

リモートストレージにデータをアップロードします。

```bash
dvc push
```

これにより、データがリモートストレージに保存されます。

## 7. データの取得

別の環境でデータを取得するには、以下を実行します。

```bash
git clone <repository-url>
cd <repository-directory>
dvc pull
```

これで、リモートストレージからデータが取得されます。

## まとめ

以上が DVC を使用してデータを管理する基本的な手順です。このリポジトリを使用して、データのバージョン管理を簡単に行うことができます。詳細は [DVC の公式ドキュメント](https://dvc.org/doc) を参照してください。  