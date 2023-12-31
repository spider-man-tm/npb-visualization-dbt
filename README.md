## npb-visualization-dbt

- 自作のWebAppである[NPB Data Visualization](https://npb-visualization.com/)のELTパイプラインに関するレポジトリ

<br />

### Overview
- アプリと連携した Google Analytics GA4 のデータに関してBigQueryと日次で自動連携を行なっています。
  - 日付別にシャーディングされた`events*`テーブルが保存されていきます。
- BigQuery上に保存された`events*`テーブルをマート化していくデータ基盤部分をdbtを使って構築しています。
- 開発時には{開発環境用で定義されたtarget_name}_{dbt_project.ymlで定義したスキーマ名}データセットに各モデルの結果が保存されていきます。
- 本番ジョブを用意して、そこではスケジュールに沿って{本番環境用で定義されたtarget_name}_{dbt_project.ymlで定義したスキーマ名}データセットに各モデルの結果が保存されていきます。
- 各ディレクトの構成、マクロ関数、モデル、スキーマの定義やテストについてはdbt公式に沿って作成していますので詳細はそちらを参照ください。
- `dbt-ga4`パッケージを使用しているので、そちらのインストールが必要です。

### Usage
- 始めに`dbt_project.yml`が正しいことを確認します。
- [dbt-ga4](https://hub.getdbt.com/Velir/ga4/latest)パッケージを使用しているので始めにインストールします。

```shell
# パッケージをインストール
dbt deps
```

```shell
# モデルを実行
dbt run

# テストを実行
dbt test

# テスト & モデル実行
dbt build
```
