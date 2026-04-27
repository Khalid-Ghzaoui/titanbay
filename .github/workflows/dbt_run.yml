name: dbt build

on:
  push:
    branches:
      - main

jobs:
  dbt-build:
    runs-on: windows-latest

    steps:
      - name: Checkout code
        uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.12'

      - name: Install dependencies
        run: pip install -r requirements.txt

      - name: Authenticate to GCP
        uses: google-github-actions/auth@v1
        with:
          credentials_json: ${{ secrets.GCP_CREDENTIALS }} 

      - name: Set up dbt profile
        run: |
          mkdir -p ~/.dbt
          cat <<EOF > ~/.dbt/profiles.yml
          titanbay:
            target: dev
            outputs:
              dev:
                type: bigquery
                method: oauth
                project: titanbay-494310
                dataset: titanbay_dev
                threads: 4
                timeout_seconds: 300
          EOF

      - name: Run dbt build
        run: dbt build