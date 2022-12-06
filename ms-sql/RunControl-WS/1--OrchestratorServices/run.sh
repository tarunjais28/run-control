#!/usr/bin/env bash

cargo run --release -- \
-h localhost:8000 \
-c "Driver={ODBC Driver 17 for SQL Server};Server={192.168.66.20};Database={RunControl};UID={RunCtrl};PWD={OuPatqQbMaRH7Fu};" \
-l test-bed/log.txt \
-d test-bed/diag-log.txt