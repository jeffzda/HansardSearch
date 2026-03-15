#!/bin/bash
# Committee download jobs — run with: bash run_committee_downloads.sh
# Logs to /tmp/committee_dl_*.log

cd /home/jeffzda/Hansard/pipeline

echo "[$(date)] Starting 2001+2012-2020 download" >> /tmp/committee_dl_main.log
python3 00b_download_committee.py \
    --datasets commsen,commrep,commjnt,estimate \
    --start 2001-01-01 --end 2020-12-31 \
    --out ../data/raw/committee \
    >> /tmp/committee_dl_main.log 2>&1
echo "[$(date)] Finished 2001+2012-2020 download" >> /tmp/committee_dl_main.log

echo "[$(date)] Starting commbill download" >> /tmp/committee_dl_commbill.log
python3 00b_download_committee.py \
    --datasets commbill \
    --start 1998-01-01 --end 2025-12-31 \
    --out ../data/raw/committee \
    >> /tmp/committee_dl_commbill.log 2>&1
echo "[$(date)] Finished commbill download" >> /tmp/committee_dl_commbill.log
