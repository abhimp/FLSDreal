#!/usr/bin/env bash

sudo -u exp -i <<EOF
unset http_proxy
unset https_proxy

pwd
cd ${MININET_WD}/../

python3.7 ${MININET_WD}/../webviewtest.py -m http://10.0.0.1:9876/media/mpdjson -n 10.0.0.2:10000

EOF
