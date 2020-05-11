#!/usr/bin/env bash
set -x
python3.7 ${MININET_WD}/../liveVideo/proxyServer.py -a $WEBVIEWTEST_PARAM $MPD_SERVER_VIDEO_PATH

