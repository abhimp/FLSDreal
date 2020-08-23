#!/usr/bin/env bash

echo $WEBVIEWTEST_PARAM

sudo -u exp -i <<EOF
unset http_proxy
unset https_proxy

set -x

pwd
cd ${MININET_WD}/../

if [ "$LOG_FILE_PATH" != "" ]
then
    mkdir -p $(dirname $LOG_FILE_PATH)
fi

python3.7 ${MININET_WD}/../webviewtestAsync.py -b browsers/firefoxLinux -m http://10.0.0.1:9876/media/mpdjson $WEBVIEWTEST_PARAM |& tee $LOG_FILE_PATH

EOF

