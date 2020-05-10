#!/usr/bin/env bash

# sudo -u exp -i <<EOF

# sleep 2

dumpcap -p -n -t -i $MININET_IFC -y EN10MB -w - >$MININET_TRACE_FILE 2> /dev/null


# sleep 52

# EOF

