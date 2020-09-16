# FLSDReal

The project contains three components,
1. The FLSDReal player
2. A live video emulator (inside liveVideo directory)
3. A mininet testbed to test the player

All the scripts require python version 3.7 or more to execute. Several dependency may be there and all of them can be downloaded via `pip`. One such dependacy is `requests`.

## Live video emulator
The FLSDReal player require a live video stream. As it is difficult to find a real video stream, we emulate real video stream using live video proxy. However, the emulator needs a DASHified video to emulate as live. One such emulated video can be found at [DASHVideo](https://github.com/abhimp/DASHVideos). The command to run the live video proxy is

    python3.7 liveVideo/proxyServer.py /path/to/mpd/file

## FLSDReal player
The FLSDReal player plays a live video stream available from the emulator. It can connect to other players and form a coalition. However, the current implementation does not support automatic player finding and coalition formation. We have to inform the player whom to connect in the command connect. It plays the live video in an HTML5 based video player. The entry point for the player is `webviewtestAsync.py`. It accepts the following arguments.

      -m MPDPATH, --mpd-path MPDPATH
      -p GROUPPORT, --listenPort GROUPPORT
      -n NEIGHBOURADDRESS, --neighbourAddress NEIGHBOURADDRESS
      -b BROWSERCOMMAND, --browserCommand BROWSERCOMMAND
      -L LOGDIR, --logDir LOGDIR
      -F FINSOCK, --finishedSocket FINSOCK
      -d TMPDIR, --tmpDir TMPDIR
Here is a detailed description of these options.

| Option | Mandatory | Description |
|---|---|---|
| `MPDPATH` | Yes | A URL to the live video feed. In the case of the live video emulator, URL looks like `http://localhost:9876/media/mpdjson`. |
| `GROUPPORT` | No | A port number to listen for incoming connections from other players. If port no is not provided, the player will try to listen at the port 10000. In the current implementation, the same port is also used to communicate the browser-based player. In the current implementation, all the communication is performed through the HTTP protocol only. It can be changed to other protocols in the future. |
| `NEIGHBOURADDRESS` | No | Neighbour player address in the form of `ip:port`. It is not mandatory. If the address is provided, the current player will try to connect to the address and join the coalition. However, if the neighbor address is not provided, the player will act as a standalone player. |
| `BROWSERCOMMAND` | No | A path browser to access the native HTML5 player. In the current implementation, we use a bash script to run the browser. These scripts are present at the `browser` directory. |
| `LOGDIR` | No | A path to store several stats of the playback. |
| `FINSOCK` | No | External experiment script (e.g. mininet testbed) may want to be notified when live video playback is ended. `FINSOCK` is the way. It is a path to a `named fifo` file. When playback is over, the player opens the `FINSOCK` and write something. |
| `TMPDIR` | No | Temporary directory to store downloaded chunk by the player. |

## Mininet testbed
To perform evaluation of the FLSDReal platform, We use Mininet test which is available at the `mininet-test` directory. The emulator have capability to run trace driven emulation. It automatically runs the live video emulator in a node, a player in another node and multiple other neighbors in different node. Link capacity can be emulated independently using trace files. The is entry point of the test bed is `mininet-network.py`. It also has several options. These are

    -n NEIGHBORS, --neighbor NEIGHBORS
    -a, --stand-alone
    -m MPDPATH, --mpd-path MPDPATH
    -L LOGDIR, --logDir LOGDIR
    -F FINSOCK, --finishedSocket FINSOCK
    -t [TRACEFILES [TRACEFILES ...]], --trace-file [TRACEFILES [TRACEFILES ...]]

| Option | Manadatory | Description |
|---|---|---|
| NEIGHBORS | No | |
| -a | No | Do not allow players to form coalition. Nothing fancy trick here. It just does not pass the neighboraddress parameter in the player script. |
| MPDPATH | Yes | Path to MPD file |
| LOGDIR | - | |
| FINSOCK | - | |
| TRACEFILES | No | List of trace files for each player. If number of trace files is lesser than the number of player, list of trace files are looped around. |

Example tracefile can be found at `mininet-test`. A example command to run the mininet test is as follows:

    python3.7 FLSDreal/mininet-test/mininet-network.py -m /path/to/mpdfile -L Log -t FLSDreal/mininet-test/exampletrace
