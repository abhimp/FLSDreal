try:
    from . import multiproc as mp
except:
    import multiproc as mp


import os
Pipe = mp.Pipe
Queue = mp.Queue

DEFAULT_STD_DIR = "/tmp/stdouterrdir"

def Process(outPref=None, errPref=None, *argv, **kwargv):
    if not outPref and not errPref:
        if not os.path.isdir(DEFAULT_STD_DIR):
            os.makedirs(DEFAULT_STD_DIR)
        outPref = os.path.join(DEFAULT_STD_DIR, "out_" + str(os.getpid()))
        errPref = os.path.join(DEFAULT_STD_DIR, "err_" + str(os.getpid()))
    return mp.Process(outPref = outPref, errPref = errPref, *argv, **kwargv)



def __test():
    print("hello from 1")
    time.sleep(1)

if __name__ == "__main__":
    p1 = Process(target=__test)
    p2 = Process(target=__test)
    p1.start()
    p2.start()
    p1.join()
    p2.join()
