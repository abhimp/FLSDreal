import traceback as tb
import os
import sys



def getTraceBack(exc_info):
    error = "pid:" + str(os.getpid()) + " ppid:" + str(os.getppid()) + "\n"
    error += str(exc_info[0]) + "\n"
    error += str(exc_info[1]) + "\n\n"
    error += "\n".join(tb.format_tb(exc_info[2]))
    return error

def lineno():
    return sys._getframe().f_back.f_lineno

def getPosition():
    frame = sys._getframe().f_back
    line = frame.f_lineno
    fileName = frame.f_code.co_filename
    return f"{fileName}:{line}"

def getStack():
    frame = sys._getframe().f_back
    stack = []
    while frame:
        line = frame.f_lineno
        fileName = frame.f_code.co_filename
        func = frame.f_code.co_name
        st = f"{func} at {fileName}:{line}"
        stack += [st]
        frame = frame.f_back
    return stack
