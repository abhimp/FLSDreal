import sys
import time

BLACK = "\033[30m" #\033[40m
RED = "\033[31m" #\033[41m
GREEN = "\033[32m" #\033[42m
ORANGE = "\033[33m" #\033[43m
BLUE = "\033[34m" #\033[44m
MAGENTA = "\033[35m" #\033[45m
CYAN = "\033[36m" #\033[46m
GRAY = "\033[37m" #\033[47m
RESET = "\033[39m" #\033[49m

def __startColor(color, file=None):
    if file is None:
        file=sys.stdout
    if file.isatty():
        return color
    return str(time.time()) + ":"

def __endColor(file=None):
    if file is None:
        file=sys.stdout
    if file.isatty():
        return RESET
        file.flush()
    return ""

def myprint(*a, **b):
    file=b.get("file", None)
    print(*a, **b)
    if file:
        file.flush() # I need all the log possible.

def red(*a, **b):
    file=b.get("file", None)
    myprint(__startColor(RED, file=file), *a, __endColor(file=file), **b)

def green(*a, **b):
    file=b.get("file", None)
    myprint(__startColor(GREEN, file=file), *a, __endColor(file=file), **b)

def orange(*a, **b):
    file=b.get("file", None)
    myprint(__startColor(ORANGE, file=file), *a, __endColor(file=file), **b)

def blue(*a, **b):
    file=b.get("file", None)
    myprint(__startColor(BLUE, file=file), *a, __endColor(file=file), **b)

def magenta(*a, **b):
    file=b.get("file", None)
    myprint(__startColor(MAGENTA, file=file), *a, __endColor(file=file), **b)

def cyan(*a, **b):
    file=b.get("file", None)
    myprint(__startColor(CYAN, file=file), *a, __endColor(file=file), **b)

def gray(*a, **b):
    file=b.get("file", None)
    myprint(__startColor(GRAY, file=file), *a, __endColor(file=file), **b)


def redS(**b):
    file=b.get("file", None)
    myprint(__startColor(RED, file=file))

def greenS(**b):
    file=b.get("file", None)
    myprint(__startColor(GREEN, file=file))

def orangeS(**b):
    file=b.get("file", None)
    myprint(__startColor(ORANGE, file=file))

def blueS(**b):
    file=b.get("file", None)
    myprint(__startColor(BLUE, file=file))

def magentaS(**b):
    file=b.get("file", None)
    myprint(__startColor(MAGENTA, file=file))

def cyanS(**b):
    file=b.get("file", None)
    myprint(__startColor(CYAN, file=file))

def grayS(**b):
    file=b.get("file", None)
    myprint(__startColor(GRAY, file=file))

def reset(**b):
    file=b.get("file", None)
    myprint(__endColor(file=file))
