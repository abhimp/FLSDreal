import threading
import queue
import time

from util.misc import CallableObj

class Worker():
    def __init__(self, resultCB, exitCB=None):
        self.thread = threading.Thread(target=self.run)
        self.working = False
        self.exit = False
        self.taskQueue = queue.Queue()
        self.resultCB = resultCB
        self.exitCB = exitCB
        self.thread.start()

    def getId(self):
        return self.thread.ident

    def run(self):
        while not self.exit:
            task = self.taskQueue.get()
            if task[0] == 'exit':
                break
            cb, a, b = task
            self.working = True
            res = cb(*a, **b)
            self.working = False
            self.resultCB(self, res)
        self.exit = True
        if callable(self.exitCB):
            self.exitCB(self)


class EventLoop():
    def __init__(self, maxWorkers=7):
        self.EV_QUEUE = queue.Queue()
        self.TIMER_QUEUE = queue.PriorityQueue()
        self.WORKER_TASK_QUEUE = queue.Queue()
        self.workerGroupIdle = []
        self.workerGroupTerminated = []
        self.maxWorkers = maxWorkers
        self.numWorkers = 0
        self.origThread = None
        self.workerSem = threading.Semaphore(1)
        self.exit = False
        self.workerTerminateSem = threading.Semaphore(0)
        pass

    def amIMainThread(self):
        assert self.origThread is not None
        curId = threading.get_ident()
        return curId == self.origThread.ident

    def runInWorker(self, cb, *a, **b):
        assert self.origThread is not None
        curId = threading.get_ident()
        assert curId == self.origThread.ident
        if self.exit:
            return

        worker = None
        self.workerSem.acquire()
        if len(self.workerGroupIdle) == 0:
            if self.numWorkers < self.maxWorkers:
                worker = Worker(self.workerFinishedTask, self.workerExited)
                self.numWorkers += 1
                print("numWorkers:", self.numWorkers)
        else:
            worker = self.workerGroupIdle.pop(0)

        if worker:
            worker.taskQueue.put((cb, a, b))
        else:
            self.WORKER_TASK_QUEUE.put((cb, a, b))

        self.workerSem.release()

    def workerFinishedTask(self, worker, res):
        assert self.origThread is not None
        curId = threading.get_ident()
        assert curId != self.origThread.ident and curId == worker.getId()

        self.workerSem.acquire()
        if self.WORKER_TASK_QUEUE.qsize() > 0:
            task = self.WORKER_TASK_QUEUE.get()
            worker.taskQueue.put(task)
        else:
            self.workerGroupIdle.append(worker)
        self.workerSem.release()

    def workerExited(self, worker):
        assert self.origThread is not None
        curId = threading.get_ident()
        assert curId != self.origThread.ident and curId == worker.getId()

        self.workerSem.acquire()
        self.numWorkers -= 1
        self.workerGroupTerminated.append(worker)
        self.workerTerminateSem.release()
        self.workerSem.release()

    def terminateAndJoinWorker(self):
        assert self.origThread is not None
        curId = threading.get_ident()
        assert curId == self.origThread.ident

        assert self.exit
        numWorkers = 0

        self.workerSem.acquire()
        numWorkers = self.numWorkers
        for x in range(numWorkers):
            if len(self.workerGroupIdle) > 0:
                worker = self.workerGroupIdle.pop()
                worker.taskQueue.put(('exit',))
            else:
                self.WORKER_TASK_QUEUE.put(('exit',))
        self.workerSem.release()

        for x in range(numWorkers):
            self.workerTerminateSem.acquire()
        for worker in self.workerGroupTerminated:
            worker.thread.join()


    def run(self):
        assert self.origThread is None
        self.origThread = threading.current_thread()
        startTime = time.time()
        while not self.exit:
            ev = None
            timeout = None
            if self.TIMER_QUEUE.qsize() > 0:
                curTime = time.time()
                ev = self.TIMER_QUEUE.get()
                if ev[0] - curTime > 0.001:
                    self.TIMER_QUEUE.put(ev)
                    timeout = ev[0] - curTime
                    ev = None
            if ev is None:
                try:
                    ev = self.EV_QUEUE.get(timeout=timeout)
                except queue.Empty:
                    pass
            if ev is None:
                continue

            _, cb, a, b = ev
            cb(*a, **b)
        self.exit = True
        self.terminateAndJoinWorker()
        self.origThread = None

    def shutdown(self):
        self.exit = True
        self.addTask(self.noop)

    def addTask(self, cb, *a, **b):
        self.EV_QUEUE.put((None, cb, a, b))

    def setTimeout(self, timeout, cb, *a, **b):
        assert timeout >= 0
        if timeout <= 0.001:
            return self.addEvent(cb, *a, **b)
        runat = time.time() + timeout
        self.TIMER_QUEUE.put((runat, cb, a, b))
        self.addTask(self.noop) #let the event handler know that there is a event

    def noop(self):
        pass

DEFAULT_EVENT_LOOP = EventLoop()

def addTask(*a, **b):
    DEFAULT_EVENT_LOOP.addTask(*a, **b)
def setTimeout(*a, **b):
    DEFAULT_EVENT_LOOP.setTimeout(*a, **b)
def runInWorker(*a, **b):
    DEFAULT_EVENT_LOOP.runInWorker(*a, **b)
def run():
    DEFAULT_EVENT_LOOP.run()


