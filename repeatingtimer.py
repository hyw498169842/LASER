from threading import Thread, Event

class RepeatingTimer(Thread):
    def __init__(self, interval, function, args=[], kwargs={}):
        Thread.__init__(self)
        self.interval = interval
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.finished = Event()
        self.interrupt = Event()
        self.is_waiting = True

    def run(self):
        while not self.finished.is_set():
            self.interrupt.wait(self.interval)
            self.is_waiting = False
            self.function(*self.args, **self.kwargs)
            self.is_waiting = True
            self.interrupt.clear()
    
    def stop_waiting(self):
        self.interrupt.set()
    
    def stop_running(self):
        self.finished.set()
        self.interrupt.set()
