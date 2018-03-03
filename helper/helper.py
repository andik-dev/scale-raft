def synchronized(lock):
    def synchronized_decorator(func):
        def decorator(*args, **kwargs):
            lock().acquire()
            r = func(*args, **kwargs)
            lock().release()
            return r
        return decorator
    return synchronized_decorator
