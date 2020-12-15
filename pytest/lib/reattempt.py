import time


def retry(fn, timeout):
    started = time.time()
    delay = 0.05
    while True:
        try:
            return fn()
        except:
            if time.time() - started >= timeout:
                raise
            time.sleep(delay)
            delay *= 1.2
