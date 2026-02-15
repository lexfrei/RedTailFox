import time


class MetricContext:
    def __init__(self, slot):
        self._slot = slot

    def inc(self, name, delta=1):
        self._slot.metrics[name] = self._slot.metrics.get(name, 0) + delta
        self._slot.metrics_version += 1
        self._slot.last_active = time.time()

    def set(self, name, value):
        self._slot.metrics[name] = value
        self._slot.metrics_version += 1
        self._slot.last_active = time.time()
