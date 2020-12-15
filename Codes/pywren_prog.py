import pywren
import numpy as np


def addone(x):
    return x + 1

pwex = pywren.default_executor()
xlist = np.arange(10)
futures = pwex.map(addone, xlist)  # placeholders

# The results will be blocked until the remote job is completed.
print([f.result() for f in futures])