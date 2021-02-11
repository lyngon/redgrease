# You can external libraris, as long as they are in the 'requirements.txt' file
import numpy as np

# This is for syntax highlighting and auto completion of RedisGears stuff
# during development.
# I.e: 'GearsBuilder' and its functions.
# It can be removed for final deployments, but can also be left as is.
from redgrease import GearsBuilder


def foo():
    pass


rg = np.random.default_rng(1)

# import matplotlib.pyplot as plt

# Build a vector of 10000 normal deviates with variance 0.5^2 and mean 2

mu, sigma = 2, 0.5

v = rg.normal(mu, sigma, 10000)

# Plot a normalized histogram with 50 bins


# plt.hist(v, bins=50, density=1)       # matplotlib version (plot)

# Compute the histogram with numpy and then plot it

(n, bins) = np.histogram(v, bins=50, density=True)  # NumPy version (no plot)

# plt.plot(.5*(bins[1:]+bins[:-1]), n)

gb = GearsBuilder("CommandReader")
gb.map(foo)
gb.register(trigger="video")
