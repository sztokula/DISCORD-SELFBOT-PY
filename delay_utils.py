import random


def gaussian_delay(min_seconds, max_seconds):
    try:
        min_val = float(min_seconds)
        max_val = float(max_seconds)
    except (TypeError, ValueError):
        return 0.0
    if max_val < min_val:
        min_val, max_val = max_val, min_val
    if max_val == min_val:
        return max_val
    mean = (min_val + max_val) / 2.0
    sigma = (max_val - min_val) / 6.0
    if sigma <= 0:
        return mean
    value = random.gauss(mean, sigma)
    if value < min_val:
        return min_val
    if value > max_val:
        return max_val
    return value
