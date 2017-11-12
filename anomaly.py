import numpy as np


def is_anomaly(result):
    data = np.array([item[1] for name in result for item in result[name]])

    for name in result:
        for item in result[name]:
            item[2] = item[1] > data.mean() + 3 * data.std()