import numpy as np


def is_anomaly(result):
    data = [item[1] for name in result for item in result[name]]

    x = np.array(data)

    for name in result:
        for item in result[name]:
            item[2] = item[1] > x.mean() + 3 * x.std()