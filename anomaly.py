import numpy as np


def is_anomaly(result):
    data = [item["value"] for name in result for item in result[name]]

    x = np.array(data)

    for name in result:
        for item in result[name]:
            item["anomaly"] = item["value"] > x.mean() + 3 * x.std()