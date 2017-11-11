# test-assignment

This script analyzes user requests and creates a metric of how many 5xx responses an api_name*http_method pair got in the span of 15 minutes, marking each 15 minute interval as an anomaly if the number of 5xx reponces exceeds the plus 3th-sigma (3σ) range.
