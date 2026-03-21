import numpy as np

from kwb.models.bucket_probs import Bucket, bucket_probability


def test_bucket_probability():
    samples = np.array([70, 71, 72, 73, 74])
    bucket = Bucket(label="72-73", low=72, high=73)
    assert bucket_probability(samples, bucket) == 0.4
