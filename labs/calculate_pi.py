import pyspark
import random


def __randomize(_):
    x, y = random.random(), random.random()
    return x * x + y * y < 1


def generate_pi(sc: pyspark.SparkContext, num_samples=100_000_000) -> float:
    count = sc.parallelize(range(0, num_samples)).filter(__randomize).count()
    pi = 4 * count / num_samples

    return pi


if __name__ == "__main__":
    sc = pyspark.SparkContext(appName="Pi")
    pi = generate_pi(sc)
    print(f"Approximation of Pi generated: {pi}")
    sc.stop()
