import math
import random


def static_amount() -> float:
    return 1.0

def log_normal_amount(mu=3, sd=1.7, mean = 30) -> float:
    """Return amount taken randomly from log_normal distribution.

    Args:
        mu (int, optional): mu for random.lognormvariate. Defaults to 3.
        sd (float, optional): sigma for random.lognormvariate. Defaults to 1.7.
        mean (int, optional): The expected value u want. Defaults to 30. Actual expected value will be a bit higher as values less than 1.0 are re-rolled.
            With mean = 5 and other parameters at default, average ends up at 9, but higher mean values will be  less affected.

    Returns:
        float: Generated transaction amount.
    """
    amount = 0.0
    while amount < 1.0:
        e = math.pow(math.e, (mu + (sd*sd)/2)) # Expected Value
        multiplier = random.lognormvariate(mu, sd) / e
        amount = round(mean*multiplier, 2)

    return amount

