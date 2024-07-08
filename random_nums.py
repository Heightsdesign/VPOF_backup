import random


def generate_random_numbers(n, x):
    """
    Generate a list of n random numbers between 0 and x.

    Parameters:
    n (int): The number of random numbers to generate.
    x (int): The upper limit for the random numbers (exclusive).

    Returns:
    list: A list of n random numbers between 0 and x.
    """
    random_numbers = [random.randint(0, x) for _ in range(n)]
    return random_numbers


print(generate_random_numbers(3, 280))