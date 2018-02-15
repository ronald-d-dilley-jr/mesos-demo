#! /usr/bin/env python3


import random
import time


def main():
    random.seed(time.gmtime())
    guess = random.randint(1, 100)

    if guess > 90:
        seconds = random.randint(35, 50)
    else:
        seconds = random.randint(1, 30)

    print('Starting to sleep {} seconds'.format(seconds))
    time.sleep(seconds)
    print('Finished sleeping')


if __name__ == '__main__':
    main()
