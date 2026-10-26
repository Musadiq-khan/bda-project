#!/usr/bin/env python3
import sys

current, n, total, biggest = None, 0, 0.0, 0.0

def emit():
    if current is not None:
        print(f"{current}\t{n}\t{total / n:.3f}\t{biggest:.2f}")

for line in sys.stdin:
    key, value = line.rstrip("\n").split("\t")
    value = float(value)
    if key != current:          # a new stock begins: finish the previous one
        emit()
        current, n, total, biggest = key, 0, 0.0, 0.0
    n += 1
    total += value
    biggest = max(biggest, value)

emit()                          # the last stock
