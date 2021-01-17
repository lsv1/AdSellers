#!/usr/bin/env python
# -*- coding: utf-8 -*-

def chunks(list_size, list_chunk_size):
    # For item i in a range that is a length of l,
    for i in range(0, len(list_size), list_chunk_size):
        # Create an index range for l of n items:
        yield list_size[i:i + list_chunk_size]


if __name__ == "__main__":
    first_names = ['Steve', 'Jane', 'Sara', 'Mary', 'Jack', 'Bob', 'Bily', 'Boni', 'Chris', 'Sori', 'Will', 'Won', 'Li']
    print(list(chunks(first_names, 8)))
