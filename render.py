#!/usr/bin/env python
# -*- coding: utf-8; -*-
import matplotlib.pyplot as plt
import sys
import datetime
from collections import defaultdict, namedtuple

Row = namedtuple('Row', 'job start avg p50 p90 p99')

data = []

with open(sys.argv[1]) as f:
    labels = f.readline().strip().split(",")
    for l in f.readlines():
        if not l:
            continue
        bits = l.split(",")
        data.append(Row(*map(int, bits)))

bypid = defaultdict(list)
for row in data:
  bypid[row.job].append(row)

tmin = data[0].start

for k,v in bypid.items():
  xs = [(p.start - tmin)/(1000*1000) for p in v]
  ys = [p.avg for p in v]
  plt.scatter(xs, ys, marker='x')

plt.ylabel(u'read time (µs)')
plt.xlabel('seconds')
plt.xlim(xmin=0)
plt.ylim(ymin=0)
plt.savefig(sys.argv[1].replace('.csv', '.png'))
