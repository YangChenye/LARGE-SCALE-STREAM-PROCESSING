# Created by Chenye Yang on 2020/4/1.
# Copyright © 2020 Chenye Yang. All rights reserved.

import random

data = [random.random() for i in range(1000000)]

with open('Data_Set.txt', 'w') as f:
    for item in data:
        f.write("%s\n" % item)