import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

y_label = 'Time (s)'


y1 = [231.4806529, 175.6748568, 156.0524124]
x1 = [1, 2, 3]
x1_label = 'Worker Nodes'


y2 = [106.2459897, 74.67733079, 61.19136437]
x2 = [1000, 2000, 3000]
x2_label = 'Shard Size'

y3 = [30.65154808, 71.39219123, 155.7682863]
x3 = [25, 60, 120]
x3_label = 'Data Size'

sns.set()

plt.xlabel(x1_label)
plt.ylabel(y_label)

plt.plot(x1, y1, marker='o')
# plt.legend(loc="upper left")
plt.savefig("1.png", dpi=1200)