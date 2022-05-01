import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

y_label = 'Time (s)'

def fig1():
	y11 = [118.8929016, 85.72708515, 62.98989465]
	y12 = [231.4806529, 175.6748568, 156.0524124]
	y13 = [524.7479479, 333.5272473, 262.0356072]
	x1 = [1, 2, 3]
	x1_label = 'No. of peer nodes'

	plt.xlabel(x1_label)
	plt.ylabel(y_label)

	plt.plot(x1, y11, marker='o', label='100000 documents')
	plt.plot(x1, y12, marker='d', label='200000 documents')
	plt.plot(x1, y13, marker='x', label='400000 documents')
	plt.xticks(x1)
	plt.legend(loc="upper right")
	plt.savefig("1.png", dpi=1200)

def fig2():
	y21 = [106.2459897, 74.67733079, 61.19136437]
	y22 = [174.0925441, 145.1659076, 134.5214512]
	y23 = [339.1198166, 276.3191948, 258.6467459]
	x2 = [1000, 2000, 3000]
	x2_label = 'No. of documents per shard'

	plt.xlabel(x2_label)
	plt.ylabel(y_label)

	plt.plot(x2, y21, marker='o', label='100000 documents')
	plt.plot(x2, y22, marker='d', label='200000 documents')
	plt.plot(x2, y23, marker='x', label='400000 documents')
	plt.xticks(x2)
	plt.legend(loc="upper right")
	plt.savefig("2.png", dpi=1200)

def fig3():
	y31 = [8.419035671, 32.27474665, 106.2459897, 174.0925441, 339.1198166]
	y32 = [7.237993234, 29.21284624, 74.67733079, 165.1659076, 276.3191948]
	y33 = [7.12974972, 30.65154808, 71.39219123, 155.7682863, 258.6467459]
	x3 = [10000, 40000, 100000, 200000, 400000]
	x3_label = 'No. of documents'

	plt.plot(x3, y31, marker='o', label='1000 documents per shard')
	plt.plot(x3, y32, marker='d', label='2000 documents per shard')
	plt.plot(x3, y33, marker='x', label='3000 documents per shard')
	plt.xticks(x3, rotation=30)
	plt.legend(loc="upper left")
	plt.xlabel(x3_label)
	plt.ylabel(y_label)
	print(np.polyfit(y31,x3,1))
	print(np.polyfit(y32,x3,1))
	print(np.polyfit(y33,x3,1))
	plt.tight_layout()
	plt.savefig("3.png", dpi=1200)


def fig4():
	y41 = [6.016498362, 15.46836869, 36.23507445, 68.37538687, 86.90143997]
	y42 = [4.946416766, 11.41071347, 22.14102983, 47.30628942, 70.1668702]
	y43 = [3.270765838, 9.372785871, 19.06251548, 37.00922669, 52.219885448]
	x4 = [10000, 40000, 100000, 200000, 400000]
	x4_label = 'No. of documents'

	plt.xlabel(x4_label)
	plt.ylabel(y_label)

	plt.plot(x4, y41, marker='o', label='1000 documents per shard')
	plt.plot(x4, y42, marker='d', label='2000 documents per shard')
	plt.plot(x4, y43, marker='x', label='3000 documents per shard')
	plt.xticks(x4, rotation=30)
	plt.legend(loc="upper left")
	plt.tight_layout()
	print(np.polyfit(y41,x4,1))
	print(np.polyfit(y42,x4,1))
	print(np.polyfit(y43,x4,1))
	plt.savefig("4.png", dpi=1200)

def fig5():
	y51 = [105, 103, 102]
	y52 = [203, 200, 197]
	y53 = [421, 416, 408]
	x5 = [1, 2, 3]
	x5_label = 'No. of peer nodes'

	plt.xlabel(x5_label)
	plt.ylabel(y_label)

	plt.plot(x5, y51, marker='o', label='100000 documents')
	plt.plot(x5, y52, marker='d', label='200000 documents')
	plt.plot(x5, y53, marker='x', label='400000 documents')
	plt.xticks(x5)
	plt.legend(loc="center right")
	plt.savefig("5.png", dpi=1200)

def fig6():
	y6 = [11, 44, 102, 197, 408]
	x6 = [10000, 40000, 100000, 200000, 400000]
	x6_label = 'No. of documents'

	plt.xlabel(x6_label)
	plt.ylabel(y_label)

	plt.plot(x6, y6, marker='o')
	plt.xticks(x6, rotation=30)
	# plt.legend(loc="upper left")
	plt.tight_layout()
	print(np.polyfit(y6,x6,1))
	plt.savefig("6.png", dpi=1200)

def fig7():
	y71 = [7.12974972, 30.65154808, 71.39219123, 155.7682863, 258.6467459]
	y72 = [11, 44, 102, 197, 408]
	x7 = [10000, 40000, 100000, 200000, 400000]
	x7_label = 'No. of documents'

	plt.xlabel(x7_label)
	plt.ylabel(y_label)

	plt.plot(x7, y72, marker='d', label='Hadoop')
	plt.plot(x7, y71, marker='o', label='IPFS')
	plt.xticks(x7, rotation=30)
	plt.legend(loc="upper left")
	plt.tight_layout()
	plt.savefig("7.png", dpi=1200)

if __name__ == "__main__":
	# sns.set()
	fig1()
	plt.clf()
	fig2()
	plt.clf()
	fig3()
	plt.clf()
	fig4()
	plt.clf()
	fig5()
	plt.clf()
	fig6()
	plt.clf()
	fig7()
