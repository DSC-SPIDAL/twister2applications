import matplotlib.pyplot as plt
import numpy as np
import os

data = [1000, 2000, 4000, 8000, 16000, 32000]
data = [1000, 2000, 4000, 8000, 16000, 32000]

x_small = [16, 32, 64, 128, 256, 512]
xlabels_large = [16, 32, 64, 128, 256, 512]
xlabels_small = [16, 32, 64, 128, 256, 512]

x_sizes = [1, 2, 4, 8, 16, 32, 64]
xlabels_1_64 = [1, 2, 4, 8, 16, 32, 64]
xlabels_1_400 = [.125, .250, .5, 1, 2, 4]
xlabels_1_32 = [1, 2, 4, 8, 16, 32]
xlabels_1_16 = [1, 2, 4, 8, 16]

markers=["o", "x", "^", "v", "D", "*"]
cls=["khaki", "lightblue", "gray", "chocolate", "darksalmon", "black", "magenta"]
lcls=["navy", "black", "crimson", "green", "magenta", "black", "magenta"]
patterns = [ "/" , "+" , "-" , ">" , "\\" , "|", "o", "O", ".", "*" ]

def plot_line(y=None, x=None, xlabel=None, ylabel=None, title=None, col=None, legend=None, plot=None, logy=False, ylim=None, legendloc=None, ticks=None, ymin=None, ymax=None, mrks=True, y_ticks=None) :
    if not plot:
        p = plt
    else:
        p = plot
    if not col:
        col = lcls

    for i in range(len(y)):
        if logy:
            if mrks:
                p.semilogy(x, y[i], color=col[i], marker=markers[i])
            else:
                p.semilogy(x, y[i], color=col[i], linewidth=2.0)
        else:
            if mrks:
                p.plot(x, y[i], color=col[i], marker=markers[i])
            else:
                p.plot(x, y[i], color=col[i], linewidth=2.0)

    if ylim:
        p.ylim(ylim)
    if not xlabel:
        xlabel = 'message size (KB)'
    p.xlabel(xlabel)
    if ylabel:
        # ylabel = 'time (ms)'
        p.ylabel(ylabel)

    if ymin:
        p.ylim(ymin=ymin)
    if ymax:
        p.ylim(ymax=ymax)
    # if ticks:
    #     p.xticks(np.array([0, 64, 128,256,512]))

    if y_ticks != None and y_ticks.any():
        plt.yticks(y_ticks)

    if title:
        p.title(title)

    for l in y:
        print title, l
    p.grid(True)
    if legend:
        if legendloc:
            p.legend(legend, loc=legendloc, fancybox=True, framealpha=0.25)
        else:
            p.legend(legend, loc="upper left", fancybox=True, framealpha=0.25)
    # p.minorticks_on()
    p.grid(b=True, which='major', color='k', linestyle='-')
    # p.grid(b=True, which='minor', color='grey', linestyle='-', alpha=0.1)
    # p.tight_layout()
    if not plot:
        p.show()
    return plt

def plot_bar(y=None, x=None, xlabel=None, ylabel=None, title=None, col=None, legend=None, plot=None, logy=False, ylim=None, legendloc=None, y_std=None, bar_width=None, n=None, ymax=None) :
    N = 3
    if n:
        N = n
    width = .15
    ind = np.arange(0, .5*N, .5)

    if not plot:
        p = plt
    else:
        p = plot
    if not col:
        col = cls

    l = []
    current_width = 0
    if bar_width:
        width = bar_width
    count = len(y)

    for i in range(len(y)):
        temp = None
        if logy:
            temp = p.bar(ind + current_width, y[i], width, edgecolor='black', color=col[i], hatch=patterns[i], log=logy,bottom=0)
        else:
            if y_std:
                temp = p.bar(ind + current_width, y[i], width, edgecolor='black', color=col[i], yerr=y_std[i], hatch=patterns[i], log=logy, bottom=0)
            else:
                temp = p.bar(ind + current_width, y[i], width, edgecolor='black', color=col[i], hatch=patterns[i], log=logy, bottom=0)
        l.append(temp[0])
        current_width = current_width + width

    p.xticks(ind + width * count / 2, x)

    if ylim:
        p.ylim(ylim)
    if ymax:
        p.ylim(ymax=ymax)
    if not xlabel:
        xlabel = 'message size (KB)'
    p.xlabel(xlabel)
    if ylabel:
        # ylabel = 'time (ms)'
        p.ylabel(ylabel)

    if title:
        p.title(title)

    for l in y:
        print title, l
    # p.grid(True)
    if legend:
        if legendloc:
            p.legend(legend, loc=legendloc, fancybox=True, framealpha=0.25, bbox_to_anchor=(.5, 1.2), ncol=3)
        else:
            p.legend(legend, loc="upper left", fancybox=True, framealpha=0.25, bbox_to_anchor=(0.5, 1.05))
    p.minorticks_on()
    p.grid(b=True, which='major', color='k', linestyle='-', axis='y', alpha=.5)
    # p.grid(b=True, which='minor', color='grey', linestyle='-', alpha=0.1, axis='y')
    p.tight_layout()
    if not plot:
        p.show()
    return plt

def plot_latency_heron():
    heron_partition = [[13,	15,	25,	41,	52,	59,	77],
                       [0.33,	0.36,	0.39,	0.42,	0.56,	0.65,	0.95],
                       [2.88,	2.96,	3.07,	3.4,	5.33,	10.07,	19.99]]

    heron_reduce = [[77,	104,	194,	276,	450,	695,	1217],
                    [0.5,	0.52,	0.56,	0.61,	0.66,	0.776,	0.826],
                    [0.66,	0.95,	1.3,	1.47,	1.88,	2.76,	4.5]]

    heron_broadcast = [[42,	45,	61,	103, 184,	348,	675],
                       [1.3,	1.29,	1.31,	1.3,	1.36,	1.7,	2.1],
                       [2.9,	3,	3.2,	3.7,	5.78,	11.06,	20.95],
                       ]

    fig = plt.figure(figsize=(15, 5), dpi=100)

    plt.subplot2grid((10,15), (0, 0), colspan=5, rowspan=8)
    plot_line(heron_partition, x=x_sizes, title="Latency of Partition", plot=plt, ticks=xlabels_1_64, logy=True, ylabel='Latency (ms) Log', ymax=100)

    plt.subplot2grid((10,15), (0, 5), colspan=5, rowspan=8)
    plot_line(heron_reduce, x=x_sizes, title="Latency of Reduce", plot=plt, ticks=xlabels_1_64, logy=True, ylabel="Latency (ms) Log", ymax=2000)

    plt.subplot2grid((10,15), (0, 10), colspan=5, rowspan=8)
    plot_line(heron_broadcast, x=x_sizes, title="Latency of Broadcast", plot=plt, ticks=xlabels_1_64, logy=True, ylabel="Latency (ms) Log", ymax=1000)

    plt.subplots_adjust(left=0.06, right=0.98, top=0.9, bottom=0.2)

    fig.tight_layout()
    fig = plt.gcf()
    plt.legend(["Heron-1Gbps", "Twister2-IB", "Twister2-1GBps"], fancybox=True, framealpha=0.25, loc="lower center", bbox_to_anchor=(-.7, -.3), ncol=3)
    fig.savefig("/home/supun/data/twister2/pics/heron_latency.png")
    plt.show()

def plot_latency_flink():
    flink_reduce = [[486,	770,	1300,	2430,	5020,	10360],
                    [0.1,	0.18,	0.2,	0.28,	0.43,	0.79],
                    [0.17,	0.2,	0.36,	0.7,	1.3,	2.6]]

    flink_partition = [[45.19,	89.4,	171.9,	362.8,	725.02,	1443],
                    [25.1,	52.5,	109,	205,	342,	522],
                     [223,	445,	820,	1555,	3046,	6102],
                       [210,	405,	779,	1565,	3067,	5870]]

    fig = plt.figure(figsize=(9, 4), dpi=100)
    plt.subplot2grid((10, 16), (0, 0), colspan=8, rowspan=8)
    plot_line(flink_reduce, x=xlabels_1_32, title="Total time Reduce", plot=plt, ticks=xlabels_1_32, logy=True, ylabel="Total time (s) Log", ymax=20000, legendloc="center right")

    plt.subplot2grid((10, 16), (0, 8), colspan=8, rowspan=8)
    # plot_line(flink_partition, x=xlabels_1_32, title="Latency of Partition", plot=plt, ticks=xlabels_1_32, logy=True, ylabel="Total time (s) Log", ymax=10000, legendloc="bottom right")
    plot_bar(flink_partition, x=[1,2,4,8,16,32], xlabel="Parallelism", title="Total time Partition", plot=plt, logy=True, ylabel="time(ms)", bar_width=.075, col=cls, ymax=10000,n=6)

    plt.subplots_adjust(left=0.06, right=0.98, top=0.9, bottom=0.2)

    fig.tight_layout()
    fig = plt.gcf()
    plt.legend(["Flink-IPoIB", "Twister-IB", "Fink-1Gpbs", "Twister-1Gbps"], fancybox=True, framealpha=0.25, loc="lower center", bbox_to_anchor=(-.1, -.35), ncol=4)
    fig.savefig("/home/supun/data/twister2/pics/flink_time.png")
    plt.show()

def plot_bandwidth():
    y_short_large_parallel = [[.117,	1.083,	.842],
                              [.117, 1.092, 3.13],
                              [.117, 1.067,	3.798]]

    fig = plt.figure(figsize=(5, 4), dpi=100)

    plt.subplot2grid((1,8), (0, 0), colspan=8)
    plot_bar(y_short_large_parallel, x=[1,10,40], xlabel="Different networks", legend=["Flink", "Twister2", "MPI"], title="Bandwidth Utilization",plot=plt, ylabel="GB/s")
    plt.subplots_adjust(left=0.06, right=0.98, top=0.9, bottom=0.2)
    fig.tight_layout()
    fig = plt.gcf()
    fig.savefig("/home/supun/data/twister2/pics/bandwidth.png")
    plt.show()

def plot_latency_mpi():
    reduce = [[0.04,	0.08,	0.26,	0.4,	0.95,	1.6, 2.8],
              [0.36,	0.55,	0.87,	1.8,	3.2,	8.5, 16],
              [0.06,	0.13,	0.22,	0.31,	0.57,	1.04,	1.94],
              [0.14,	0.17,	0.28,	0.49,	1.06,	1.94,	4.1]]

    gather = [[0.2,	0.25,	0.3,	0.7,	1.7,	2.5],
              [1.2,	1.5,	1.9,	2.2,	2.6,	3.1],
              [1.3,	1.43,	1.64,	2.03,	3.6,	7.2],
              [1.6,	1.8,	2.1,	2.82,	5.08,	7.9]]
    fig = plt.figure(figsize=(9, 4), dpi=100)
    plt.subplot2grid((10, 16), (0, 0), colspan=8, rowspan=8)
    plot_line(reduce, x=xlabels_1_64, title="Latency of Reduce", plot=plt, ticks=xlabels_1_64, logy=True, ylabel="Total time (s) Log", ymax=20, legendloc="center right")

    plt.subplot2grid((10, 16), (0, 8), colspan=8, rowspan=8)
    plot_line(gather, x=xlabels_1_400, title="Latency of Gather", plot=plt, ticks=xlabels_1_400, logy=True, ylabel="Total time (s) Log", ymax=10, legendloc="bottom right")

    plt.subplots_adjust(left=0.06, right=0.98, top=0.9, bottom=0.2)

    fig.tight_layout()
    fig = plt.gcf()
    plt.legend(["MPI-INT", "MPI-OBJECT", "TWS-INT", "TWS-OBJECT"], fancybox=True, framealpha=0.25, loc="lower center", bbox_to_anchor=(-.1, -.35), ncol=4)
    fig.savefig("/home/supun/data/twister2/pics/mpi_latency.png")
    plt.show()

    # fig = plt.figure(figsize=(9, 4), dpi=100)
    #
    # plt.subplot2grid((10, 16), (0, 0), colspan=8, rowspan=8)
    # plot_line(reduce, x=xlabels_1_64, title="Latency of Reduce", plot=plt, ticks=xlabels_1_64, logy=True, ylabel="Latency (ms)", ymax=20)
    #
    # plt.subplot2grid((10, 16), (0, 8), colspan=8, rowspan=8)
    # plot_line(gather, x=xlabels_1_400, title="Latency of Gather", plot=plt, ticks=xlabels_1_400, logy=True, ylabel="Latency (ms)", ymax=10)
    #
    # plt.subplots_adjust(left=0.06, right=0.98, top=0.9, bottom=0.2)
    #
    # fig.tight_layout()
    # fig = plt.gcf()
    # plt.legend(legend=["MPI-INT", "MPI-OBJECT", "TWS-INT", "TWS-OBJECT"], fancybox=True, framealpha=0.25, loc="lower center", bbox_to_anchor=(-.1, -.35), ncol=4)
    # fig.savefig("/home/supun/data/twister2/pics/mpi_latency.png")
    # plt.show()

def plot_benchmark_latency():
    reduce = [[7.4,	8.4,	9.8,	11.9,	17.1,	25.9,	42.9],
              [4.6,	5,	6.3,	8.7,	14.5,	22.04,	38.2],
              [101,	160,	164,	164,	168,	184,	236],
              [107,	163,	163,	163,	163,	171,	224],
              [482,	520,	594,	672,	714,	921,	1425]]


    fig = plt.figure(figsize=(5, 4), dpi=100)

    plt.subplot2grid((1, 8), (0, 0), colspan=8)
    plot_line(reduce, x=xlabels_1_64, legend=["Twister-IB", "MPI-IB","Twister-10Gbps", "MPI-10Gbps", "Heron-10Gbps"], title="Latency", plot=plt, ticks=xlabels_1_64, logy=True, ylabel=r"Latency ($\mu$s)", ymax=1500, legendloc="right center")

    plt.subplots_adjust(left=0.06, right=0.98, top=0.9, bottom=0.2)

    fig.tight_layout()
    fig = plt.gcf()
    fig.savefig("/home/supun/data/twister2/pics/benchmark_latency.png")
    plt.show()

def plot_kmeans():
    y_short_large = [[4.743,	7.554,	12.721,	22.379,	41.4],
                     [4.132,	6.876,	12.64,	23.921,	40.012],
                     [4.466,	7.172,	13.768,	24.337,	41.17],
                     [4.132,	6.876,	12.64,	23.921,	40.012],
                     [107.61,	140.298,	166.284,	203.07,	349.801]]

    y_short_large_parallel = [[142.128,	73.119,	40.012],
                              [150.813,	76.626,	42.904],
                              [138.395,	74.645,	39.596],
                              [141.75,	75.092,	41.4],
                              [251.652,	327.399,	352.31]]

    fig = plt.figure(figsize=(10, 5), dpi=100)

    plt.subplot2grid((10,16), (0, 0), colspan=8, rowspan=8)
    plot_bar(y_short_large, x=[1,2,4,8,16], xlabel="Centers x 1000", title="K-Means", plot=plt, logy=True, ylabel="time(ms) log", bar_width=.075, col=cls, n=5, ymax=400)


    plt.subplot2grid((10,16), (0, 8), colspan=8, rowspan=8)
    plot_bar(y_short_large_parallel, x=[4,8,16], xlabel="Nodes", title="K-Means", plot=plt, logy=True, ylabel="time(ms) log", bar_width=.075, col=cls, ymax=400)

    plt.subplots_adjust(left=0.06, right=0.98, top=5, bottom=0.2)
    fig.tight_layout()
    fig = plt.gcf()
    plt.legend(["DFW IB", "DFW 10Gbps", "BSP - IB", "BSP - 10Gbps", "Spark - 10Gbps"], fancybox=True, framealpha=0.25, loc="lower center", bbox_to_anchor=(0, -.35), ncol=3)
    fig.savefig("/home/supun/data/twister2/pics/kmeans.png")
    plt.show()


def plot_throughput():
    large = [[122176,	60608,	30048,	14896,	7408,	3408],
             [172048,	127200,	82208,	43008,	22128,	7664],
             [420208,	326080,	188016,	78880,	34144,	13664]]

    small = [[2227536,	2201600,	2156768,	2068432,	1894352,	1566192],
             [2362592,	2324928,	2291040,	2188176,	2053456,	1844528],
             [3205568,	3188000,	3200704,	3070656,	2832448,	2809568]]

    small_parallel = [[1261888,	2068432,	1927104],
                      [1346432,	2188176,	1979488],
                      [1798960,	3070656,	3175296]]
    large_parallel = [[7760,	14896,	13920],
                      [29624,	43008,	41984],
                      [42304,	78880,	96960]]

    fig = plt.figure(figsize=(18, 4), dpi=100)

    plt.subplot2grid((1,35), (0, 0), colspan=8)
    plot_line(large, x=x_small, legend=["TCP", "IPoIB", "IB"], title="a) Top. B Large Messages", plot=plt, ticks=xlabels_large, ylabel="Messages per Sec (log)", logy=True, legendloc="top right")
    # plt.ticklabel_format(style='sci', axis='y', scilimits=(0,0))

    plt.subplot2grid((1,35), (0, 9), colspan=8)
    plot_line(small, x=x_small, xlabel="Message size bytes", legend=["TCP", "IPoIB", "IB"], title="b) Top. B Small Messages", plot=plt, ticks=xlabels_small, ylabel="Messages per Sec", legendloc="top right")
    plt.ticklabel_format(style='sci', axis='y', scilimits=(0,0))

    plt.subplot2grid((1,35), (0, 18), colspan=8)
    plot_bar(large_parallel, x=[8,16,32], xlabel="Parallelism", legend=["TCP", "IPoIB", "IB"], title="c) Top. B Large Messages", plot=plt,ylabel="Messages per Sec")
    plt.ticklabel_format(style='sci', axis='y', scilimits=(0,0))

    plt.subplot2grid((1,35), (0, 27), colspan=8)
    plot_bar(small_parallel, x=[8,16,32], xlabel="Parallelism", legend=["TCP", "IPoIB", "IB"], title="d) Top. B Small Messages", plot=plt, ylabel="Messages per Sec")
    plt.ticklabel_format(style='sci', axis='y', scilimits=(0,0))
    plt.subplots_adjust(left=0.06, right=0.98, top=0.9, bottom=0.2)
    fig.tight_layout()
    fig = plt.gcf()
    fig.savefig("/home/supun/data/heron/pics/throughput.png")
    plt.show()

def main():
    # plot_latency_heron()
    plot_latency_flink()
    plot_latency_mpi()
    # plot_bandwidth()
    # plot_benchmark_latency()
    plot_kmeans()
    # plot_latency_parallel_ib()
    # plot_yahoo_percentages()
    # plot_inflight()
    # plot_throughput()
    # plot_omni()
    # proto_buf()

if __name__ == "__main__":
    main()