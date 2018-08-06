from sklearn import cluster
import numpy as np
from matplotlib import pyplot as plt


def kmeans(infile, outfile):
    data = []
    with open(infile, 'r') as csvfile:
        cap = 10000
        lineno = 0
        for line in csvfile:
            if lineno >= cap:
                break
            lon, lat = line.split(',')
            data.append([float(lon), float(lat)])
            lineno += 1

    X = np.array(data)
    # plt.scatter(X[:, 1], X[:, 0], label='True Position')

    kmeans = cluster.KMeans(n_clusters=3)
    kmeans.fit(X)

    plt.scatter(X[:, 0], X[:, 1], c=kmeans.labels_, cmap='rainbow')
    plt.scatter(
        kmeans.cluster_centers_[:, 0],
        kmeans.cluster_centers_[:, 1],
        color='black')
    plt.xlabel('longitude')
    plt.ylabel('latitude')
    plt.savefig(outfile)


if __name__ == "__main__":
    kmeans('combined.csv', 'b.png')
