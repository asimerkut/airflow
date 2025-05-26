import pandas as pd
from sklearn.decomposition import PCA
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis as LDA


def lda(cmp, object_prop, object_vars, df):
    dim = object_prop['dim']
    X = df.drop(columns=object_prop['class_column'])
    y = df[object_prop['class_column']]
    lda = LDA(n_components=dim)
    X_lda = lda.fit_transform(X, y)
    # İndirgenmiş veriyi DataFrame olarak oluşturma
    reduced_df = pd.DataFrame(data=X_lda, columns=[f"LDA_{i + 1}" for i in range(dim)])

    # Hedef sütunu (class_column) koruyarak DataFrame'i döndürme
    reduced_df[object_prop['class_column']] = df[object_prop['class_column']]

    return object_vars, reduced_df, lda


def pca(cmp, object_prop, object_vars, df):
    dim = object_prop['dim']
    X = df.drop(columns=object_prop['class_column'])
    pca = PCA(n_components=dim)
    X_pca = pca.fit_transform(X)
    # İndirgenmiş veriyi DataFrame olarak oluşturma
    reduced_df = pd.DataFrame(data=X_pca, columns=[f"PC{i + 1}" for i in range(dim)])

    # Hedef sütunu (class_column) koruyarak DataFrame'i döndürme
    reduced_df[object_prop['class_column']] = df[object_prop['class_column']]

    return object_vars, reduced_df, pca
