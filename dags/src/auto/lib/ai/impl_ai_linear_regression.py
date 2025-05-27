from sklearn.linear_model import LinearRegression


def learner(cmp, df, label_column):
    X = df.drop(label_column, axis=1)
    y = df[label_column]
    model = LinearRegression()
    model = model.fit(X, y)
    # y_pred = lr.predict(X)
    # df[f"__#Prediction#__"] = y_pred
    return model


def predictor(cmp, df, model):
    y_pred = model.predict(df)
    df[f"__#Prediction#__"] = y_pred
    return df
