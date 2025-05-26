from sklearn.linear_model import LogisticRegression


def learner(cmp, df, label_column, columns):
    X = df.drop(label_column, axis=1)
    X = df[columns]
    y = df[label_column]
    model = LogisticRegression()
    model = model.fit(X, y)
    # y_pred = lr.predict(X)
    # df[f"__#Prediction#__"] = y_pred
    return model


def predictor(cmp, df, model, columns):
    y_pred = model.predict(df[columns])
    df[f"__#Prediction#__"] = y_pred
    return df
