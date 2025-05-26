from sklearn.ensemble import RandomForestClassifier


def learner(cmp, label_column, df):
    if df[label_column].dtypes != "object":
        numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
        df = df.select_dtypes(numerics)
    X = df.drop(label_column, axis=1)
    y = df[label_column]
    model = RandomForestClassifier()
    model = model.fit(X, y)
    # y_pred = rf.predict(X)
    # df[f"__#Prediction#__"] = y_pred
    return model


def predictor(cmp, class_column, df, model):
    if df[class_column].dtypes != "object":
        numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
        df = df.select_dtypes(numerics)
    if class_column is not None:
        X = df.drop(class_column, axis=1)
    else:
        X = df
    y_pred = model.predict(X)
    df[f"__#Prediction#__"] = y_pred
    return df
