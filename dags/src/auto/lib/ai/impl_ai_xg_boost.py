import xgboost as xgb


def learner(cmp, label_column, df):
    if df[label_column].dtypes != "object":
        numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
        df = df.select_dtypes(numerics)
    X_train = df.drop(label_column, axis=1)
    y_train = df[label_column]
    data_train = xgb.DMatrix(X_train, label=y_train)
    params = {
        'objective': 'multi:softmax',  # Çok sınıflı sınıflandırma
        'num_class': 3,  # Iris veri setinde 3 sınıf var
        'eval_metric': 'mlogloss'  # Çok sınıflı log loss metriği
    }
    num_round = 100
    model = xgb.train(
        params,
        data_train,
        num_round
    )
    return model


def predictor(cmp, class_column, model, df):
    if df[class_column].dtypes != "object":
        numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
        df = df.select_dtypes(numerics)
    X_test = df.drop(class_column, axis=1)
    y_test = df[class_column]

    dtest = xgb.DMatrix(X_test, label=y_test)
    y_pred = model.predict(dtest)
    df[f"__#Prediction#__"] = y_pred
    return df
