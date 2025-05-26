import lightgbm as lgb
import numpy as np
from sklearn.model_selection import train_test_split


def learner(cmp, label_column, df):
    # if df1[label_column].dtypes != "object":
    #    numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
    #    df1 = df1.select_dtypes(numerics)

    train1, temp1 = train_test_split(df, test_size=0.4, random_state=42, stratify=df[label_column])
    X_train1 = train1.drop(label_column, axis=1)
    y_train1 = train1[label_column]

    X_val1 = temp1.drop(label_column, axis=1)
    y_val1 = temp1[label_column]

    # LightGBM için veri formatı
    train_data1 = lgb.Dataset(X_train1, label=y_train1)
    val_data1 = lgb.Dataset(X_val1, label=y_val1, reference=train_data1)

    params = {
        'objective': 'multiclass',
        'num_class': 3,
        'metric': 'multi_logloss',
        'boosting_type': 'gbdt',
        'learning_rate': 0.1,
        'num_leaves': 31,
        'max_depth': -1,
        'verbose': -1
    }
    model1 = lgb.train(params, train_data1, valid_sets=[val_data1], num_boost_round=100)
    return model1


def predictor(cmp, class_column, model, df):
    if df[class_column].dtypes != "object":
        numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
        df = df.select_dtypes(numerics)
    X_test = df.drop(class_column, axis=1)
    y_test = df[class_column]

    y_pred = model.predict(X_test)
    y_pred_labels = np.argmax(y_pred, axis=1)
    df[f"__#Prediction#__"] = y_pred_labels
    return df
