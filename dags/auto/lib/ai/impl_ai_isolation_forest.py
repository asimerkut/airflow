from sklearn.ensemble import IsolationForest


def learner(cmp, bootstrap, contamination, df, max_features, max_samples, n_estimators, n_jobs,
            random_state, verbose, warm_start, columns):
    if max_samples != 'auto':
        max_samples = float(max_samples)
    if contamination != 'auto':
        contamination = float(contamination)
    iso = IsolationForest(n_estimators=n_estimators, max_samples=max_samples, contamination=contamination,
                          max_features=max_features, bootstrap=bootstrap, n_jobs=n_jobs, random_state=random_state,
                          verbose=verbose, warm_start=warm_start)
    model = iso.fit(df[columns])
    return model


def predictor(cmp, model, df, columns):
    y_pred = model.predict(df[columns])
    df[f"__#Prediction#__"] = y_pred
    return df
