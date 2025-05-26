import numpy as np
import pandas as pd
from mlxtend.frequent_patterns import fpgrowth, association_rules
from mlxtend.preprocessing import TransactionEncoder
from pyspark.ml.fpm import FPGrowth
from scipy.stats import zscore
from sklearn import metrics
from sklearn.cluster import KMeans
from sklearn.preprocessing import LabelEncoder


# Other methods
def string_indexer(cmp, df, columns_array, remove_columns):
    category_map = dict()
    for column_name in columns_array:
        label_encoder = LabelEncoder()
        column_name_out = column_name + "_#i"
        df[column_name_out] = label_encoder.fit_transform(df[column_name])
        category_map[column_name] = dict(
            zip(label_encoder.classes_, label_encoder.transform(label_encoder.classes_)))
        if remove_columns:
            df.drop(columns=[column_name], inplace=True)  # drop origin column

    return df, category_map


def correlation_matrix(cmp, df):
    corr_matrix = df.corr()
    return corr_matrix


def association_fpgrowth(cmp, df, object_prop):
    metric = object_prop.get("metric")
    min_support = object_prop.get("min_support")
    min_threshold = object_prop.get("min_threshold")
    use_colnames = object_prop.get("use_colnames")
    verbose = object_prop.get("verbose")

    col_key = object_prop.get("col_key")
    col_group = object_prop.get("col_group")
    col_item = object_prop.get("col_item")
    col_item_list = col_item + "_list"
    col_pred_list = "pred_list"

    df_grp = df.groupby(col_group)[col_item].agg(lambda x: list(dict.fromkeys(x))).reset_index()
    spark_df = cmp.spark.createDataFrame(df_grp)
    fp_growth_spark = FPGrowth(itemsCol=col_item, minSupport=min_support, minConfidence=min_threshold)
    spark_model = fp_growth_spark.fit(spark_df)

    rules = spark_model.associationRules.toPandas()
    rules["antecedent"] = [list(x) for x in rules["antecedent"]]
    rules["consequent"] = [list(x) for x in rules["consequent"]]

    rules.replace([np.inf, -np.inf], np.nan, inplace=True)

    # Uyumsuz tanıları bul (lift < 1 olan kurallar)
    incompatible_rules = rules[rules['lift'] < 1].copy()

    # Sonuçları sırala (lift değerine göre artan sırada)
    incompatible_rules = incompatible_rules.sort_values('lift')

    # Uyumsuz tanıların geçtiği basket'ları bul
    incompatible_diagnoses = set()
    for _, row in incompatible_rules.iterrows():
        incompatible_diagnoses.update(row['antecedent'])
        incompatible_diagnoses.update(row['consequent'])

    # Her basket için uyumsuz tanıları ve lift değerlerini topla
    basket_results = {}
    for _, row in incompatible_rules.iterrows():
        lift_value = row['lift']
        confidence = row['confidence']

        # Antecedent ve consequent'ı birleştir
        all_diagnoses = row['antecedent'] + row['consequent']

        for diagnosis in all_diagnoses:
            # Bu tanının geçtiği tüm kayıtları bul
            diagnosis_records = df[df[col_item] == diagnosis]

            for _, record in diagnosis_records.iterrows():
                basket_id = record[col_group]
                item_id = record[col_key]

                if basket_id not in basket_results:
                    basket_results[basket_id] = {
                        col_group: int(basket_id),
                        col_item_list: []
                    }

                # Bu item_id için prediction_list oluştur
                prediction_list = [
                    {"lift": round((1 - lift_value) * 100, 2)},
                    {"confidence": round(confidence * 100, 2)}
                ]

                # Eğer bu item_id zaten eklenmemişse ekle
                if not any(item[col_item] == item_id for item in basket_results[basket_id][col_item_list]):
                    basket_results[basket_id][col_item_list].append({
                        col_item: int(item_id),
                        col_pred_list: prediction_list
                    })

    result_df = pd.DataFrame(basket_results.values())
    # Dictionary'yi listeye çevir ve basket_id'ye göre sırala
    # result = sorted(basket_results.values(), key=lambda x: x["basket_id"])

    # JSON formatında yazdır
    # print("\nUyumsuz Tanıların Basket Bazlı Analizi:")
    # print("======================================")
    # print(json.dumps(result, indent=2))

    return rules, result_df


def association_fpgrowth_spark(cmp, df, object_prop):
    metric = object_prop.get("metric")
    min_support = object_prop.get("min_support")
    min_threshold = object_prop.get("min_threshold")
    use_colnames = object_prop.get("use_colnames")
    verbose = object_prop.get("verbose")

    col_key = object_prop.get("col_key")
    col_group = object_prop.get("col_group")
    col_item = object_prop.get("col_item")

    df_grp = df.groupby(col_group)[col_item].agg(lambda x: list(dict.fromkeys(x))).reset_index()
    spark_df = cmp.spark.createDataFrame(df_grp)
    fp_growth_spark = FPGrowth(itemsCol=col_item, minSupport=min_support, minConfidence=min_threshold)
    spark_model = fp_growth_spark.fit(spark_df)
    # frq_items = spark_model.freqItemsets.toPandas()

    rules = spark_model.associationRules.toPandas()
    rules["antecedent"] = [list(x) for x in rules["antecedent"]]
    rules["consequent"] = [list(x) for x in rules["consequent"]]

    rules.replace([np.inf, -np.inf], np.nan, inplace=True)
    print("association_rules_fpgrowth finished")
    return rules


def association_fpgrowth_pandas(cmp, df, object_prop):
    metric = object_prop.get("metric")
    min_support = object_prop.get("min_support")
    min_threshold = object_prop.get("min_threshold")
    use_colnames = object_prop.get("use_colnames")
    verbose = object_prop.get("verbose")

    col_key = object_prop.get("col_key")
    col_group = object_prop.get("col_group")
    col_item = object_prop.get("col_item")

    df_grp = df.groupby(col_group).agg({col_item: list}).reset_index()  # col_item mut be unique
    transactions = df_grp[col_item].tolist()
    encoder = TransactionEncoder()
    onehot = encoder.fit(transactions).transform(transactions)
    trans_df = pd.DataFrame(onehot, columns=encoder.columns_)
    frq_items = fpgrowth(
        df=trans_df,
        min_support=min_support,
        use_colnames=use_colnames,
        verbose=verbose
    )
    # frq_items = frq_items.sort_values('support', ascending=False)
    rules = association_rules(
        df=frq_items,
        metric=metric,
        min_threshold=min_threshold
    )
    rules["antecedents"] = [list(x) for x in rules["antecedents"]]
    rules["consequents"] = [list(x) for x in rules["consequents"]]

    rules.replace([np.inf, -np.inf], np.nan, inplace=True)
    print("association_rules_fpgrowth finished")
    return rules


def k_means(cmp, cluster_num, df, max_iter, n_init):
    numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
    df_num = df.select_dtypes(numerics)
    kmeans = KMeans(n_clusters=cluster_num,
                    max_iter=max_iter,
                    n_init=n_init).fit(df_num)
    df[f"__#Prediction#__"] = kmeans.labels_

    return df, kmeans


def score(cmp, df, actual_column, pred_column):
    unique = df[actual_column].unique().tolist()
    dictionary = dict()
    for i, name in enumerate(unique):
        dictionary[name] = i
    actual = df[actual_column].map(dictionary).astype(int)
    predicted = df[pred_column].map(dictionary).astype(int)
    confusion_matrix = metrics.confusion_matrix(actual, predicted)
    # conf. mat
    df_conf_mat = pd.DataFrame(columns=unique, data=confusion_matrix.tolist())
    df_conf_mat["pred column"] = unique
    df_conf_mat.set_index("pred column", inplace=True)
    df_conf_mat.reset_index(inplace=True)
    # metrics
    """
            Accuracy
            Accuracy measures how often the model is correct.
            How to Calculate
            (True Positive + True Negative) / Total Predictions
            """
    Accuracy = metrics.accuracy_score(actual, predicted)

    """
            Precision
            Of the positives predicted, what percentage is truly positive?
            How to Calculate
            True Positive / (True Positive + False Positive)
            Precision does not evaluate the correctly predicted negative cases:
            """
    Precision = metrics.precision_score(actual, predicted, average="macro", zero_division=0.0)

    """
            Sensitivity (Recall)
            Of all the positive cases, what percentage are predicted positive?
            Sensitivity (sometimes called Recall) measures how good the model is at predicting positives.
            This means it looks at true positives and false negatives (which are positives that have been incorrectly predicted as negative).
            How to Calculate
            True Positive / (True Positive + False Negative)
            Sensitivity is good at understanding how well the model predicts something is positive:
            """
    Sensitivity_recall = metrics.recall_score(actual, predicted, average="macro")

    """
            Specificity
            How well the model is at prediciting negative results?
            Specificity is similar to sensitivity, but looks at it from the persepctive of negative results.
            How to Calculate
            True Negative / (True Negative + False Positive)
            Since it is just the opposite of Recall, we use the recall_score function, taking the opposite position label:
            """
    Specificity = metrics.recall_score(actual, predicted, average="macro")

    """
            F-score
            F-score is the "harmonic mean" of precision and sensitivity.
            It considers both false positive and false negative cases and is good for imbalanced datasets.
            How to Calculate
            2 * ((Precision * Sensitivity) / (Precision + Sensitivity))
            This score does not take into consideration the True Negative values:
            """
    F1_score = metrics.f1_score(actual, predicted, average="macro")

    df_metrics = pd.DataFrame(columns=["Accuracy", "Precision", "Sensitivity_recall", "Specificity", "F1_score"],
                              data=[[Accuracy, Precision, Sensitivity_recall, Specificity, F1_score]])
    return df_conf_mat, df_metrics


def z_score(cmp, df):
    df_zscore = df.apply(zscore)
    return df_zscore
