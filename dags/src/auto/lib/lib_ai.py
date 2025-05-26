import io
import pickle
from datetime import datetime

import matplotlib.pyplot as plt
from sklearn import metrics
from ydata_profiling import ProfileReport

import src.auto as auto
import src.auto.lib.ai.impl_ai as LibAiImpl
import src.auto.lib.ai.impl_ai_association_fpgrowth as LibAiAssociationFpgrowth
import src.auto.lib.ai.impl_ai_decision_tree as LibAiDecisionTree
import src.auto.lib.ai.impl_ai_isolation_forest as LibAiIsolationForest
import src.auto.lib.ai.impl_ai_light_gbm as LibAiLightGbm
import src.auto.lib.ai.impl_ai_linear_regression as LibAiLinearRegression
import src.auto.lib.ai.impl_ai_logistic_regression as LibAiLogisticRegression
import src.auto.lib.ai.impl_ai_random_forest as LibAiRandomForest
import src.auto.lib.ai.impl_ai_tabnet_deep as LibAiTabnetDeep
import src.auto.lib.ai.impl_ai_xg_boost as LibAiXgBoost
import src.auto.env as env
from src.auto.util.enum_util import KeyEnum



# Learner methods
def decision_tree_learner(cmp, object_prop, object_vars, df):
    class_column = object_prop["class_column"]
    model = LibAiDecisionTree.learner(cmp=cmp, df=df, label_column=class_column)
    return object_vars, model


def random_forest_learner(cmp, object_prop, object_vars, df):
    class_column = object_prop["class_column"]
    model = LibAiRandomForest.learner(cmp=cmp, df=df, label_column=class_column)
    return object_vars, model


def linear_regression_learner(cmp, object_prop, object_vars, df):
    target = object_prop["target"]
    model = LibAiLinearRegression.learner(cmp=cmp, df=df, label_column=target)
    return object_vars, model


def logistic_regression_learner(cmp, object_prop, object_vars, df):
    target = object_prop["target"]
    columns = object_prop["columns"]
    model = LibAiLogisticRegression.learner(cmp=cmp, df=df, label_column=target, columns=columns)
    return object_vars, model


def isolation_forest_learner(cmp, object_prop, object_vars, df):
    max_samples = object_prop["max_samples"]
    contamination = object_prop["contamination"]
    n_estimators = object_prop["n_estimators"]
    max_features = object_prop["max_features"]
    bootstrap = object_prop["bootstrap"]
    n_jobs = object_prop["n_jobs"]
    random_state = object_prop["random_state"]
    verbose = object_prop["verbose"]
    warm_start = object_prop["warm_start"]
    columns = object_prop["columns"]

    model = LibAiIsolationForest.learner(cmp=cmp, bootstrap=bootstrap, contamination=contamination, df=df, max_features=max_features,
                                         max_samples=max_samples, n_estimators=n_estimators, n_jobs=n_jobs, random_state=random_state,
                                         verbose=verbose, warm_start=warm_start, columns=columns)

    return object_vars, model


def xg_boost_learner(cmp, object_prop, object_vars, df):
    class_column = object_prop["class_column"]
    model = LibAiXgBoost.learner(cmp=cmp, df=df, label_column=class_column)
    return object_vars, model


def light_gbm_learner(cmp, object_prop, object_vars, df):
    class_column = object_prop["class_column"]
    model = LibAiLightGbm.learner(cmp=cmp, df=df, label_column=class_column)
    return object_vars, model


def tabnet_deep_learner(cmp, object_prop, object_vars, df):
    class_column = object_prop["class_column"]
    model, scaler = LibAiTabnetDeep.learner(cmp=cmp, df=df, label_column=class_column)
    return object_vars, model, scaler


def association_fpgrowth_learner(cmp, object_prop, object_vars, df):
    class_column = object_prop["class_column"]
    rules = LibAiAssociationFpgrowth.learner(cmp=cmp, df=df, label_column=class_column)
    return object_vars, rules


# Predictor methods

def decision_tree_predictor(cmp, object_prop, object_vars, classifier, df):
    class_column = object_prop["class_column"]
    df = LibAiDecisionTree.predictor(cmp=cmp, class_column=class_column, model=classifier, df=df)
    return object_vars, df


def random_forest_predictor(cmp, object_prop, object_vars, classifier, df):
    class_column = object_prop["class_column"]
    df = LibAiRandomForest.predictor(cmp=cmp, class_column=class_column, df=df, model=classifier)
    return object_vars, df


def linear_regression_predictor(cmp, object_prop, object_vars, classifier, df):
    df = LibAiLinearRegression.predictor(cmp=cmp, df=df, model=classifier)
    return object_vars, df


def logistic_regression_predictor(cmp, object_prop, object_vars, classifier, df):
    columns = object_prop["columns"]
    df = LibAiLogisticRegression.predictor(cmp=cmp, df=df, model=classifier, columns=columns)
    return object_vars, df


def isolation_forest_predictor(cmp, object_prop, object_vars, classifier, df):
    columns = object_prop["columns"]
    df = LibAiIsolationForest.predictor(cmp=cmp, model=classifier, df=df, columns=columns)
    return object_vars, df


def xg_boost_predictor(cmp, object_prop, object_vars, classifier, df):
    class_column = object_prop["class_column"]
    df = LibAiXgBoost.predictor(cmp=cmp, class_column=class_column, model=classifier, df=df)
    return object_vars, df


def light_gbm_predictor(cmp, object_prop, object_vars, classifier, df):
    class_column = object_prop["class_column"]
    df = LibAiLightGbm.predictor(cmp=cmp, class_column=class_column, model=classifier, df=df)
    return object_vars, df


def tabnet_deep_predictor(cmp, object_prop, object_vars, classifier, scaler, df):
    class_column = object_prop["class_column"]
    df = LibAiTabnetDeep.predictor(cmp=cmp, class_column=class_column, model=classifier, scaler=scaler, df=df)
    return object_vars, df


def association_fpgrowth_predictor(cmp, object_prop, object_vars, classifier, df):
    class_column = object_prop["class_column"]
    rules = LibAiAssociationFpgrowth.predictor(cmp=cmp, class_column=class_column, model=classifier)
    return object_vars, rules


# Other methods

def string_indexer(cmp, object_prop, object_vars, df):
    columns_array = object_prop["columns_array"]
    remove_columns = object_prop["remove_columns"]
    out_df, var_pdf = LibAiImpl.string_indexer(cmp=cmp, df=df, columns_array=columns_array, remove_columns=remove_columns)
    return object_vars, out_df, var_pdf


def association_fpgrowth(cmp, object_prop, object_vars, df):
    rules, results = LibAiImpl.association_fpgrowth(cmp=cmp, df=df, object_prop=object_prop)
    return object_vars, rules, results


def correlation_matrix(cmp, object_prop, object_vars, df):
    corr_matrix = LibAiImpl.correlation_matrix(cmp=cmp, df=df)
    return object_vars, corr_matrix


def describe(cmp, object_prop, object_vars, df):
    result1 = df.describe(include="all")
    prof_rep = ProfileReport(df, explorative=True, minimal=True) # airflow lib error
    result2 = prof_rep.to_html()
    return object_vars, result1, result2


def k_means(cmp, object_prop, object_vars, df):
    cluster_num = object_prop["cluster_num"]
    max_iter = object_prop["max_iter"]
    n_init = object_prop["n_init"]
    df, kmeans = LibAiImpl.k_means(cmp=cmp, cluster_num=cluster_num, df=df, max_iter=max_iter, n_init=n_init)
    return object_vars, df, kmeans


def roc_curve(cmp, object_prop, object_vars, df):
    test_column = object_prop["test_column"]
    pred_column = object_prop["pred_column"]
    plot_title = object_prop["plot_title"]

    unique = df[test_column].tolist()
    dictionary = dict()
    for i, name in enumerate(unique):
        dictionary[name] = i

    x = df[test_column].map(dictionary).astype(int)
    y = df[pred_column].map(dictionary).astype(int)
    false_positive_rate, true_positive_rate, thresolds = metrics.roc_curve(x, y)
    auc = metrics.roc_auc_score(x, y)

    plt.figure(figsize=(10, 8), dpi=100)
    plt.axis('scaled')
    plt.xlim([0, 1])
    plt.ylim([0, 1])
    plt.title("AUC & ROC Curve")
    plt.plot(false_positive_rate, true_positive_rate, 'g')
    plt.fill_between(false_positive_rate, true_positive_rate, facecolor='lightgreen', alpha=0.7)
    plt.text(0.95, 0.05, 'AUC = %0.4f' % auc, ha='right', fontsize=12, weight='bold', color='blue')

    plt.xlabel("False Positive Rate")
    plt.ylabel("True Positive Rate")

    # SVG
    buf_svg = io.BytesIO()
    plt.savefig(buf_svg, format='svg')
    buf_svg.seek(0)

    plt.close()
    return object_vars, buf_svg.read()


def score(cmp, object_prop, object_vars, df):
    actual_column = object_prop["actual_column"]
    pred_column = object_prop["pred_column"]
    df_conf_mat, df_metrics = LibAiImpl.score(cmp=cmp, df=df, actual_column=actual_column, pred_column=pred_column)
    try:
        print("------------------------------------------------")
        # print(f"Flow   Id : {self.flow_id}")
        print(f"Actual Col: {actual_column}")
        print(f"Pred   Col: {pred_column}")
        print(df_conf_mat.head())
        print(df_metrics.head())
        print("\n")
    except Exception as e:
        raise e
    return object_vars, df_conf_mat, df_metrics


def z_score(cmp, object_prop, object_vars, df):
    df_zscore = LibAiImpl.z_score(cmp=cmp, df=df)
    return object_vars, df_zscore


def model_save(cmp, object_prop, object_vars, model):
    model_cls = model.__class__.__name__ + "_" + datetime.now().strftime("%Y%m%d%S%M")
    print(model_cls)
    if object_prop.get("model_kc") is not None:
        model_kc_arr = object_prop["model_kc"].split(":")
        model_item = auto.get_system_db().get_item_value(model_kc_arr[0], model_kc_arr[1])
    else:
        prm_code = model_cls
        prm_name = model_cls
        model_item = auto.get_system_db().create_item_value(KeyEnum.SMODL, prm_code, prm_name)

    model_path = env.PRM_MODEL_PATH + "/model_" + str(model_item["id"]).replace("-", "") + ".pkl"
    model_data = pickle.dumps(model)
    auto.get_system_fs().write_bin(path=model_path, data=model_data)
    object_vars.update({"var_model_name": model.__class__.__name__})
    return object_vars


def model_load(cmp, object_prop, object_vars):
    model_kc_arr = object_prop["model_kc"].split(":")
    model_item = auto.get_system_db().get_item_value(model_kc_arr[0], model_kc_arr[1])
    model_path = env.PRM_MODEL_PATH + "/model_" + str(model_item["id"]).replace("-", "") + ".pkl"
    model_data = auto.get_system_fs().read_binary(path=model_path)
    model = pickle.loads(model_data)
    object_vars.update({"var_model_name": model.__class__.__name__})
    return object_vars, model
