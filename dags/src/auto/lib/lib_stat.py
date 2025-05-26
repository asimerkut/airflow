import pandas as pd
import pingouin as pg
from scipy.stats import anderson
from scipy.stats import chi2_contingency
from scipy.stats import f_oneway
from scipy.stats import friedmanchisquare
from scipy.stats import kendalltau
from scipy.stats import kruskal
from scipy.stats import mannwhitneyu
from scipy.stats import normaltest
from scipy.stats import pearsonr
from scipy.stats import shapiro
from scipy.stats import spearmanr
from scipy.stats import ttest_ind
from scipy.stats import ttest_rel
from scipy.stats import wilcoxon
from statsmodels.tsa.stattools import adfuller
from statsmodels.tsa.stattools import kpss


def analysis_of_variance(cmp, object_prop, object_vars, df):
    params = []
    for c in object_prop["column_names"]:
        params.append(df[c])
    stat, p = f_oneway(*params, axis=0)

    data = {"Statistic": [stat], "p-Value": [p], "alpha": object_prop["alpha"],
            "p > alpha": (p > object_prop["alpha"])}
    df = pd.DataFrame(data=data)
    return object_vars, df


def anderson_darling(cmp, object_prop, object_vars, df):
    df = df[object_prop["column_name"]]
    result = anderson(df)

    df_list = []
    for i in range(len(result.critical_values)):
        sl, cv = result.significance_level[i], result.critical_values[i]

        df1 = {'Statistic': [result.statistic],
               'level': [sl],
               "statement": [bool(result.statistic < cv)]}
        df_list.append(df1)
    df = pd.DataFrame(data=df_list)
    return object_vars, df


def augmented_dickey_fuller(cmp, object_prop, object_vars, df):
    df = df[object_prop["column_name"]]

    stat, p, lags, obs, crit, t = adfuller(df)

    df = {'Statistic': [stat],
          'p-Value': [p],
          "alpha": object_prop["alpha"],
          "stationary": (p < object_prop["alpha"])}
    df = pd.DataFrame(data=df)
    return object_vars, df


def chi_squared(cmp, object_prop, object_vars, df):
    df = df[object_prop["column_names"]]

    stat, p, dof, expected = chi2_contingency(df)

    info = {'Statistic': [stat],
            'p-Value': [p],
            "alpha": object_prop["alpha"],
            "dependent": (p > object_prop["alpha"])}
    df = pd.DataFrame(data=info)
    return object_vars, df


def d_agostino_k_squared(cmp, object_prop, object_vars, df):
    df = df[object_prop["column_name"]]
    stat, p = normaltest(df)
    result = {'Statistic': [stat], 'p-Value': [p], "alpha": object_prop["alpha"],
              "p > alpha": (p > object_prop["alpha"])}
    df = pd.DataFrame(data=result)
    return object_vars, df


def friedman(cmp, df, object_prop):
    params = []
    for c in object_prop["column_names"]:
        params.append(df[c])

    stat, p = friedmanchisquare(*params)

    data = {"Statistic": [stat], "p-Value": [p], "alpha": object_prop["alpha"],
            "p > alpha": (p > object_prop["alpha"])}
    df1 = pd.DataFrame(data=data)
    return df1


def kendalls_rank_correlation(cmp, object_prop, object_vars, df):
    df1 = df[object_prop["column_name_1"]]
    df2 = df[object_prop["column_name_2"]]

    stat, p = kendalltau(df1, df2)

    df = {'Statistic': [stat],
          'p-Value': [p],
          "alpha": object_prop["alpha"],
          "dependent": (p > object_prop["alpha"])}
    df = pd.DataFrame(data=df)
    return object_vars, df


def kruskal_wallis_h(cmp, object_prop, object_vars, df):
    params = []
    for c in object_prop["column_names"]:
        params.append(df[c])

    stat, p = kruskal(*params, nan_policy=object_prop["nan_policy"])

    data = {"Statistic": [stat], "p-Value": [p], "alpha": object_prop["alpha"],
            "p > alpha": (p > object_prop["alpha"])}
    df = pd.DataFrame(data=data)
    return object_vars, df


def kwiatkowski_phillips_schmidt_shin(cmp, object_prop, object_vars, df):
    df = df[object_prop["column_name"]]

    stat, p, lags, crit = kpss(df)

    df = {'Statistic': [stat],
          'p-Value': [p],
          "alpha": object_prop["alpha"],
          "stationary": (p > object_prop["alpha"])}
    df = pd.DataFrame(data=df)
    return object_vars, df


def mann_whitney_u(cmp, object_prop, object_vars, df):
    stat, p = mannwhitneyu(x=df[object_prop["column-1"]], y=df[object_prop["column-2"]],
                           use_continuity=object_prop["use_continuity"])

    data = {"Statistic": [stat], "p-Value": [p], "alpha": object_prop["alpha"],
            "p > alpha": (p > object_prop["alpha"])}
    df = pd.DataFrame(data=data)
    return object_vars, df


def paired_student_t(cmp, object_prop, object_vars, df):
    stat, p = ttest_rel(a=df[object_prop["column-1"]], b=df[object_prop["column-2"]],
                        nan_policy=object_prop["nan_policy"])

    data = {"Statistic": [stat], "p-Value": [p], "alpha": object_prop["alpha"],
            "p > alpha": (p > object_prop["alpha"])}
    df = pd.DataFrame(data=data)
    return object_vars, df


def pearson_correlation_coefficient(cmp, object_prop, object_vars, df):
    df1 = df[object_prop["column_name_1"]]
    df2 = df[object_prop["column_name_2"]]

    stat, p = pearsonr(df1, df2)

    df = {'Statistic': [stat],
          'p-Value': [p],
          "alpha": object_prop["alpha"],
          "dependent": (p > object_prop["alpha"])}
    df = pd.DataFrame(data=df)
    return object_vars, df


def repeated_measures_anova(cmp, object_prop, object_vars, df):
    df = pg.rm_anova(dv=object_prop["dv"], within=object_prop["within"], subject=object_prop["subject"],
                     data=df, detailed=object_prop["detailed"], effsize=object_prop["effsize"])
    return object_vars, df


def shapiro_wilk(cmp, object_prop, object_vars, df):
    df = df[object_prop["column_names"]]

    stat, p = shapiro(df)

    df = {'Statistic': [stat], 'p-Value': [p], "alpha": object_prop["alpha"],
          "p > alpha": (p > object_prop["alpha"])}
    df = pd.DataFrame(data=df)
    return object_vars, df


def spearmans_rank_correlation(cmp, object_prop, object_vars, df):
    df1 = df[object_prop["column_name_1"]]
    df2 = df[object_prop["column_name_2"]]

    stat, p = spearmanr(df1, df2)

    df = {'Statistic': [stat],
          'p-Value': [p],
          "alpha": object_prop["alpha"],
          "dependent": (p > object_prop["alpha"])}
    df = pd.DataFrame(data=df)
    return object_vars, df


def student_t(cmp, object_prop, object_vars, df):
    stat, p = ttest_ind(a=df[object_prop["column-1"]], b=df[object_prop["column-2"]])

    data = {"Statistic": [stat], "p-Value": [p], "alpha": object_prop["alpha"],
            "p > alpha": (p > object_prop["alpha"])}
    df = pd.DataFrame(data=data)
    return object_vars, df


def wilcoxon_signed_rank(cmp, object_prop, object_vars, df):
    stat, p = wilcoxon(x=df[object_prop["column-1"]], y=df[object_prop["column-2"]])

    data = {"Statistic": [stat], "p-Value": [p], "alpha": object_prop["alpha"],
            "p > alpha": (p > object_prop["alpha"])}
    df = pd.DataFrame(data=data)
    return object_vars, df
