import io

import matplotlib.pyplot as plt
import seaborn as sns


def box_plot(cmp, object_prop, object_vars, df):
    sns.boxplot(data=df, x=object_prop["x"], y=object_prop["y"], hue=object_prop["hue"])

    # SVG
    buf_svg = io.BytesIO()
    plt.savefig(buf_svg, format='svg')
    buf_svg.seek(0)

    plt.close()
    svg = buf_svg.read()
    return object_vars, svg


def cluster_map(cmp, object_prop, object_vars, df):
    numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
    df = df.select_dtypes(numerics)
    sns.clustermap(df)

    # SVG
    buf_svg = io.BytesIO()
    plt.savefig(buf_svg, format='svg')
    buf_svg.seek(0)

    plt.close()
    svg = buf_svg.read()
    return object_vars, svg


def dist_plot(cmp, object_prop, object_vars, df):
    numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
    df = df.select_dtypes(numerics)
    sns.displot(x=object_prop["x"], data=df)

    # SVG
    buf_svg = io.BytesIO()
    plt.savefig(buf_svg, format='svg')
    buf_svg.seek(0)

    plt.close()
    svg = buf_svg.read()
    return object_vars, svg


def heat_map(cmp, object_prop, object_vars, df):
    numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
    df = df.select_dtypes(numerics)
    sns.heatmap(df)

    # SVG
    buf_svg = io.BytesIO()
    plt.savefig(buf_svg, format='svg')
    buf_svg.seek(0)

    plt.close()
    svg = buf_svg.read()
    return object_vars, svg


def violin_plot(cmp, object_prop, object_vars, df):
    numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
    sns.violinplot(x=object_prop["x"], y=object_prop["y"], hue=object_prop["hue"], data=df)

    # SVG
    buf_svg = io.BytesIO()
    plt.savefig(buf_svg, format='svg')
    buf_svg.seek(0)

    plt.close()
    svg = buf_svg.read()
    return object_vars, svg


def scatter_plot(cmp, object_prop, object_vars, df):
    sns.scatterplot(x=object_prop["x"], y=object_prop["y"], hue=object_prop["hue"], data=df)

    # SVG
    buf_svg = io.BytesIO()
    plt.savefig(buf_svg, format='svg')
    buf_svg.seek(0)

    plt.close()
    svg = buf_svg.read()
    return object_vars, svg


def pie_plot(cmp, object_prop, object_vars, df):
    numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
    df = df.select_dtypes(numerics)
    df[object_prop["column"]].value_counts().plot.pie(autopct='%1.1f%%')
    plt.title(object_prop["plot_name"])

    # SVG
    buf_svg = io.BytesIO()
    plt.savefig(buf_svg, format='svg')
    buf_svg.seek(0)

    plt.close()
    svg = buf_svg.read()
    return object_vars, svg


def line_plot(cmp, object_prop, object_vars, df):
    numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
    df = df.select_dtypes(numerics)
    sns.lineplot(x=object_prop["x"], y=object_prop["y"], data=df)

    # SVG
    buf_svg = io.BytesIO()
    plt.savefig(buf_svg, format='svg')
    buf_svg.seek(0)

    plt.close()
    svg = buf_svg.read()
    return object_vars, svg


def histogram(cmp, object_prop, object_vars, df):
    numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
    df = df.select_dtypes(numerics)
    sns.histplot(x=object_prop["x"], y=object_prop["y"], hue=object_prop["hue"], data=df)

    # SVG
    buf_svg = io.BytesIO()
    plt.savefig(buf_svg, format='svg')
    buf_svg.seek(0)

    plt.close()
    svg = buf_svg.read()
    return object_vars, svg
