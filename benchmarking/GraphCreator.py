import itertools
import logging

import numpy as np
import pandas as pd
import seaborn as sbn

logger = logging.getLogger(__name__)

logger.setLevel(logging.INFO)


def export_raw_results_to_csv(results, filename):
    # convert results to dataframe
    df = pd.DataFrame(results)

    # export results dataframe to csv
    df.to_csv(filename + ".csv", index=False)

def ci(a, which=95, axis=None):
    """Return a percentile range from an array of values."""
    p = 50 - which / 2, 50 + which / 2
    return np.nanpercentile(a, p, axis)


def process_and_export_results_to_csv(results, filename):
    # convert {'query': [], 'latency': [], 'category': []} map to {'Query': [], 'Postgres': [], 'Min': [], 'Max': [], 'Neo4j': [], 'Min': [], 'Max': []}
    # convert results to dataframe
    df = pd.DataFrame(results)
    old = df
    
    # mean latency per query and category
    df = df.groupby(['query', 'category']).mean().reset_index()

    # add min and max latency per query and category
    df['min'] = old.groupby(['query', 'category']).min().reset_index()['latency']
    df['max'] = old.groupby(['query', 'category']).max().reset_index()['latency']

    # calculate seaborn 95 % confidence interval for latency per query and category
    df['lower_ci'] = old.groupby(['query', 'category']).apply(lambda x: ci(x['latency'])[0]).reset_index()[0]
    df['upper_ci'] = old.groupby(['query', 'category']).apply(lambda x: ci(x['latency'])[1]).reset_index()[0]

    print(df)

    # export results dataframe to dat
    df.to_csv(filename, index=False)
    

def create_latency_scatter_plot(results, x_axis, x_label, title = ""):
    # seaborn scatter plot of results
    sbn.set(style="darkgrid")
    sbn.set_context("poster")

    # print(results)

    # ax = sbn.lineplot(data=results, x=x_axis, y="latency", hue="category", style="category", markers=True)
    ax = sbn.scatterplot(data=results, x=x_axis, y="latency", hue="category", style="category", markers=True)

    if(title != ""):
        ax.set(xlabel=x_label, ylabel='Mean Latency (ms)', title=title)
    else:
        ax.set(xlabel=x_label, ylabel='Mean Latency (ms)')
    
    logger.info("Latency scatter plot created for : " + title)
    return ax


def create_latency_barchart(results, title = ""):
    # seaborn bar plot of results
    sbn.set(style="darkgrid")
    sbn.set_context("poster")

    ax = sbn.barplot(x="query", y="latency", hue="category", data=results, log=True, palette="Set2", capsize=.05,
                     errcolor='gray')
    
    if(title != ""):
        ax.set(xlabel='Query', ylabel='Mean Latency (ms)', title=title)
    else:
        ax.set(xlabel='Query', ylabel='Mean Latency (ms)')

    __show_barchart_values(ax)

    sbn.despine()
    logger.info("Latency barchart created for : " + title)
    return ax


def create_paper_latency_barchart(results,pattern=False):
    # seaborn bar plot of results
    sbn.set(style="darkgrid")
    sbn.set_context("poster")

    ax = sbn.barplot(x="query", y="latency", hue="category", data=results, log=True, capsize=.05, errcolor='gray')
    ax.set(ylabel='Mean Latency (ms)')

    ylabels = ['{:.0f}'.format(y) for y in ax.get_yticks()]
    #print(ylabels)
    ax.set_yticklabels(ylabels)

    if(pattern):
        __give_bars_pattern(results, ax)
    __show_barchart_values(ax)

    sbn.despine()
    logger.info("Latency barchart created for paper format")
    return ax


def __give_bars_pattern(results, ax):
    num_queries = len(np.unique(np.array(results['category'])))
    hatches = itertools.cycle(['/', '//', '+', '-', 'x', '\\', '*', 'o', 'O', '.'])
    for i, bar in enumerate(ax.patches):
        if i % num_queries == 0:
            hatch = next(hatches)
        bar.set_hatch(hatch)

    ax.legend(fancybox=True)


def __show_barchart_values(axs, orient="v", space=.01):
    def _single(ax):
        if orient == "v":
            for p in ax.patches:
                _x = p.get_x() + p.get_width() / 2
                _y = p.get_y() + p.get_height()  # + (p.get_height( ) *0.01)
                if (p.get_height() < 100):
                    value = '{:.1f}'.format(p.get_height())
                else:
                    value = '{:.0f}'.format(p.get_height())
                ax.text(_x, _y, value, ha="center")
        elif orient == "h":
            for p in ax.patches:
                _x = p.get_x() + p.get_width() + float(space)
                _y = p.get_y() + p.get_height() - (p.get_height() * 0.5)
                if (p.get_height() < 100):
                    value = '{:.1f}'.format(p.get_height())
                else:
                    value = '{:.0f}'.format(p.get_height())
                ax.text(_x, _y, value, ha="left")

    if isinstance(axs, np.ndarray):
        for idx, ax in np.ndenumerate(axs):
            _single(ax)
    else:
        _single(axs)
