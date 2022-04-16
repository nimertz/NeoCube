import logging

import numpy as np
import seaborn as sbn

logger = logging.getLogger(__name__)

def create_barchart(title, results):
    # seaborn bar plot of results
    sbn.set(style="darkgrid")

    ax = sbn.barplot(x="query", y="latency" ,hue="category", data=results, log=True ,palette="Set2", capsize=.05)
    ax.set(xlabel='Query', ylabel='Mean Latency (ms) - Log scale', title=title)

    show_barchart_values(ax)

    sbn.despine()
    logger.info("Latency barchart created for : " + title)
    return ax

def show_barchart_values(axs, orient="v", space=.01):
    def _single(ax):
        if orient == "v":
            for p in ax.patches:
                _x = p.get_x() + p.get_width() / 1.5 # change to move text to the right
                _y = p.get_y() + p.get_height() + (p.get_height( ) *0.01)
                value = '{:.1f}'.format(p.get_height())
                ax.text(_x, _y, value, ha="left")
        elif orient == "h":
            for p in ax.patches:
                _x = p.get_x() + p.get_width() + float(space)
                _y = p.get_y() + p.get_height() - (p.get_height( ) *0.5)
                value = '{:.1f}'.format(p.get_width())
                ax.text(_x, _y, value, ha="left")

    if isinstance(axs, np.ndarray):
        for idx, ax in np.ndenumerate(axs):
            _single(ax)
    else:
        _single(axs)