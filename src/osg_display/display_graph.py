
import os
try:
    import Image
except ImportError:
    from PIL import Image
import types
import matplotlib
matplotlib.use("Agg")
import matplotlib.figure
import matplotlib.text
import matplotlib.font_manager
import matplotlib.backends.backend_svg
import matplotlib.backends.backend_agg
import matplotlib.ticker as ticker

from matplotlib.pylab import setp
from mpl_toolkits.axes_grid.parasite_axes import HostAxes, ParasiteAxes

from common import log, get_files, commit_files

dpi = 72

def item_name(item, num):
    if isinstance(num, types.IntType):
        return "%s%i" % (item, num)
    return "%s_%s" % (item, num)

class DisplayGraph(object):

    def __init__(self, cp, graph_num):
        self.cp = cp
        self.num = graph_num
        self.format = self.cp.get("Settings", "graph_output").split(",")
        self.format = [i.strip() for i in self.format]
        font_list = [i.strip() for i in self.cp.get("Settings", "Font").\
            split(",")]
        #if 'font.sans-serif' in matplotlib.rcParams:
        #    font_list = font_list + matplotlib.rcParams['font.sans-serif']
        matplotlib.rcParams['font.family'] = 'sans-serif'
        matplotlib.rcParams['font.sans-serif'] = font_list
        fm = matplotlib.font_manager.FontManager()
        prop = matplotlib.font_manager.FontProperties()
        self.legend = self.cp.get("Labels", "Legend").lower() == "true"

        self.num_points = 24

    def build_canvas(self, format="SVG"):
        ylabel = self.cp.get("Labels", item_name("YLabel", self.num))
        if format=="SVG" and "SVG" in self.format:
            FigureCanvas = matplotlib.backends.backend_svg.FigureCanvasSVG
        else:
            FigureCanvas = matplotlib.backends.backend_agg.FigureCanvasAgg

        fig = matplotlib.figure.Figure()
        canvas = FigureCanvas(fig)
        height = int(self.cp.get("Sizes", "height"))
        height_inches = height/float(dpi)
        width = int(self.cp.get("Sizes", "width"))
        width_inches = width/float(dpi)
        self.height, self.width = height, width
        fig.set_size_inches(width_inches, height_inches)
        fig.set_dpi(dpi)
        fig.set_facecolor('white')
        if self.legend:
            ax_rect = (.11, 0.06, .89, .85)
        else:
            label_len = len(ylabel)
            extra = label_len*.006
            if label_len > 5:
                extra += .01
            if label_len < 5:
                extra -= .02
            extra = 0.0
            ax_rect = (.05+extra, 0.06, .91-extra, .86)
        left, right, top, bottom = ax_rect[0], ax_rect[0] + ax_rect[2], \
            ax_rect[1]+ax_rect[3]+.10, ax_rect[1]
        ax = fig.add_axes(ax_rect)
        frame = ax.patch
        frame.set_fill(False)
        ax.grid(True, color='#555555', linewidth=1)

        ax.set_frame_on(False)
        setp(ax.get_xgridlines(), visible=True)
        ax.xaxis.set_major_formatter(ticker.FuncFormatter(self.hour_formatter))

        if self.num_points == 25:
            ax.xaxis.set_ticks((0, 8, 16, 24))

        if ylabel:
            #ylabel_obj = ax.set_ylabel(ylabel)
            #ylabel_obj.set_size(int(self.cp.get("Sizes", "YLabelSize")))
            #ylabel_obj.set_rotation("horizontal")
            ylabel_obj = ax.text(-.05, top, ylabel, horizontalalignment='left',
                verticalalignment='bottom', transform=ax.transAxes)
            ylabel_obj.set_size(int(self.cp.get("Sizes", "YLabelSize")))
        setp(ax.get_yticklabels(), size=int(self.cp.get("Sizes", "YTickSize")))
        setp(ax.get_xticklabels(), size=int(self.cp.get("Sizes", "YTickSize")))
        xticks = ax.get_xticklabels()
        #if xticks:
            #xticks[ 0].set_horizontalalignment("left")
            #xticks[-1].set_horizontalalignment("right")
        setp(ax.get_ygridlines(), linestyle='-')
        setp(ax.get_yticklines(), visible=False)
        ax.xaxis.set_ticks_position("bottom")

        self.ax = ax
        self.canvas = canvas
        self.fig = fig

    def write_graph(self, format="SVG"):
        self.canvas.draw()
        if format == "SVG" and "SVG" in self.format:
            renderer = matplotlib.backends.backend_svg.RendererSVG( \
                self.width, self.height, self.file)
            self.canvas.figure.draw(renderer)
            renderer.finalize()
        else:
            size = self.canvas.get_renderer().get_canvas_width_height()
            buf = self.canvas.tostring_argb()
            im = Image.fromstring("RGBA", (int(size[0]), int(size[1])), buf,
                "raw", "RGBA", 0, 1)
            a, r, g, b = im.split()
            im = Image.merge("RGBA", (r, g, b, a))
            if format == "JPG":
                format = "JPEG"
            im.save(self.file, format=format)

    def draw(self):
        data_len = len(self.data)
        X = range(data_len)
        data = [float(i) for i in self.data]
        color = self.cp.get("Colors", item_name("Line", self.num))
        line = self.ax.plot(X, data, marker="o", markeredgecolor=color,
            markerfacecolor="white", markersize=13, linewidth=5,
            markeredgewidth=5, color=color, label=self.cp.get("Labels",
            item_name("Legend", self.num)))
        max_ax = max(data)*1.1
        if max_ax <= 10:
            self.ax.set_ylim(-0.1, max_ax)
        else:
            self.ax.set_ylim(-0.5, max_ax)
        self.ax.set_xlim(-1, data_len+1)

        if self.mode == "hourly":
            self.ax.xaxis.set_ticks((0, 8, 16, 24))
        elif self.mode == "daily":
            self.ax.xaxis.set_ticks((0, 15, 30))
        elif self.mode == "monthly":
            self.ax.xaxis.set_ticks((0, 4, 8, 12))
        else:
            raise Exception("Unknown time interval mode.")

        if self.legend:
            legend = self.ax.legend(loc=9, mode="expand",
                bbox_to_anchor=(0.25, 1.02, 1., .102))
            setp(legend.get_frame(), visible=False)
            setp(legend.get_texts(), size=int(self.cp.get("Sizes",
                "LegendSize")))

    def parse_data(self):
        self.num_points = len(self.data)

    def hour_formatter(self, x, pos=None):
        if (self.mode == "hourly" and x == 24) or (self.mode == "monthly" \
                and x == 12) or (self.mode == "daily" and x == 30):
            return "Now"
        if self.mode == "monthly":
            return "%i months ago" % (self.num_points-x-1)
        elif self.mode == "daily":
            return "%i days ago" % (self.num_points-x-1)
        return "%i hours ago" % (self.num_points-x-1)

    def run(self, sect, mode="hourly"):

        self.mode = mode
        if self.mode == "monthly":
            self.num_points = 12
        elif self.mode == "daily":
            self.num_points = 31

        self.parse_data()

        for format in self.format:
            name, tmpname = get_files(self.cp, sect, format=format)
            self.build_canvas(format=format)
            self.draw()
            fd = open(tmpname, 'w')
            self.file = fd
            self.write_graph(format=format)
            fd.flush()
            os.fsync(fd)
            commit_files(name, tmpname)

