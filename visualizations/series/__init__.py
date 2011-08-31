from visualizations import ItemVisualization

class Visualization(ItemVisualization):
    CRITERIA = {'series': None}

    def getTitle(self):
        return "%(series)s - %(season)sx%(episode)s" % self.item
