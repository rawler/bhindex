from visualizations import ItemVisualization

class Visualization(ItemVisualization):
    CRITERIA = {'category': 'movies'}

    def getTitle(self):
        return "%(series)s - %(season)sx%(episode)s" % self.item
