from visualizations import ItemVisualization

class Visualization(ItemVisualization):
    CRITERIA = {'category': 'movies'}

    def getTitle(self):
        a = self.asset
        if 'title' in a:
            return a['title']
        else:
            return a['name']
