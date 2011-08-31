from visualizations import ItemVisualization

class Visualization(ItemVisualization):
    CRITERIA = {'category': 'Movies'}

    def getTitle(self):
        a = self.asset
        if 'title' in a:
            return a['title'].join()
        else:
            return a['name'].join()
