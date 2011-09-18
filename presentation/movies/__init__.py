from presentation import ItemPresentation

class Presentation(ItemPresentation):
    CRITERIA = {'category': 'Movies'}

    def getTitle(self):
        a = self.asset
        if 'title' in a:
            return a['title'].join()
        else:
            return a['name'].join()
