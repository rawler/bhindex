from presentation import ItemPresentation

class Presentation(ItemPresentation):
    CRITERIA = {'series': None}

    def getTitle(self):
        return "%(series)s - %(season)sx%(episode)s" % self.asset
