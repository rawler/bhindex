from presentation import ItemPresentation

class Presentation(ItemPresentation):
    CRITERIA = {'series': None}

    def getTitle(self):
        a = self.asset
        series = a.get('series')
        season = a.get('season')
        episode = a.get('episode')
        epname = a.get('episode_name', '')
        epname = epname and (" - "+epname.any())
        return "%s - %sx%s%s" % (series, season, episode, epname)
