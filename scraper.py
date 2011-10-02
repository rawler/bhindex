#!/usr/bin/python

import sys, os.path

from time import time

try:
    from imdb import IMDb
    ia = IMDb()
except ImportError:
    print "WARNING: failed to load imdb-scraper due to missing library imdbpy."
    ia = None

HERE = os.path.dirname(__file__)
sys.path.append(os.path.join(HERE, "tvdb_api"))
import tvdb_api

def imdb_scraper(obj, id):
    if not ia:
        print "WARNING: failed to scrape from imdb due to missing library imdbpy."
        return
    movie = ia.get_movie(id)

    def plot_map(plots):
        if plots:
            return plots[0].rsplit("::", 1)[0]
        else:
            return None

    if movie:
        t = time()
        def map_item(localName, names, filter=unicode):
            if not isinstance(names, tuple):
                names = (names,)
            for name in names:
                val = movie.get(name)
                if val:
                    obj.update_key(localName, filter(movie[name]), t)
                    return True
            print u"No match for %s" % name
        obj.update_key(u'imdb', unicode(id), t)
        map_item(u'rating', 'rating')
        map_item(u'title', 'title')
        map_item(u'image', 'cover url')
        map_item(u'year', 'year')
        map_item(u'genre', 'genres', set)
        map_item(u'plot', ('plot', 'plot outline'), plot_map)
        map_item(u'country', 'countries', set)
        directors = movie.get('director') or movie.get('directors')
        if directors:
            obj.update_key(u'director', (p['name'] for p in directors), t)
        cast = movie.get('cast')
        if cast:
            obj.update_key(u'actor', (p['name'] for p in cast), t)
        return True
    else:
        print "Movie not found in IMDB"
        return False

def imdb_search(obj):
    if not ia:
        print "WARNING: failed to scrape from imdb due to missing library imdbpy."
        return False
    for title in obj['title']:
        movies = ia.search_movie(title)
        for movie in movies:
            year = unicode(movie['year'])
            if year in obj['year']:
                print "IMDB Scraper found match for %s (%s)" % (title, year)
                return imdb_scraper(obj, movie.movieID)
    return False

def tvdb_search(obj):
    lang = obj.get('language')
    tvdb = tvdb_api.Tvdb(language=lang)

    def iter_series():
        res = False
        if 'series_tvdbid' in obj:
            for seriesid in obj['series_tvdbid']:
                try:
                    res = iter_seasons(tvdb[seriesid]) or res
                except tvdb_api.tvdb_shownotfound:
                    pass
        if res: return res
        for series in obj['series']:
            try:
                year = obj.get('year')
                if year:
                    series = "%s (%s)" % (series, year)
                res = iter_seasons(tvdb[series]) or res
            except tvdb_api.tvdb_shownotfound:
                pass
        return res
    def iter_seasons(series):
        res = False
        for season in obj['season']:
            try:
                res = iter_episodes(series, series[int(season)]) or res
            except tvdb_api.tvdb_seasonnotfound:
                pass
        return res
    def iter_episodes(series, season):
        res = False
        for episode in obj['episode']:
            try:
                res = map(series, season, season[int(episode)]) or res
            except tvdb_api.tvdb_episodenotfound:
                pass
        return res
    def map(series, season, episode):
        def trim_split(str, delim='|'):
            return (x for x in str.split(delim) if x)
        def genre_split(str):
            res = set()
            for x in trim_split(str):
                for y in x.split(' and '):
                    res.add(y.strip())
            return res
        t = time()
        def map_item(localName, remote_dict, name, filter=unicode):
            try:
                value = remote_dict[name]
            except tvdb_api.tvdb_attributenotfound:
                return
            if value:
                obj.update_key(localName, filter(value), t)

        obj.update_key(u'episode_tvdbid', unicode(episode['id']))

        map_item(u'rating', episode, 'rating')
        map_item(u'episode_name', episode, 'episodename')

        map_item(u'actor', episode, 'gueststars', trim_split)
        map_item(u'director', episode, 'director', trim_split)
        map_item(u'writer', episode, 'writer', trim_split)

        obj.update_key(u'series_tvdbid', unicode(series['id']))
        map_item(u'actor', series, 'actors', trim_split)
        map_item(u'genre', series, 'genre', genre_split)
        map_item(u'image', series, 'poster', trim_split)
        map_item(u'rating', series, 'rating', trim_split)
        return True

    return iter_series()

def scrape_for(obj):
    if obj.get('imdb'):
        return imdb_scraper(obj, obj['imdb'].any())
    elif obj.get('title') and obj.get('year'):
        return imdb_search(obj)
    elif obj.get('series') and obj.get('season') and obj.get('episode'):
        return tvdb_search(obj)

if __name__ == '__main__':
    import db, config, sys, cliopt

    config = config.read()
    db = db.open(config)

    usage = "usage: %prog [options] [assetid] ..."
    parser = cliopt.OptionParser(usage=usage)
    parser.add_option("-a", "--add", action="append", dest="adds",
                      help="Add a value for an attr for objects, such as '-tname:monkey'. Previous tags for the attribute will be kept.")
    parser.add_option("-s", "--set", action="append", dest="attrs",
                      help="Overwrite an attr tag for objects, such as '-tname:monkey'. Previous tags for the attribute will be removed.")
    (options, args) = parser.parse_args()

    attrs = cliopt.parse_attrs(options.attrs)
    adds = cliopt.parse_attrs(options.adds)

    for arg in args:
        obj = db[arg]

        if obj:
            for k,v in attrs.iteritems():
                obj[k] = v
            for k,v in adds.iteritems():
                obj.update_key(k, v)

            if scrape_for(obj):
                db.update(obj)
                db.commit()