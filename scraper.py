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
tvdb = tvdb_api.Tvdb()

def imdb_scraper(obj, id):
    if not ia:
        print "WARNING: failed to scrape from imdb due to missing library imdbpy."
        return
    movie = ia.get_movie(id)
    if movie:
        t = time()
        obj.update_key(u'imdb', unicode(id), t)
        obj.update_key(u'rating', unicode(movie['rating']), t)
        obj.update_key(u'name', movie['title'], t)
        obj.update_key(u'image', movie.get('cover url', ()), t)
        obj.update_key(u'year', unicode(movie['year']), t)
        obj.update_key(u'director', (p['name'] for p in movie['director']), t)
        obj.update_key(u'genre', movie['genres'], t)
        obj.update_key(u'actor', (p['name'] for p in movie['cast']), t)
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
    res = False
    def iter_series():
        for series in obj['series']:
            try:
                iter_seasons(tvdb[series])
            except tvdb_api.tvdb_shownotfound:
                pass
    def iter_seasons(series):
        for season in obj['season']:
            try:
                iter_episodes(series, series[int(season)])
            except tvdb_api.tvdb_seasonnotfound:
                pass
    def iter_episodes(series, season):
        for episode in obj['episode']:
            try:
                map(series, season, season[int(episode)])
            except tvdb_api.tvdb_episodenotfound:
                pass

    def map(series, season, episode):
        def trim_split(str, delim='|'):
            return (x for x in str.split(delim) if x)
        t = time()
        def map_item(localName, dict, name, filter=unicode):
            if name in dict and dict[name]:
                obj.update_key(localName, filter(dict[name]), t)

        obj.update_key(u'episode_tvdbid', unicode(episode['id']))

        map_item(u'rating', episode, 'rating')
        map_item(u'episode_name', episode, 'episodename')

        map_item(u'actor', episode, 'gueststars', trim_split)
        map_item(u'director', episode, 'director', trim_split)
        map_item(u'writer', episode, 'writer', trim_split)

        map_item(u'actor', series, 'actors', trim_split)
        map_item(u'genre', series, 'genre', trim_split)
        map_item(u'image', series, 'poster', trim_split)
        map_item(u'rating', series, 'rating', trim_split)
        res = True
    iter_series()
    return res

def scrape_for(obj):
    if 'imdb' in obj:
        return imdb_scraper(obj, obj['imdb'].any())
    elif 'title' in obj and 'year' in obj:
        return imdb_search(obj)
    elif 'series' in obj and 'season' in obj and 'episode' in obj:
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