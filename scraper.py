#!/usr/bin/python

from time import time

try:
    from imdb import IMDb
    ia = IMDb()
except ImportError:
    print "WARNING: failed to load imdb-scraper due to missing library imdbpy."
    ia = None

def imdb_scraper(obj, id):
    if not ia:
        print "WARNING: failed to scrape from imdb due to missing library imdbpy."
        return
    movie = ia.get_movie(id)
    if movie:
        t = time()
        obj.update_key(u'rating', unicode(movie['rating']), t)
        obj.update_key(u'name', movie['title'], t)
        obj.update_key(u'image', movie.get('cover url', ()), t)
        obj.update_key(u'year', unicode(movie['year']), t)
        obj.update_key(u'director', (p['name'] for p in movie['director']), t)
        obj.update_key(u'genre', movie['genres'], t)
        obj.update_key(u'actor', (p['name'] for p in movie['cast']), t)
    else:
        print "Movie not found in IMDB"

def imdb_search(obj):
    if not ia:
        print "WARNING: failed to scrape from imdb due to missing library imdbpy."
        return
    for title in obj['title']:
        movies = ia.search_movie(title)
        for movie in movies:
            year = unicode(movie['year'])
            if year in obj['year']:
                print "IMDB Scraper found match for %s (%s)" % (title, year)
                return imdb_scraper(obj, movie.movieID)

def scrape_for(obj):
    if 'imdb' in obj:
        imdb_scraper(obj, obj['imdb'].any())
    elif 'title' in obj and 'year' in obj:
        imdb_search(obj)

if __name__ == '__main__':
    import db, config, sys

    config = config.read()
    db = db.open(config)

    obj = db[sys.argv[1]]
    if obj:
        scrape_for(obj)
        db.update(obj)
        db.commit()