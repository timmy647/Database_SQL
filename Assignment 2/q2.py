# COMP3311 22T1 Ass2 ... print info about different releases for Movie

import sys
import psycopg2

# define any local helper functions here
movie = ''
title = ''
year = ''

# set up some globals

usage = "Usage: q2.py 'PartialMovieTitle'"
db = None

# process command-line args

argc = len(sys.argv)
if argc == 2:
	movie = str(sys.argv[1])
else:
	print(usage)
	exit(1)

# manipulate database

try:
	db = psycopg2.connect("dbname=imdb")
	cur = db.cursor()

	query = "select start_year, title from movies where title ~* %s"
	cur.execute(query,[movie])
	if (cur.rowcount == 0):
		print(f"No movie matching '{movie}'")
	elif (cur.rowcount != 1):
		query = 'select rating, title, start_year from movies where title ~* %s order by rating desc, start_year asc, title asc'
		cur2 = db.cursor()
		cur2.execute(query,[movie])
		if (cur2.rowcount == 0):
			print(f"No movie matching '{movie}'")
		else:
			print(f"Movies matching '{movie}'")
			print("===============")
			for tup in cur2.fetchall():
				rating, title, year = tup
				print(f"{rating} {title} ({year})")
	else:
		for tup in cur.fetchall():
			year, title = tup
			query = "select a.local_title, a.region, a.language, a.extra_info from aliases a join movies m on (a.movie_id=m.id) where m.title ~* %s order by a.ordering asc"
			cur3 = db.cursor()
			cur3.execute(query, [title])
			if (cur3.rowcount == 0):
				print(f"{title} ({year}) has no alternative releases")
			else:
				print(f"{title} ({year}) was also released as")
				for tup3 in cur3.fetchall():
					local_title, region, language, extra = tup3
					print(f"'{local_title}' ", end='')
					if (region != None or language != None):
						print("(", end='')
						print(f"region: {str(region).strip()}" if region!=None else '', end='')
						print(f", language: {str(language).strip()}" if language!=None else '', end='')
						print(")")
					elif (extra != None):
						print(f"({str(extra).strip()})")

except psycopg2.Error as err:
	print("DB error: ", err)
finally:
	if db:
		db.close()
