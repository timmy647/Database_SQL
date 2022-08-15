# COMP3311 22T1 Ass2 ... get Name's biography/filmography

import sys
import psycopg2

# define any local helper functions here

# set up some globals

usage = "Usage: q3.py 'MovieTitlePattern' [Year]"
db = None
movie = ''
year = -1

# process command-line args

argc = len(sys.argv)
if argc == 2:
	try:
		movie = str(sys.argv[1])
	except:
		print(usage)
		exit(1)
elif argc == 3:
	try:
		movie = str(sys.argv[1])
		year = int(sys.argv[2])
	except:
		print(usage)
		exit(1)
else:
	print(usage)
	exit(1)

# manipulate database

try:
	db = psycopg2.connect("dbname=imdb")
	cur = db.cursor()
	if (year == -1):
		query = "select rating, title, start_year from movies \
				 where title ~* %s \
				 order by rating desc, start_year asc, title asc"
		cur.execute(query,[movie])
	else:
		query = "select rating, title, start_year from movies \
				 where title ~* %s and start_year = %s \
				 order by rating desc, start_year asc, title asc"
		cur.execute(query,[movie, year])
	
	if (cur.rowcount == 0):
		if (year == -1):
			print(f"No movie matching '{movie}'")
		else:
			print(f"No movie matching '{movie}' {year}")
	elif (cur.rowcount > 1):
		if (year == -1):
			print(f"Movies matching '{movie}'")
		else:
			print(f"Movies matching '{movie}' {year}")
		print("===============")
		for tup in cur.fetchall():
			rating, title, year = tup
			print(f"{rating} {title} ({year})")
		
	elif (cur.rowcount == 1):
		rating, title, year = cur.fetchone()
		print(f"{title} ({year})")
		print("===============")
		cur2 = db.cursor()
		query = "select n.name, a.played from acting_roles a \
				 join movies m on (a.movie_id=m.id) \
				 join names n on (a.name_id=n.id) \
				 join principals p on (p.name_id=n.id) \
				 where p.movie_id = m.id \
				 and m.title = %s \
				 and m.start_year = %s \
				 order by p.ordering asc, a.played asc"
		cur2.execute(query, [title, year])
		print("Starring")
		for tup in cur2.fetchall():
			name, role = tup
			print(f" {name} as {role}")
		
		cur3 = db.cursor()
		query = "select n.name, a.role from crew_roles a \
				 join movies m on (a.movie_id=m.id) \
				 join names n on (a.name_id=n.id) \
				 join principals p on (p.name_id=n.id) \
				 where p.movie_id = m.id \
				 and m.title = %s \
				 and m.start_year = %s \
				 order by p.ordering asc, a.role asc"
		cur3.execute(query, [title, year])
		print("and with")
		for tup in cur3.fetchall():
			name, role = tup
			role.replace("_"," ")
			role = role.capitalize()
			print(f" {name}: {role}")
	

except psycopg2.Error as err:
	print("DB error: ", err)
finally:
	if db:
		db.close()
