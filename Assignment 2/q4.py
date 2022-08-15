# COMP3311 22T1 Ass2 ... get Name's biography/filmography

import sys
import psycopg2

# define any local helper functions here
def print_life_year(birth, death):
	if (birth == None):
		print("(???)")
	elif (birth != None and death == None):
		print(f"({birth}-)")
	else:
		print(f"({birth}-{death})")

# set up some globals

usage = "Usage: q4.py 'NamePattern' [Year]"
db = None
# person name
p_name = "" 
# person birth year
p_birth = -1

# process command-line args

argc = len(sys.argv)
if argc == 2:
	try:
		p_name = str(sys.argv[1])
	except:
		print(usage)
		exit(1)
elif argc == 3:
	try:
		p_name = str(sys.argv[1])
		p_birth = int(sys.argv[2])
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
	if (p_birth == -1):
		query = "select name, birth_year, death_year from names \
				 where name ~* %s \
				 order by name asc, birth_year asc, id asc"
		cur.execute(query, [p_name])
	else:
		query = "select name, birth_year, death_year from names \
				 where name ~* %s and birth_year = %s \
				 order by name asc, birth_year asc, id asc"
		cur.execute(query, [p_name, p_birth])
	
	if (cur.rowcount == 0):
		if (p_birth == -1):
			print(f"No name matching '{p_name}'")
		else:
			print(f"No name matching '{p_name}' {p_birth}")
	elif (cur.rowcount > 1):
		if (p_birth == -1):
			print(f"Names matching '{p_name}'")
		else:
			print(f"Names matching '{p_name}' {p_birth}")
		print("===============")
		for tup in cur.fetchall():
			name, birth, death = tup
			print(f"{name} ", end='')
			print_life_year(birth, death)

	elif (cur.rowcount == 1):
		tup = cur.fetchone()
		name, birth, death = tup
		print(f"Filmography for {name} ",end='')
		print_life_year(birth, death)
		print("===============")

		cur2 = db.cursor()
		query = "select avg(m.rating) from principals p \
				 join movies m on (p.movie_id=m.id) \
				 join names n on (p.name_id=n.id) \
				 where n.name = %s \
				 group by n.id"
		cur2.execute(query,[name])
		if (cur2.rowcount == 0):
			print("Personal Rating: 0")
		else:
			rating, = cur2.fetchone()
			print(f"Personal Rating: {round(float(rating),1)}")

		cur3 = db.cursor()
		query = "select g.genre, count(g.genre) from principals p \
				 join movies m on (p.movie_id=m.id) \
				 join names n on (p.name_id=n.id) \
				 join movie_genres g on (g.movie_id=m.id) \
				 where n.name = %s \
				 group by g.genre \
				 order by count(g.genre) desc, g.genre asc"
		cur3.execute(query,[name])
		print("Top 3 Genres:")
		i=0
		for tup in cur3.fetchall():
			genre, count = tup
			print(f" {genre}")
			i = i + 1
			if i >= 3: break

		print("===============")
		cur4 = db.cursor()
		query = "select p.movie_id, p.name_id, m.title, m.start_year, \
				(select string_agg(a.played,',') from acting_roles a \
				where a.movie_id = p.movie_id \
				and a.name_id = p.name_id) as played, \
				(select string_agg(c.role,',') from crew_roles c \
				where c.movie_id = p.movie_id \
				and c.name_id = p.name_id) as role \
				from principals p \
				join movies m on (p.movie_id=m.id) \
				join names n on (p.name_id=n.id) \
				where n.name = %s \
				group by p.movie_id, p.name_id, m.title, m.start_year \
				order by m.start_year asc, m.title asc"
		cur4.execute(query,[name])
		for tup in cur4.fetchall():
			movie_id, name_id, title, year, played, role = tup
			print(f"{title} ({year})")
			if (played != None and played != ""):
				played = played.split(",")
				played.sort()
				for p in played:
					print(f" playing {p}")
			if (role != None and role != ""):
				role = role.split(",")
				role.sort()
				for r in role:
					r = r.replace("_"," ")
					print(f" as {r.capitalize() }")

except psycopg2.Error as err:
	print("DB error: ", err)
finally:
	if db:
		db.close()

