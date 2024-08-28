import ast
import csv
import re
import database_querys_Olympics as Luca


def check_user_credentials(db, username, password):
    cursor = db.cursor()
    query = "SELECT * FROM utente_amazon WHERE username = %s AND password = %s"
    cursor.execute(query, (username, password))
    user = cursor.fetchone()
    cursor.close()
    return user


def validate_password(password):
    if len(password) < 8:
        return False, "Password più lunga pl0x"
    if not re.search(r"[A-Z]", password):
        return False, "Password maiuscola pl0x"
    if not re.search(r"[a-z]", password):
        return False, "Password minuscola pl0x"
    if not re.search(r"[0-9]", password):
        return False, "Password numerosa pl0x"
    if not re.search(r"[@$!%*?_&]", password):
        return False, "Password carattere disabile pl0x"
    return True, "password valida"

print(__name__)
if __name__ == '__main__':
    Luca.create_database("localhost", "root", "", "olympics_2024")
    db = Luca.connect_database("localhost", "root", "", "olympics_2024")

    input_path = r'C:\Users\Luca\OneDrive\Documenti\Data_Engineer\PW\athletes.csv'
    with open(input_path, encoding="utf-8") as f:
        lettura = csv.reader(f, delimiter=",")
        next(f)
        lista_db_atleta = []
        nazioni = set()
        type_athlete = set()
        gender_set = set()
        for elem in lettura:
            sub_list = elem[0], elem[1], elem[4], elem[5], elem[6], elem[9], elem[14].strip("[]'"), elem[15].strip('[]"'), elem[16]
            nazioni.add(elem[9])
            type_athlete.add(elem[5])
            gender_set.add(elem[4])
            lista_db_atleta.append(sub_list)

    input_path = r'C:\Users\Luca\OneDrive\Documenti\Data_Engineer\PW\events.csv'
    with open(input_path, encoding="utf-8") as f:
        lettura = csv.reader(f, delimiter=",")
        next(f)
        lista_events = []
        for elem in lettura:
            sub_list = elem[0], elem[1], elem[3]
            lista_events.append(sub_list)

    input_path = r'C:\Users\Luca\OneDrive\Documenti\Data_Engineer\PW\medallists.csv'
    with open(input_path, encoding="utf-8") as f:
        lettura = csv.reader(f, delimiter=",")
        next(f)
        lista_medals = set()
        medallist_list = []
        for elem in lettura:
            sub_list = elem[0], elem[1], elem[16]
            medallist_list.append(sub_list)
            lista_medals.add(elem[1])
    input_path = r'C:\Users\Luca\OneDrive\Documenti\Data_Engineer\PW\medals_total.csv'
    with open(input_path, encoding="utf-8") as f:
        lettura = csv.reader(f, delimiter=",")
        next(f)
        total_medals = []
        for elem in lettura:
            sub_list = elem[3], elem[4], elem[5], elem[6], elem[1]
            total_medals.append(sub_list)

    input_path = r'C:\Users\Luca\OneDrive\Documenti\Data_Engineer\PW\schedules.csv'
    with open(input_path, encoding="utf-8") as f:
        lettura = csv.reader(f, delimiter=",")
        next(f)
        schedules_list = []
        status_set = set()
        discipline_set = []
        for elem in lettura:
            sub_list = elem[0], elem[1], elem[2], elem[3], elem[4], elem[6], elem[7], elem[11]
            discipline_sub_list = elem[4], elem[5]
            schedules_list.append(sub_list)
            status_set.add(elem[3])
            discipline_set.append(discipline_sub_list)

        discipline_set = set(discipline_set)
        discipline_set = tuple(discipline_set)

    input_path = r'C:\Users\Luca\OneDrive\Documenti\Data_Engineer\PW\venues.csv'
    with open(input_path, encoding="utf-8") as f:
        lettura = csv.reader(f, delimiter=",")
        next(f)
        venues_list = []
        for elem in lettura:
            sub_list = elem[0], elem[1], elem[2], elem[3]
            venues_list.append(sub_list)


colonne = {
    "discipline_name": "VARCHAR(30)",
    "sport_code": "VARCHAR(30)"
}
Luca.crea_tabelle(db, "disciplines", "discipline_ID", colonne_aggiuntive=colonne, Auto_I=True)
Luca.insert_query(db, "disciplines", "discipline_name, sport_code", discipline_set)

colonne = {
    "status": "VARCHAR(20)"
}
Luca.crea_tabelle(db, "status", "status_ID", colonne_aggiuntive=colonne, Auto_I=True)
Luca.insert_query(db, "status", "status", status_set)

colonne = {
    "gender_name": "VARCHAR(10)"
}
Luca.crea_tabelle(db, "gender", "gender_ID", colonne_aggiuntive=colonne, Auto_I=True)
Luca.insert_query(db, "gender", "gender_name", gender_set)

colonne = {
    "nazione": "VARCHAR(30)"
}
Luca.crea_tabelle(db, "nazioni", "nazioni_ID", colonne_aggiuntive=colonne, Auto_I=True)
Luca.insert_query(db, "nazioni", "nazione", nazioni)

colonne = {
    "type": "VARCHAR(30)"
}
Luca.crea_tabelle(db, "function", "type_ID", colonne_aggiuntive=colonne, Auto_I=True)
Luca.insert_query(db, "function", "type", type_athlete)

colonne = {
    "event": "VARCHAR(30)",
}

colonne_FK = {
    "discipline_ID": ("INT", "disciplines", "discipline_ID")
}

Luca.crea_tabelle(db, "events", "event_ID", colonne_aggiuntive=colonne, colonne_FK=colonne_FK, Auto_I=True)

colonne = {
    "name": "VARCHAR(30)",
    "nationality": "VARCHAR(30)",
    "birth_date": "DATE",
}

colonne_FK = {

    "type_ID": ("INT", "function", "type_ID"),
    "gender_ID": ("INT", "gender", "gender_ID"),
    "nazioni_ID": ("INT", "nazioni", "nazioni_ID"),
    "event_ID": ("INT", "events", "event_ID")
}

Luca.crea_tabelle(db, "athlete", "code", colonne_aggiuntive=colonne, colonne_FK=colonne_FK)
discipline_diz = dict(Luca.select_query(db, "disciplines", "discipline_name, discipline_ID"))
events_completed = []
for elem in lista_events:
    if elem[1].title() in discipline_diz.keys():
        new_elem = list(elem)
        new_elem[1] = discipline_diz[new_elem[1].title()]
        new_elem = new_elem[0], new_elem[1]
        events_completed.append(new_elem)

Luca.insert_query(db, "events", "event, discipline_ID", events_completed)
Luca.alter_table_unique(db, "events", "event, discipline_ID")

colonne = {
    "medal_type": "VARCHAR(30)",
}
Luca.crea_tabelle(db, "medal_type", "medal_type_ID", colonne_aggiuntive=colonne, Auto_I=True)
Luca.insert_query(db, "medal_type", "medal_type", lista_medals)

colonne = {
    "medal_date": "DATE",
}
colonne_FK = {
    "medal_type_ID": ("INT", "medal_type", "medal_type_ID"),
    "code": ("INT", "athlete", "code"),

}
Luca.crea_tabelle(db, "medallist", "medallist_ID", colonne_aggiuntive=colonne, colonne_FK=colonne_FK, Auto_I=True)

colonne = {
    "gold_medal": "INT",
    "silver_medal": "INT",
    "bronze_medal": "INT",
    "total": "INT"
}

colonne_FK = {
    "nazioni_ID": ("INT", "nazioni", "nazioni_ID"),
}
Luca.crea_tabelle(db, "medals_total", "medals_total_ID", colonne_aggiuntive=colonne, colonne_FK=colonne_FK, Auto_I=True)

colonne = {
    "venue_name": "VARCHAR(50)",
    "start_date": "DATE",
    "end_date": "DATE",
}

Luca.crea_tabelle(db, "venue", "venue_ID", colonne_aggiuntive=colonne, Auto_I=True)


io = Luca.select_query(db, "events", "event_ID, event, discipline_ID")
new_discipline_diz = dict(Luca.select_query(db, "disciplines", "discipline_ID, discipline_name"))
new_io = []
for elem in io:
    if elem[2] in new_discipline_diz.keys():
        new_elem = list(elem)
        new_elem[2] = new_discipline_diz[elem[2]]
        new_io.append(new_elem)


athlete_completa = []
naz_diz = dict(Luca.select_query(db, "nazioni", "nazione, nazioni_ID"))
type_diz = dict(Luca.select_query(db, "function", "type, type_ID"))
gender_diz = dict(Luca.select_query(db, "gender", "gender_name, gender_ID"))

for elem in lista_db_atleta:
    for id_value, event, sport in new_io:
        if elem[7] == event and elem[6].lower() == sport.lower():
            new_elem = list(elem)
            new_elem[7] = id_value
            new_elem[6] = id_value
            del new_elem[6]
            new_elem.insert(6, new_elem.pop(7))

            if new_elem[5] in naz_diz.keys():
                new_elem[5] = naz_diz[new_elem[5]]
            if new_elem[3] in type_diz.keys():
                new_elem[3] = type_diz[new_elem[3]]
            if new_elem[2] in gender_diz.keys():
                new_elem[2] = gender_diz[new_elem[2]]

            definitivo = (
                new_elem[0], new_elem[1], new_elem[4], new_elem[6],
                new_elem[3], new_elem[2], new_elem[5], new_elem[7]
            )

            athlete_completa.append(definitivo)
Luca.insert_query(db, "athlete", "code, name, nationality, birth_date, type_ID, gender_ID, nazioni_ID, event_ID", athlete_completa)

medals_diz = dict(Luca.select_query(db, "medal_type", "medal_type, medal_type_ID"))

new_medals_list = []
for elem in medallist_list:
    if elem[1] in medals_diz.keys():
        new_elem = list(elem)
        new_elem[1] = medals_diz[elem[1]]
        new_medals_list.append(new_elem)


Luca.insert_query(db, "medallist", "medal_date, medal_type_ID, code", new_medals_list)
medals_total_db = []
for elem in total_medals:
    if elem[4] in naz_diz.keys():
        new_elem = list(elem)
        new_elem[4] = naz_diz[new_elem[4]]
        medals_total_db.append(new_elem)

Luca.insert_query(db, "medals_total", "gold_medal, silver_medal, bronze_medal, total, nazioni_ID", medals_total_db)

colonne = {
    "start_date": "DATE",
    "end_date": "DATE",
    "day": "DATE",
    "event_medal": "INT",
}

colonne_FK = {
    "venue_ID": ("INT", "venue", "venue_ID"),
    "status_ID": ("INT", "status", "status_ID"),
    "event_ID": ("INT", "events", "event_ID")
}

Luca.crea_tabelle(db, "schedules", "schedule_ID", colonne_aggiuntive=colonne, colonne_FK=colonne_FK,  Auto_I=True)
schedule_complete = []


new_venue_list = []
for elem in venues_list:
    new_elem = elem[0], elem[2], elem[3]
    new_venue_list.append(new_elem)
Luca.insert_query(db, "venue", "venue_name, start_date, end_date", new_venue_list)

status_diz = dict(Luca.select_query(db, "status", "status, status_ID"))
venue_diz = dict(Luca.select_query(db, "venue", "venue_name, venue_ID"))

for elem in schedules_list:
    for id_value, event, sport in new_io:
        if elem[5] == event and elem[4].lower() == sport.lower():
            new_elem = list(elem)
            new_elem[5] = id_value
            new_elem[4] = id_value
            del new_elem[4]
            new_elem.insert(4, new_elem.pop(5))

            if new_elem[3] in status_diz:
                new_elem[3] = status_diz[new_elem[3]]

            new_elem[6] = re.sub(r'South Paris Arena [0-9]', 'South Paris Arena', new_elem[6])
            new_elem[6] = re.sub(r'Chateauroux Shooting Ctr', 'Chateauroux Shooting', new_elem[6])
            if new_elem[6] in venue_diz:
                new_elem[6] = venue_diz[new_elem[6]]
            new_elem = new_elem[0], new_elem[1], new_elem[2], new_elem[4], new_elem[6], new_elem[3], new_elem[5]

            schedule_complete.append(new_elem)

Luca.insert_query(db, "schedules", "start_date, end_date, day, event_medal, venue_ID, status_ID, event_ID", schedule_complete)
venue_mostly_complete = []
for elem in venues_list:

    venue_name = venue_diz[elem[0]]
    events = elem[1]
    events_list = ast.literal_eval(events)

    for event in events_list:
        event_id = discipline_diz.get(event)
        result = (venue_name, event_id)
        venue_mostly_complete.append(result)

venue_sport_list = []
for res in venue_mostly_complete:
    if res[1] is not None:
        venue_sport_list.append(res)

colonne_FK = {
    "venue_ID": ("INT", "venue", "venue_ID"),
    "discipline_ID": ("INT", "disciplines", "discipline_ID")
}

Luca.crea_tabelle(db, "venue_sport", "venue_sport_ID", colonne_FK=colonne_FK, Auto_I=True)
Luca.insert_query(db, "venue_sport", "venue_ID, discipline_ID", venue_sport_list)

Luca.crea_tabelle(db, "utente_scn", "utente_scn_ID", tipo_ID="VARCHAR")


input_path = r'C:\Users\Luca\OneDrive\Documenti\Data_Engineer\PW\flags.csv'
with open(input_path, encoding="utf-8") as f:
    lettura = csv.reader(f, delimiter=";")
    next(f)
    nazion = list(Luca.select_query(db, "nazioni", "nazione"))
    nazion_set = set(n[0] for n in nazion)
    flag_nations = []
    for elem in lettura:
        naz = elem[0].strip().replace("-", " ")
        if naz in nazion_set:
            if naz in naz_diz.keys():
                naz = naz_diz[naz]
                sub_list = elem[1], naz
                flag_nations.append(sub_list)
    colonne = {
        "flag_link": "TEXT"
    }
    colonne_FK = {
        "nazioni_ID": ("INT", "nazioni", "nazioni_ID")
    }
    Luca.crea_tabelle(db, "flag_nations", "flag_ID", colonne_aggiuntive=colonne, colonne_FK=colonne_FK, Auto_I=True)
    Luca.insert_query(db, "flag_nations", "flag_link, nazioni_ID", flag_nations)


colonne = {
    "email": "VARCHAR(30)"
}

Luca.crea_tabelle(db, "newsletter", "email_ID", colonne_aggiuntive=colonne, Auto_I=True)
Luca.alter_table_unique(db, "newsletter", "email")

colonne = {
    "password": "VARCHAR(255)",
}

colonne_default = {
    "role": ("VARCHAR(30)", "utente")
}

check = [('role', 'admin', 'utente')]


Luca.crea_tabelle(db, "account", "username", colonne_aggiuntive=colonne,
                  colonne_aggiuntive_default=colonne_default, check=check, tipo_ID="VARCHAR")
amministratori = [("admin", "admin", "admin")]
Luca.insert_query(db, "account", "username, password, role", amministratori)
