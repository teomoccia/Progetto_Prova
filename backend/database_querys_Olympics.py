from tkinter import messagebox
import mysql.connector
from mysql.connector import errorcode


def create_database(host, user, password, database_name):
    try:
        # Connessione al server MySQL XAMPP localhost
        db = mysql.connector.connect(
            host=host,
            user=user,
            password=password
        )

        cursor = db.cursor()
        query = f"""DROP DATABASE IF EXISTS {database_name}"""

        cursor.execute(query)
        db.commit()
        cursor.close()

        # Creazione del cursore DB
        cursor = db.cursor()

        # Creazione del database se non esiste, utilizzerà i parametri forniti sopra
        cursor.execute("CREATE DATABASE IF NOT EXISTS {}".format(database_name))

        # Chiudo il cursore e la connessione al DB
        cursor.close()
        db.close()
    except mysql.connector.Error:
        messagebox.showerror("errore creazione DataBase", "Il sistema non è riuscito a creare\n"
                                                          "o a connettersi al database")


def connect_database(host, user, password, database):
    try:
        db = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        return db
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("dati non corretti")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database non esiste")
        else:
            print(err)
        return None


def truncate(db, tabella_name):
    cursor = db.cursor()
    query = f""" TRUNCATE {tabella_name} """
    cursor.execute(query)
    db.commit()
    cursor.close()


def crea_tabelle(db, tabella_name, colonna_ID, colonne_FK=None, colonne_aggiuntive=None,
                 tipo_ID=None, Auto_I=None, colonne_aggiuntive_default=None, check=None,
                 ):
    cursor = db.cursor()

    if tipo_ID == "VARCHAR":
        query = f"""
            CREATE TABLE IF NOT EXISTS {tabella_name} (
            {colonna_ID} {tipo_ID}(255) PRIMARY KEY
            """
    else:
        tipo_ID = "INT"
        if Auto_I is None:
            query = f"""
                CREATE TABLE IF NOT EXISTS {tabella_name} (
                {colonna_ID} {tipo_ID} PRIMARY KEY
                """
        else:
            query = f"""
                CREATE TABLE IF NOT EXISTS {tabella_name} (
                {colonna_ID} {tipo_ID} AUTO_INCREMENT PRIMARY KEY
                """
    if colonne_aggiuntive:
        for colonna, tipo in colonne_aggiuntive.items():
            query += f", {colonna} {tipo} NOT NULL"

    if colonne_aggiuntive_default:
        for colonna, tipo in colonne_aggiuntive_default.items():
            query += f", {colonna} {tipo[0]} NOT NULL DEFAULT '{tipo[1]}'"

    if colonne_FK:
        for chiave, valore in colonne_FK.items():
            query += f", {chiave} {valore[0]} NOT NULL"
            query += f", FOREIGN KEY ({chiave}) REFERENCES {valore[1]}({valore[2]}) ON UPDATE CASCADE"

    if check:
        check_list = []
        for condizione in check:
            if isinstance(condizione, tuple) and len(condizione) > 1:
                check_list.append(
                    f"{condizione[0]} IN ({', '.join([f'\'{valore}\'' for valore in condizione[1:]])})")
            else:
                raise ValueError(
                    "Le condizioni check devono essere scritte come check = [('colonna', 'check')] ")
        query += f", CHECK ({' AND '.join(check_list)})"
    query += ")"
    cursor.execute(query)
    db.commit()
    cursor.close()


def insert_query(db, tabella_name, colonne, values):
    cursor = db.cursor()

    percentuali_esse = ', '.join(['%s'] * (len(colonne.split(', '))))
    query_insert = f"""
        INSERT IGNORE INTO {tabella_name} ({colonne})
        VALUES ({percentuali_esse}) 
        """

    # Ensure values are formatted as a list of tuples
    if not all(isinstance(v, (list, tuple)) for v in values):
        values = [(v,) for v in values]

    try:
        cursor.executemany(query_insert, values)
        db.commit()
        #print(f"{cursor.rowcount} rows were inserted.")
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        print(query_insert)

        db.rollback()
    finally:
        cursor.close()


def select_query(db, tabella_name, colonne, join=None, distinct=None, where=None):
    cursor = db.cursor()
    if distinct is None:
        query_select = f"""SELECT {colonne} FROM {tabella_name}"""
    else:
        query_select = f"""SELECT DISTINCT {colonne} FROM {tabella_name}"""

    if join:
        for chiave, valore in join.items():
            query_select += f" JOIN {chiave} ON {tabella_name}.{valore} = {chiave}.{valore}"
    if where:
        query_select += f" WHERE"
        for chiave, valore in where.items():
            query_select += f" {chiave} = %s"
            cursor.execute(query_select, (valore,))
        result = cursor.fetchall()
        result_list = [row for row in result]
        return result_list

    cursor.execute(query_select)
    result = cursor.fetchall()
    cursor.close()
    return result


def insert_N_N(db, tabella_name, colonne, lista, elem_dict, n, diff_value=None):
    sub_tuple_elem = []
    for row in lista:
        if row[n]:
            elem_divisi = row[n].split("|")
            for elem_solo in elem_divisi:
                elem = elem_solo.strip()

                if elem in elem_dict:
                    if diff_value:
                        sub_tuple_elem.append((row[0], elem_dict[elem]))
                    else:
                        sub_tuple_elem.append((elem_dict[elem], row[0]))

    insert_query(db, tabella_name, colonne, sub_tuple_elem)


def fk_disable(db):
    cursor = db.cursor()
    query = """SET FOREIGN_KEY_CHECKS=0;"""
    cursor.execute(query)
    cursor.close()


def alter_table_unique(db, tabella_name, colonne):
    cursor = db.cursor()
    query = f"""ALTER TABLE {tabella_name} ADD UNIQUE ({colonne})"""
    cursor.execute(query)
    cursor.close()

