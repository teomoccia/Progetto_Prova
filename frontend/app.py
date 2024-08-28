from flask import Flask, render_template, request, flash, redirect, url_for, make_response, session
from flask_mail import Mail, Message
#from backend.database_querys_Olympics import connect_database
#from ...PW.backend import database_querys_Olympics as Luca
import uuid
import os

from backend import database_querys_Olympics as Luca

app = Flask(__name__)
app.secret_key = os.urandom(24)

# Configurazione Flask-Mail
app.config['MAIL_SERVER'] = 'smtp.gmail.com'
app.config['MAIL_PORT'] = 465
app.config['MAIL_USERNAME'] = 'team.yusuf2024@gmail.com'
app.config['MAIL_PASSWORD'] = 'qnxg qoht cjgy gqst'
app.config['MAIL_USE_TLS'] = False
app.config['MAIL_USE_SSL'] = True

mail = Mail(app)

db = Luca.connect_database("localhost", "root", "", "olympics_2024")


def check_user_credentials(db, username, password):
    cursor = db.cursor()
    query = "SELECT * FROM utente_amazon WHERE username = %s AND password = %s"
    cursor.execute(query, (username, password))
    user = cursor.fetchone()
    cursor.close()
    return user


def admin_required(f):
    def check_admin(*args, **kwargs):
        if session.get('role') != 'admin':
            print("rifiutato")
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return check_admin


@app.route('/')
def index():
    user_id = request.cookies.get('user_id')
    if not user_id:
        user_id = str(uuid.uuid4())

    utenti_scn = list(Luca.select_query(db, "utente_scn", "utente_scn_ID"))
    lista_ids_scn = []
    if user_id not in utenti_scn:
        lista_ids_scn.append(user_id)
        Luca.insert_query(db, "utente_scn", "utente_scn_ID", lista_ids_scn)

    response = make_response(render_template('index.html'))

    if not request.cookies.get('user_id'):
        response.set_cookie('user_id', user_id)
    #email_to_all("newsletter_template.html")
    return response


@app.route('/subscribe', methods=['POST'])
def subscribe():
    email = request.form.get('email')
    emails = [email]
    if not email:
        flash('Please enter a valid email address.', 'danger')
        return redirect(url_for('index'))
    Luca.insert_query(db, "newsletter", "email", emails)

    msg = Message('Benvenuto alla nostra newsletter!',
                  sender='team.yusuf2024@gmail.com',
                  recipients=[email])
    msg.html = render_template('newsletter_template.html')
    try:
        print(msg)
        mail.send(msg)
        flash('Iscrizione avvenuta con successo! Controlla la tua email.', 'success')
    except Exception as e:
        print(e)
        flash('Si è verificato un errore durante l\'invio dell\'email.', 'danger')

    return redirect(url_for('index'))


@app.route("/admin/email")
def email_to_all(newsletter_template):
    email = list(Luca.select_query(db, "newsletter", "email"))
    for elem in email:
        msg = Message("gg", sender='team.yusuf2024@gmail.com', recipients=[elem[0]])
        msg.html = render_template(f'{newsletter_template}')
        mail.send(msg)

    return redirect(url_for('index'))


@app.route('/medagliere')
def medagliere():
    join = {"nazioni": "nazioni_ID"}
    medagliere_list = Luca.select_query(db, "medals_total",
                                        "gold_medal, silver_medal, bronze_medal, total, nazione",
                                        join=join)
    return render_template('medagliere.html', medagliere=medagliere_list)


@app.route('/login')
def login():
    username = request.form['username']
    password = request.form['password']

    utenti = check_user_credentials(db, username, password)

    if utenti:
        session['user_id'] = utenti[0]
        session['username'] = utenti[1]
        session['role'] = utenti[2]
        return redirect(url_for('index'))
    else:
        return 'Username o password errati!'



@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('index'))


@app.route('/admin')
@admin_required
def admin():
    return render_template('admin.html')


@app.route('/atleti')
def atleti():
    join = {
        "athlete": "nazioni_ID",
        "nazioni": "nazioni_ID"
    }
    flags = Luca.select_query(db, "flag_nations", "nazioni.nazioni_ID, flag_link, nazione",
                              join=join, distinct=True)
    return render_template('atleti.html', flags=flags)


@app.route('/atleti/<int:nazioni_ID>')
def atleti_by_nazione(nazioni_ID):
    cursor = db.cursor()
    query = ("""
        SELECT name, nationality, flag_nations.flag_link
        FROM athlete
        JOIN flag_nations ON athlete.nazioni_ID = flag_nations.nazioni_ID
        WHERE flag_nations.nazioni_ID = %s
    """)
    cursor.execute(query, (nazioni_ID,))
    result = cursor.fetchall()
    athletes = [row for row in result]

    return render_template('atleti_by_nazione.html', athletes=athletes, nazioni_ID=nazioni_ID)


if __name__ == "__main__":
    app.run(debug=True)
