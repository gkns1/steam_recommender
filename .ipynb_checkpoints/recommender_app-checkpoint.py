import flask
from markupsafe import escape

app = flask.Flask(__name__)
app.config["DEBUG"] = True
@app.route('/', methods=['GET', 'POST'])
def home():
    return 'Hello!'
@app.route('/test/login')
def test_login():
    return "ok"
@app.route('/test/<user>')
def test(user):
    return 'Works, %s!' % escape(user)

app.run()