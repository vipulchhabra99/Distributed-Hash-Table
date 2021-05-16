#import flask
from flask import Flask, render_template, request
import requests
app = Flask(__name__)


@app.route('/', methods=["GET", "POST"])
def add_key():
    if(request.method == 'POST'):
        result = request.form
        key = result['key']
        value = result['value']
        link = "http://localhost:8080/insert_in_other_node?key=" +key + "&value=" + value
        requests.get(link)

    return render_template("add.html")


@app.route('/search', methods=["GET", "POST"])
def search_key():
    if(request.method == 'POST'):
        result = request.form
        key = result['search_key']
        link = "http://localhost:8080/search_in_other_node?key=" +key
        data = requests.get(link).content.decode("utf-8")
        return render_template("search.html", data=data)
    else:
        return render_template("search.html", data="")


@app.route('/delete', methods=["GET", "POST"])
def delete_key():

    if(request.method == 'POST'):
        result = request.form
        key = result['key']
        link = "http://localhost:8080/delete_in_other_node?key=" +key
        data = requests.get(link).content.decode("utf-8")

    return render_template("delete.html")
    return "<p> Data Deleted Successfully </p>"


@app.route('/update', methods=['GET', 'POST'])
def update():
    if(request.method == 'POST'):
        result = request.form
        key = result['key']
        value = result['value']
        link = "http://localhost:8080/update_in_other_node?key=" +key + "&value=" + value
        data = requests.get(link).content.decode("utf-8")

    return render_template("update.html")


if __name__ == '__main__':
    app.run(debug=True, port = 5000)
