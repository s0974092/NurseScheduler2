from flask import Flask, render_template, redirect, url_for
import os

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/staff')
def staff():
    return render_template('staff.html')

@app.route('/shift')
def shift():
    return render_template('shift.html')

@app.route('/schedule')
def schedule():
    return render_template('schedule.html')

@app.route('/view_schedule')
def view_schedule():
    return render_template('view_schedule.html')

if __name__ == '__main__':
    app.run(debug=True) 