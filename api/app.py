from flask import Flask, jsonify
from pymongo import MongoClient

app = Flask(__name__)

client = MongoClient("mongodb://localhost:27017/")
db = client["projet_data"]

@app.route("/kpis")
def get_kpis():
    data = list(db.kpis.find({}, {"_id": 0}))
    return jsonify(data)

@app.route("/pays")
def get_pays():
    data = list(db.ventes_par_pays.find({}, {"_id": 0}))
    return jsonify(data)

@app.route("/mois")
def get_mois():
    data = list(db.ventes_par_mois.find({}, {"_id": 0}))
    return jsonify(data)

if __name__ == "__main__":
    app.run(debug=True)
