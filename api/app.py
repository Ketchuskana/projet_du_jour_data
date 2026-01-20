from flask import Flask, jsonify
from pymongo import MongoClient

app = Flask(__name__)

client = MongoClient("mongodb://mongo:27017/")
db = client["projet_data"]

@app.route("/kpis")
def kpis():
    return jsonify(list(db.kpis.find({}, {"_id": 0})))

@app.route("/pays")
def pays():
    return jsonify(list(db.ventes_par_pays.find({}, {"_id": 0})))

@app.route("/mois")
def mois():
    return jsonify(list(db.ventes_par_mois.find({}, {"_id": 0})))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
