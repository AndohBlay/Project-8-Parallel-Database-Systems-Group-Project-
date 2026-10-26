from flask import Flask, jsonify, request, render_template

from cluster import Cluster, QUERIES

app = Flask(__name__)
cluster = None


def safe(fn):
    try:
        return jsonify(fn())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/cluster")
def api_cluster():
    return safe(cluster.info)


@app.route("/api/queries")
def api_queries():
    return jsonify([
        {"id": k, "title": v["title"], "description": v["description"],
         "plan": v["plan"], "params": v["defaults"]}
        for k, v in QUERIES.items()
    ])


@app.route("/api/query", methods=["POST"])
def api_query():
    body = request.get_json(force=True)
    return safe(lambda: cluster.run(body["query"], body.get("params")))


# ---------------- Step 4: load balancing
@app.route("/api/balance")
def api_balance():
    return safe(cluster.balance_stats)


@app.route("/api/balance/generate", methods=["POST"])
def api_generate():
    body = request.get_json(force=True)
    return safe(lambda: cluster.reload(bool(body.get("skewed"))))


@app.route("/api/balance/measure", methods=["POST"])
def api_measure():
    return safe(cluster.measure)


@app.route("/api/balance/rebalance", methods=["POST"])
def api_rebalance():
    return safe(cluster.rebalance)


if __name__ == "__main__":
    cluster = Cluster()
    cluster.start()
    print("Dashboard: http://127.0.0.1:5001")
    # debug must stay False: the reloader would start the worker processes twice
    app.run(host="127.0.0.1", port=5001, debug=False, threaded=True)