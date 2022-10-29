from flask import Flask, request, render_template
from producer import Producer


app = Flask(__name__, template_folder="templates", static_folder="static")

producer = Producer()

@app.route("/", methods=["POST", "GET"])
def index():
    if request.method == 'POST':
        first_name = request.form.get("fname")
        last_name  = request.form.get("lname")
        age  = request.form.get("age")
        job  = request.form.get("job")
        record = first_name + "," + last_name + "," + age + "," + job
        producer.produce(record)
    return render_template("index.html")


if __name__ == "__main__":
    app.run(debug=True)