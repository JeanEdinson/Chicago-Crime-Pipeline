from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import subprocess

HOST = "0.0.0.0"
PORT = 8000


class SparkHandler(BaseHTTPRequestHandler):

    def do_POST(self):

        if self.path != "/submit":
            self.send_response(404)
            self.end_headers()
            return

        try:

            content_length = int(self.headers["Content-Length"])
            body = self.rfile.read(content_length)

            data = json.loads(body)

            input_file = data["input_file"]
            phase = data["phase"]

            if phase == "bronze":
                command = [
                    "/opt/spark/bin/spark-submit",
                    "--master",
                    "spark://spark-master-chicago-crime:7077",
                    "--deploy-mode",
                    "client",
                    "/opt/spark-app/ingestion/bronze_ingestion.py",
                    "--input",
                    input_file,
                ]
            elif phase == "silver":
                command = [
                    "/opt/spark/bin/spark-submit",
                    "--master",
                    "spark://spark-master-chicago-crime:7077",
                    "--deploy-mode",
                    "client",
                    "/opt/spark-app/transformation/silver_transformation.py"
                ]
            elif phase == "gold":
                command = [
                    "/opt/spark/bin/spark-submit",
                    "--master",
                    "spark://spark-master-chicago-crime:7077",
                    "--deploy-mode",
                    "client",
                    "/opt/spark-app/aggregation/gold_aggregation.py"
                ]

            result = subprocess.run(
                command,
                capture_output=True,
                text=True
            )

            if result.returncode == 0:

                response = {
                    "status": "SUCCESS",
                    "input_file": input_file
                }

                self.send_response(200)

            else:

                response = {
                    "status": "FAILED",
                    "stderr": result.stderr
                }

                self.send_response(500)

        except Exception as e:

            response = {
                "status": "ERROR",
                "message": str(e)
            }

            self.send_response(500)

        self.send_header("Content-Type", "application/json")
        self.end_headers()

        self.wfile.write(
            json.dumps(response).encode()
        )


if __name__ == "__main__":

    server = ThreadingHTTPServer((HOST, PORT), SparkHandler)

    print(f"Spark API escuchando en el puerto {PORT}")

    server.serve_forever()