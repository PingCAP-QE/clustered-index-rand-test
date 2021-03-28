import subprocess
import os
import shlex
import sys

sql_count = sys.argv[1]

env = os.environ
env["PATH"] = "tests/_utils:bin:" + env["PATH"]

openPorts = subprocess.run(shlex.split(
    "lsof -n -P -i :4001 -i :10081 -i :4002 -i :10082"
    ), env=env, capture_output=True).stdout

if len(openPorts.splitlines()) < 4 + 1:
    subprocess.run("stop_services", env=env)
    subprocess.run("start_services", env=env)

sqls = subprocess.run(shlex.split(f"clustered-index-rand-test print --count {sql_count}"),
    env=env, capture_output=True).stdout.decode("utf-8")

for port in ["4001", "4002"]:
    for line in sqls.splitlines():
        result = subprocess.run(shlex.split(
            f"mysql -h 127.0.0.1 -u root -b test -P \"{port}\" "
            f"--comments --local-infile=1 -e \"{line}\""),
            env=env, capture_output=True).stderr.decode("utf-8")
        if "ERROR 1064" in result:
            raise Exception(f"port: {port}\nsql: {line}\nresult: {result}\n")

print(f"Generated {sql_count} SQLs, no syntax error found.")
