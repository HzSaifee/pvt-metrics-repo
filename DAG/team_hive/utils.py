import json
import subprocess as sp
from jinja2 import Environment, FileSystemLoader


def render_sql(dag_home, filename, **kwargs):
    """Loads and renders a Jinja2 template SQL file dynamically."""
    env = Environment(loader=FileSystemLoader(dag_home))
    template = env.get_template(filename)
    return template.render(**kwargs)


def run_cli(cmd, fetch_data=False):
    """
    Executes a pharos CLI command.
    If fetch_data=True, parses the stdout as JSON and returns the result.data (CSV).
    """
    result = sp.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"Command failed with code {result.returncode}\nCMD: {cmd}\nSTDERR: {result.stderr}"
        )

    raw_output = result.stdout.strip()
    if fetch_data:
        try:
            parsed = json.loads(raw_output)
            return parsed["result"]["data"]
        except json.JSONDecodeError as e:
            raise RuntimeError(
                f"Failed to parse JSON output. Command: {cmd}\nOutput: {raw_output[:300]}"
            ) from e
    return raw_output


def run_cli_fetch_json(cmd):
    """Executes a pharos CLI command, parses stdout as JSON, returns the result.data (CSV)."""
    return run_cli(cmd, fetch_data=True)
