import json
from pathlib import Path
import sys
from typing import Optional
import jinja2
import yaml

def gen_template(
    input_file: str,
    output_file: str,
    template_file: Optional[str] = None):
    if input_file.endswith((".yml", ".yaml")):
        with open(input_file, "r") as f:
            data = yaml.load(f, Loader=yaml.SafeLoader)
    elif input_file.endswith(".json"):
        with open(input_file, "r") as f:
            data = json.loads(f.read())
    else:
        sys.exit(f"input_file {input_file} type not supported.")

    if template_file and template_file.suffix != ".j2":
        sys.exit(f"Template file {template_file} format not supported")

    if not template_file:
        template_file = Path("common/templates/feast_repo.j2").absolute()
        template_file = str(template_file)

    with open(template_file, "r") as tfile:
        template_data = tfile.read()

    template = jinja2.Template(template_data, keep_trailing_newline=True)
    config_data = template.render({"input": data})

    online_repo = Path("online_repo/").absolute()
    output_file = f"{str(online_repo)}/{output_file}"

    with open(output_file, "w") as of:
        of.write(config_data)

    print(f"Generated o/p file: {output_file}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        sys.exit("Usage: python <input file> <output file>")
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    gen_template(input_file, output_file)
