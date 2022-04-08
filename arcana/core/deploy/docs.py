from pathlib import Path
import yaml
from arcana import __version__


def create_doc(spec, doc_dir, pkg_name, flatten: bool):
    header = {
        "title": spec["package_name"],
        "weight": 10,
        "source_file": pkg_name,
    }

    if flatten:
        out_dir = doc_dir
    else:
        assert isinstance(doc_dir, Path)

        out_dir = doc_dir.joinpath(spec['_relative_dir'])

        assert doc_dir in out_dir.parents

        out_dir.mkdir(parents=True)

    # task = resolve_class(spec['pydra_task'])

    with open(f"{out_dir}/{pkg_name}.md", "w") as f:
        f.write("---\n")
        yaml.dump(header, f)
        f.write("\n---\n\n")

        f.write(f'{spec["description"]}\n\n')

        f.write("### Info\n")
        tbl_info = MarkdownTable(f, "Key", "Value")
        if spec.get("version", None):
            tbl_info.write_row("Version", spec["version"])
        if spec.get("pkg_version", None):
            tbl_info.write_row("App version", spec["pkg_version"])
        # if task.image and task.image != ':':
        #     tbl_info.write_row("Image", escaped_md(task.image))
        if spec.get("base_image", None):  # and task.image != spec["base_image"]:
            tbl_info.write_row("Base image", escaped_md(spec["base_image"]))
        if spec.get("maintainer", None):
            tbl_info.write_row("Maintainer", spec["maintainer"])
        if spec.get("info_url", None):
            tbl_info.write_row("Info URL", spec["info_url"])
        if spec.get("frequency", None):
            tbl_info.write_row("Frequency", spec["frequency"].name.title())

        f.write("\n")

        first_cmd = spec['commands'][0]

        f.write("### Inputs\n")
        tbl_inputs = MarkdownTable(f, "Name", "Bids path", "Data type")
        # for x in task.inputs:
        for x in first_cmd.get('inputs', []):
            name, dtype, path = x
            tbl_inputs.write_row(escaped_md(name), escaped_md(path), escaped_md(dtype))
        f.write("\n")

        f.write("### Outputs\n")
        tbl_outputs = MarkdownTable(f, "Name", "Data type")
        # for x in task.outputs:
        for name, dtype in first_cmd.get('outputs', []):
            tbl_outputs.write_row(escaped_md(name), escaped_md(dtype))
        f.write("\n")

        f.write("### Parameters\n")
        if not first_cmd.get("parameters", None):
            f.write("None\n")
        else:
            tbl_params = MarkdownTable(f, "Name", "Data type")
            for param in spec["parameters"]:
                tbl_params.write_row("Todo", "Todo", "Todo")
        f.write("\n")


def escaped_md(value: str) -> str:
    if not value:
        return ""
    return f"`{value}`"


class MarkdownTable:
    def __init__(self, f, *headers: str) -> None:
        self.headers = tuple(headers)

        self.f = f
        self._write_header()

    def _write_header(self):
        self.write_row(*self.headers)
        self.write_row(*("-" * len(x) for x in self.headers))

    def write_row(self, *cols: str):
        cols = list(cols)
        if len(cols) > len(self.headers):
            raise ValueError(
                f"More entries in row ({len(cols)} than columns ({len(self.headers)})")

        # pad empty column entries if there's not enough
        cols += [""] * (len(self.headers) - len(cols))

        # TODO handle new lines in col
        self.f.write("|" + "|".join(col.replace("|", "\\|") for col in cols) + "|\n")
