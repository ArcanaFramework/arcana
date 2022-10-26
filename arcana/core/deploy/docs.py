from pathlib import Path

import yaml

from arcana.core.utils import resolve_class
from arcana.data.formats import Directory


def create_doc(spec, doc_dir, pkg_name, src_file, flatten: bool):
    header = {
        "title": pkg_name,
        "weight": 10,
        "source_file": src_file.as_posix(),
    }

    if flatten:
        out_dir = doc_dir
    else:
        assert isinstance(doc_dir, Path)

        out_dir = doc_dir.joinpath(spec["_relative_dir"])

        assert doc_dir in out_dir.parents or out_dir == doc_dir

        out_dir.mkdir(parents=True, exist_ok=True)

    with open(f"{out_dir}/{pkg_name}.md", "w") as f:
        f.write("---\n")
        yaml.dump(header, f)
        f.write("\n---\n\n")

        f.write("## Package Info\n")
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

        f.write("\n")

        if "licenses" in spec:
            f.write("### Required licenses\n")

            tbl_lic = MarkdownTable(f, "Source file", "Info")
            for lic in spec.get("licenses", []):
                tbl_lic.write_row(
                    escaped_md(lic.get("source", None)),
                    lic.get("info", ""),
                )

            f.write("\n")

        f.write("## Commands\n")

        for cmd in spec["commands"]:

            f.write(f"### {cmd['name']}\n")

            short_desc = cmd.get("long_description", None) or cmd["description"]
            f.write(f"{short_desc}\n\n")

            tbl_cmd = MarkdownTable(f, "Key", "Value")
            tbl_cmd.write_row("Short description", cmd["description"])
            # if cmd.get("configuration"):
            #     config = cmd["configuration"]
            #     # configuration keys are variable depending on the workflow class
            if cmd.get("row_frequency"):
                tbl_cmd.write_row("Operates on", cmd["row_frequency"].title())

            if cmd.get("known_issues"):
                if cmd["known_issues"].get("url"):
                    tbl_cmd.write_row("Known issues", cmd["known_issues"]["url"])
                # Leaving room to extend known_issues further, e.g., an inplace list of issues

            f.write("#### Inputs\n")
            tbl_inputs = MarkdownTable(f, "Name", "Format", "Description")
            if cmd.get("inputs"):
                for inpt in cmd["inputs"]:
                    tbl_inputs.write_row(
                        escaped_md(inpt["name"]),
                        _format_html(inpt.get("stored_format")),
                        inpt.get("description", ""),
                    )
                f.write("\n")

            f.write("#### Outputs\n")
            tbl_outputs = MarkdownTable(f, "Name", "Format", "Description")
            if cmd.get("outputs"):
                for outpt in cmd.get("outputs", []):
                    tbl_outputs.write_row(
                        escaped_md(outpt["name"]),
                        _format_html(outpt.get("stored_format")),
                        outpt.get("description", ""),
                    )
                f.write("\n")

            if cmd.get("parameters"):
                f.write("#### Parameters\n")
                tbl_params = MarkdownTable(f, "Name", "Data type", "Description")
                for param in cmd.get("parameters", []):
                    tbl_params.write_row(
                        escaped_md(param["name"]),
                        escaped_md(param["type"]),
                        param.get("description", ""),
                    )
                f.write("\n")


def _format_html(format):
    if not format:
        return ""
    if ":" not in format:
        return escaped_md(format)

    resolved = resolve_class(format, prefixes=["arcana.data.formats"])
    desc = getattr(resolved, "desc", resolved.__name__)

    if ext := getattr(resolved, "ext", None):
        text = f"{desc} (`.{ext}`)"
    elif getattr(resolved, "is_dir", None) and resolved is not Directory:
        text = f"{desc} (Directory)"
    else:
        text = desc

    return f'<span data-toggle="tooltip" data-placement="bottom" title="{format}" aria-label="{format}">{text}</span>'


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
                f"More entries in row ({len(cols)} than columns ({len(self.headers)})"
            )

        # pad empty column entries if there's not enough
        cols += [""] * (len(self.headers) - len(cols))

        # TODO handle new lines in col
        self.f.write(
            "|" + "|".join(str(col).replace("|", "\\|") for col in cols) + "|\n"
        )
