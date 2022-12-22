from arcana.core.analysis.tasks import shell_cmd


def test_shell_cmd(work_dir):

    cp = shell_cmd(
        name="copy",
        executable="cp",
        inputs=[
            {
                "name": "in_dir",
                "datatype": "fileformats.common:Directory",
            }
        ],
        outputs=[
            {
                "name": "out_dir",
                "datatype": "fileformats.common:Directory",
                "position": -1,
            }
        ],
        parameters=[
            {
                "name": "recursive",
                "datatype": bool,
                "argstr": "-R",
                "position": 0,
            }
        ],
    )

    in_dir = work_dir / "source-dir"
    in_dir.mkdir()
    with open(in_dir / "a-file.txt", "w") as f:
        f.write("abcdefg")

    out_dir = work_dir / "dest-dir"

    result = cp(
        in_dir=str(in_dir),
        out_dir=str(out_dir),
        recursive=True,
    )

    assert result.output.out_dir == str(out_dir)
    assert list(p.name for p in out_dir.iterdir()) == ["a-file.txt"]
