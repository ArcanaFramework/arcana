from arcana.tasks.common import shell_cmd


def test_shell_cmd(work_dir):

    task = shell_cmd(
        name="copy",
        executable="cp",
        inputs=[
            {
                "name": "in_dir",
                "format": "common:Text",
            }
        ],
        outputs=[
            {
                "name": "out_dir",
                "format": "common:Text",
                "position": -1,
            }
        ],
        parameters=[
            {
                "name": "recursive",
                "type": bool,
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

    result = task(
        in_dir=in_dir,
        out_dir=out_dir,
        recursive=True,
    )

    assert result.output.out_dir == out_dir
    assert list(p.name for p in out_dir.iterdir()) == ["a-file.txt"]
