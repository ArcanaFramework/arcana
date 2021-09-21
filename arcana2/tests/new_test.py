import nest_asyncio
from pydra import Workflow, mark, Submitter


# nest_asyncio.apply()

@mark.task
@mark.annotate({'return': {'out': float}})
def mult(x: float, y: float) -> float:
    return x * y

@mark.task
@mark.annotate({'return': {'out': float}})
def add2(x: float) -> float:
    return x + 2


wf = Workflow(input_spec=["x", "y"], name='workflow', x=1, y=2)
# adding a task and connecting task's input
# to the workflow input
wf.add(mult(name="mlt", x=wf.lzin.x, y=wf.lzin.y))
# adding another task and connecting
# task's input to the "mult" task's output
wf.add(add2(name="add2", x=wf.mlt.lzout.out))
# setting workflow output
wf.set_output([("out", wf.add2.lzout.out)])

with Submitter(plugin="serial") as sub:
    sub(wf)
