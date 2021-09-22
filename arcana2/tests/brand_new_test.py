# import nest_asyncio
# nest_asyncio.apply()

# import pydra

# # functions used later in the notebook:

# @pydra.mark.task
# def add_two(x):
#     return x + 2

# @pydra.mark.task
# def power(a, n=2):
#     return a**n

# @pydra.mark.task
# def mult_var(a, b):
#     return a * b

# wf1 = pydra.Workflow(name="wf1", input_spec=["x"], x=3)

# wf1.add(add_two(name="sum", x=wf1.lzin.x))

# wf1.sum

# wf1.set_output([("out", wf1.sum.lzout.out)])

# with pydra.Submitter(plugin="serial") as sub:
#     sub(wf1)

# wf1.result()