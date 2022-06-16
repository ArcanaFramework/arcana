
def converter(output_format):
    def decorator(func):
        anot = func.__annotations__
        anot['arcana_converter'] = output_format
        return func
    return decorator


        # wf = Workflow(name=name,
        #               input_spec=['to_convert'],
        #               **kwargs)

        # # Add task collect the input paths to a common directory (as we
        # # assume the converter expects)
        # wf.add(func_task(
        #     access_paths,
        #     in_fields=[('from_format', type), ('file_group', FileGroup)],
        #     out_fields=[(i, str) for i in self.inputs],
        #     name='access_paths',
        #     from_format=self.from_format,
        #     file_group=wf.lzin.to_convert))

        # # Add the actual converter row
        # conv_kwargs = copy(self.task_kwargs)
        # conv_kwargs.update(kwargs)
        # # Map 
        # conv_kwargs.update((self.inputs[i], getattr(wf.access_paths.lzout, i))
        #                     for i in self.inputs)
        # wf.add(self.task(name='converter', **conv_kwargs))

        # wf.add(func_task(
        #     encapsulate_paths,
        #     in_fields=[('to_format', type)] + [(o, str) for o in self.outputs],
        #     out_fields=[('converted', FileGroup)],
        #     name='encapsulate_paths',
        #     to_format=self.to_format,
        #     **{k: getattr(wf.converter.lzout, v)
        #        for k, v in self.outputs.items()}))

        # # Set the outputs of the workflow
        # wf.set_output(('converted', wf.encapsulate_paths.lzout.converted))