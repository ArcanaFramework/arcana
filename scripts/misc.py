from arcana2.data.sets.bids import BidsDataset, BidsFormat
from arcana2.data.spaces.clinical import Clinical
from arcana2.data.types.general import directory
from arcana2.core.utils import path2name


dataset = BidsDataset.load('/var/folders/mz/yn83q2fd3s758w1j75d2nnw80000gn/T/tmpaegh6xx5/bids')
outputs = {'mriqc': directory}

output_paths = []
data_node = dataset.node(Clinical.session, 'sub-01')
for output_path, output_type in outputs.items():
    dataset.add_sink(path2name(output_path), output_type,
                        path='derivatives/' + output_path)
with dataset.repository:
    for output_name in outputs:
        item = data_node[path2name(output_name)]
        item.get()  # download to host if required
        output_paths.append(item.value)
print(output_paths)
