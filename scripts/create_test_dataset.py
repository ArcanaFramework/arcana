from argparse import ArgumentParser
from arcana.data.stores.medimage.xnat.tests.fixtures import create_dataset_data_in_repo


parser = ArgumentParser()
parser.add_argument('xnat_server',
                    help="The XNAT server to push the test dataset to")
parser.add_argument('dataset_name',
                    help="The name of the dataset to create")
parser.add_argument('alias', help="Username or token alias to access server with")
parser.add_argument('secret', help="Password or token secret to access server with")
args = parser.parse_args()


create_dataset_data_in_repo(args.dataset_name, server=args.xnat_server,
                       user=args.alias, password=args.secret,
                       max_attempts=1)
