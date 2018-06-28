import argparse
from agave.agave_s3 import AgaveS3
from sbh.synbiohub_proxy import SynBioHubProxy


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--plan_uri", help="the plan URI")
    parser.add_argument("-s", "--step_id", help="the plan step")
    args = parser.parse_args()

    sbh = SynBioHubProxy()
    # TODO: fail if cannot connect to SBH
    operator = sbh.get_operator(args.plan_uri, args.step_id)
    # TODO: fail on error

    # TODO: mangle bucket path and revise AgaveS3 to use prefix
    s3 = AgaveS3()
    # TODO: fail on error

    uplo

if __name__ == "__main__":
    main()
