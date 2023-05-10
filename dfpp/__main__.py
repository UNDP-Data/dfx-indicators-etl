import sys
import os
import asyncio
import argparse
from dotenv import load_dotenv
from dfpp.download import retrieval

parser = argparse.ArgumentParser()
parser.add_argument('--run',
                    help='The function to run. options are download, standardise, and publish, or all of the functions together like pipeline')
parser.add_argument('--env', help='Path to the .env file')


async def main():
    args = parser.parse_args()
    if args.env:
        load_dotenv(dotenv_path=args.env)
        connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        container_name = os.getenv('CONTAINER_NAME')
        if args.run == 'download':
            await retrieval(connection_string=connection_string, container_name=container_name)
    else:
        raise Exception('Environment Variable Not Found')


if __name__ == '__main__':
    asyncio.run(main())
