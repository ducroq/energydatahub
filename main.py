import asyncio
import json
import configparser
import logging
import datetime

SETTINGS_FILE_NAME = 'secrets.ini'
LOGGING_FILE_NAME = 'server.log'

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[logging.StreamHandler(), logging.FileHandler(LOGGING_FILE_NAME)]
)

config = configparser.ConfigParser()
config.read(SETTINGS_FILE_NAME)
server_ip = config['server']['ip']
server_port = int(config['server']['port'])

def get_timestamp():
  """Returns the current timestamp in ISO 8601 format (YYYY-MM-DD HH:MM:SS)."""
  now = datetime.datetime.now()
  return now.isoformat()

async def handle_client(reader, writer):
    while True:
        try:
            data = await reader.read(1024)
            if not data:
                break
            logging.info(f"Client request received: {data}")
            request = data.decode().lower()
            if request == "get_data":
                try:
                    price_value_1 = 255.0
                    json_data = {
                        "timestamp": get_timestamp(),
                        "electricity_prices": {
                            "source_1": price_value_1,
                            # ...
                            }
                    }

                    writer.write(json.dumps(json_data).encode())
                except Exception as e:
                    logging.error(f"Error writing data to client: {e}")
            else:
                writer.write(b"Invalid request")
            await writer.drain()
        except Exception as e:
            logging.error(f"Error handling client connection: {e}")
    writer.close()


async def main():
    try:
        server = await asyncio.start_server(handle_client, server_ip, server_port)
        async with server:
            logging.info(f"Server {server_ip}:{server_port} started successfully")
            await server.serve_forever()
    except Exception as e:
        logging.critical(f"Error starting server: {e}")

asyncio.run(main())


