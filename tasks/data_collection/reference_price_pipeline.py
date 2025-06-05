import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict

import aiohttp
import paho.mqtt.client as mqtt

from core.services.timescale_client import TimescaleClient
from core.task_base import BaseTask

logging.basicConfig(level=logging.INFO)


class ReferencePricePipeline(BaseTask):
    """Fetch reference prices from broker endpoints, store them in TimescaleDB and
    publish each price update to an MQTT topic."""

    def __init__(self, name: str, frequency: timedelta, config: Dict[str, Any]):
        super().__init__(name, frequency, config)
        self.endpoints = config["endpoints"]
        self.symbol = config.get("symbol", "BTCARS")
        self.mqtt_broker = config.get("mqtt_broker", "localhost")
        self.mqtt_port = config.get("mqtt_port", 1883)
        self.mqtt_topic = config.get("mqtt_topic", "ar/reference_price")
        self.ts_client = TimescaleClient(
            host=config["timescale_config"].get("host", "localhost"),
            port=config["timescale_config"].get("port", 5432),
            user=config["timescale_config"].get("user", "admin"),
            password=config["timescale_config"].get("password", "admin"),
            database=config["timescale_config"].get("database", "timescaledb"),
        )
        self.mqtt_client = mqtt.Client()
        self.mqtt_client.connect(self.mqtt_broker, self.mqtt_port)

    async def execute(self):
        await self.ts_client.connect()
        async with aiohttp.ClientSession() as session:
            records = []
            for broker, url in self.endpoints.items():
                try:
                    async with session.get(url) as resp:
                        data = await resp.json()
                        price = float(data["price"])
                        ts = datetime.utcnow().timestamp()
                        records.append((ts, broker, self.symbol, price))
                        message = json.dumps(
                            {
                                "broker": broker,
                                "symbol": self.symbol,
                                "price": price,
                                "timestamp": ts,
                            }
                        )
                        self.mqtt_client.publish(self.mqtt_topic, message)
                        logging.info("Published price from %s: %s", broker, price)
                except Exception as e:
                    logging.error("Error fetching price from %s - %s", broker, e)
            if records:
                await self.ts_client.append_reference_prices("reference_prices", records)
        await self.ts_client.close()


async def main():
    config = {
        "endpoints": {
            "ripio": "https://criptoya.com/api/ripio/btc/ars/1",
            "satoshitango": "https://criptoya.com/api/satoshitango/btc/ars/1",
        },
        "symbol": "BTCARS",
        "mqtt_broker": "localhost",
        "mqtt_port": 1883,
        "mqtt_topic": "ar/reference_price",
        "timescale_config": {
            "host": "localhost",
            "port": 5432,
            "user": "admin",
            "password": "admin",
            "database": "timescaledb",
        },
    }
    task = ReferencePricePipeline("reference_price", timedelta(minutes=1), config)
    await task.execute()


if __name__ == "__main__":
    asyncio.run(main())
