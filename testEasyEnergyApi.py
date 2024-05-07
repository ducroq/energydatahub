import asyncio

from datetime import date
from easyenergy import EasyEnergy, VatOption


async def main() -> None:
    """Show example on fetching the energy prices from easyEnergy."""
    async with EasyEnergy(vat=VatOption.INCLUDE) as client:
        today = date.today()
        energy_today = await client.energy_prices(start_date=today, end_date=today)
        print(energy_today)
        gas_today = await client.gas_prices(start_date=today, end_date=today)
        print(gas_today)

if __name__ == "__main__":
    asyncio.run(main())
